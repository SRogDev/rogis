//! Async TCP server: connection tasks, pub/sub hub, background maintenance.

use crate::persist::{Aof, PersistCtl};
use crate::resp::{Frame, RespError};
use crate::store::Store;
use crate::{cmd, resp};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc};

/// Pub/sub hub: one broadcast channel per topic.
pub struct PubHub {
    channels: Mutex<HashMap<Vec<u8>, broadcast::Sender<Vec<u8>>>>,
}

impl PubHub {
    /// Create an empty hub.
    pub fn new() -> Self {
        Self {
            channels: Mutex::new(HashMap::new()),
        }
    }

    /// Publish `msg` to `channel`. Returns the number of receivers that got it.
    pub fn publish(&self, channel: &[u8], msg: Vec<u8>) -> usize {
        let mut map = self.channels.lock().unwrap_or_else(|e| e.into_inner());
        let sent = map.get(channel).map(|tx| tx.send(msg));
        match sent {
            Some(Ok(n)) => n,
            Some(Err(_)) => {
                // No receivers left: drop the stale sender so abandoned
                // channels can't grow the map without bound.
                map.remove(channel);
                0
            }
            None => 0,
        }
    }

    /// Subscribe to `channels`. Returns one receiver per channel.
    pub fn subscribe(&self, channels: &[Vec<u8>]) -> Vec<broadcast::Receiver<Vec<u8>>> {
        let mut map = self.channels.lock().unwrap_or_else(|e| e.into_inner());
        channels
            .iter()
            .map(|ch| {
                map.entry(ch.clone())
                    .or_insert_with(|| broadcast::channel(1024).0)
                    .subscribe()
            })
            .collect()
    }
}

impl Default for PubHub {
    fn default() -> Self {
        Self::new()
    }
}

/// Run the server on `listener` until the process exits.
///
/// Spawns background tasks for expiry sweeping, AOF fsyncing, and periodic
/// snapshots, then accepts connections forever.
pub async fn serve(
    listener: TcpListener,
    store: Arc<Store>,
    hub: Arc<PubHub>,
    aof: Arc<Aof>,
    persist: PersistCtl,
) {
    // Active expiry sweep, every second.
    {
        let store = Arc::clone(&store);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                store.evict_expired();
            }
        });
    }
    // AOF fsync ticker, every second.
    {
        let aof = Arc::clone(&aof);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                if let Err(e) = aof.sync() {
                    eprintln!("rogis: AOF fsync failed: {e}");
                }
            }
        });
    }
    // Periodic snapshots, only when the AOF marked the store dirty.
    // snapshot_reset also truncates the AOF so restarts can't double-apply.
    if persist.save_secs > 0 {
        let (store, aof) = (Arc::clone(&store), Arc::clone(&aof));
        let secs = persist.save_secs;
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(secs)).await;
                if aof.take_dirty() {
                    if let Err(e) = aof.snapshot_reset(&store) {
                        eprintln!("rogis: periodic snapshot failed: {e}");
                    }
                }
            }
        });
    }

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let (store, hub, aof) = (Arc::clone(&store), Arc::clone(&hub), Arc::clone(&aof));
                tokio::spawn(async move {
                    handle_conn(stream, store, hub, aof).await;
                });
            }
            Err(e) => eprintln!("rogis: accept failed: {e}"),
        }
    }
}

/// Cap on buffered-but-undecoded bytes per connection (DoS guard).
const MAX_BUFFERED: usize = 512 * 1024 * 1024 + 65_536;

/// Read one full frame. `Ok(None)` means clean EOF.
async fn read_frame(
    rd: &mut OwnedReadHalf,
    buf: &mut Vec<u8>,
    tmp: &mut [u8],
) -> std::io::Result<Option<Frame>> {
    loop {
        match resp::decode(buf) {
            Ok((frame, n)) => {
                buf.drain(..n);
                return Ok(Some(frame));
            }
            Err(RespError::Incomplete) => {
                if buf.len() > MAX_BUFFERED {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "request exceeds maximum buffered size",
                    ));
                }
                let n = rd.read(tmp).await?;
                if n == 0 {
                    return Ok(None);
                }
                buf.extend_from_slice(&tmp[..n]);
            }
            Err(RespError::Invalid(msg)) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("protocol error: {msg}"),
                ));
            }
        }
    }
}

/// Write one frame to the connection, encoding nils for the connection's
/// negotiated protocol version (RESP2 `$-1`/`*-1`, RESP3 `_\r\n`).
async fn write_frame(wr: &mut OwnedWriteHalf, frame: &Frame, proto: u8) -> std::io::Result<()> {
    let mut out = Vec::new();
    resp::encode_with_proto(frame, &mut out, proto);
    wr.write_all(&out).await
}

async fn handle_conn(stream: TcpStream, store: Arc<Store>, hub: Arc<PubHub>, aof: Arc<Aof>) {
    let (mut rd, mut wr) = stream.into_split();
    let mut buf = Vec::new();
    let mut tmp = [0u8; 8192];
    // Negotiated RESP version for this connection: 2 until a HELLO handshake
    // selects 3. Controls pub/sub frame shapes (push vs array).
    let mut proto: u8 = 2;
    loop {
        let frame = match read_frame(&mut rd, &mut buf, &mut tmp).await {
            Ok(Some(f)) => f,
            // EOF or protocol error: close the connection.
            Ok(None) | Err(_) => return,
        };
        let argv = match cmd::frame_to_argv(&frame) {
            Some(a) => a,
            None => return,
        };
        match cmd::dispatch(&argv, &store, &hub, &aof, proto) {
            cmd::CmdOut::Reply(f) => {
                if write_frame(&mut wr, &f, proto).await.is_err() {
                    return;
                }
            }
            cmd::CmdOut::Hello { proto: p, reply } => {
                proto = p;
                if write_frame(&mut wr, &reply, proto).await.is_err() {
                    return;
                }
            }
            cmd::CmdOut::Quit(f) => {
                let _ = write_frame(&mut wr, &f, proto).await;
                return;
            }
            cmd::CmdOut::Subscribe(channels) => {
                // true = close the connection; false = back to normal mode.
                if subscriber_mode(&mut wr, &mut rd, &hub, channels, proto).await {
                    return;
                }
            }
        }
    }
}

/// Build a pub/sub confirmation or message frame: RESP3 clients get a push
/// (`>`-typed) frame — redis-py routes only those to its pubsub push handler —
/// while RESP2 clients keep the classic array encoding.
fn pubsub_frame(proto: u8, items: Vec<Frame>) -> Frame {
    if proto == 3 {
        Frame::Push(items)
    } else {
        Frame::Array(Some(items))
    }
}

/// Run subscriber mode. Returns true when the connection must close
/// (QUIT / EOF / write error), false when it should return to normal
/// command mode (UNSUBSCRIBE from everything).
///
/// `proto` is the RESP version negotiated by the connection's HELLO handshake
/// (2 by default): it selects push (`>`) versus array (`*`) frame shapes.
async fn subscriber_mode(
    wr: &mut OwnedWriteHalf,
    rd: &mut OwnedReadHalf,
    hub: &Arc<PubHub>,
    initial: Vec<Vec<u8>>,
    proto: u8,
) -> bool {
    // Fan-in: one forwarder task per subscription pushes (channel, payload)
    // here so the select! below has a single message branch.
    let (tx, mut rx) = mpsc::unbounded_channel::<(Vec<u8>, Vec<u8>)>();
    // (channel, forwarder task)
    let mut subs: Vec<(Vec<u8>, tokio::task::JoinHandle<()>)> = Vec::new();
    if !sub_add(wr, hub, &tx, &mut subs, &initial, proto).await {
        return true;
    }

    let mut buf = Vec::new();
    let mut tmp = [0u8; 8192];
    loop {
        tokio::select! {
            res = read_frame(rd, &mut buf, &mut tmp) => {
                let frame = match res {
                    Ok(Some(f)) => f,
                    Ok(None) | Err(_) => return true,
                };
                let argv = match cmd::frame_to_argv(&frame) {
                    Some(a) if !a.is_empty() => a,
                    _ => return true,
                };
                match argv[0].to_ascii_uppercase().as_slice() {
                    b"SUBSCRIBE" => {
                        if argv.len() < 2 {
                            let e = Frame::Error(
                                "ERR wrong number of arguments for 'subscribe' command".to_string(),
                            );
                            if write_frame(wr, &e, proto).await.is_err() {
                                return true;
                            }
                        } else if !sub_add(wr, hub, &tx, &mut subs, &argv[1..], proto).await {
                            return true;
                        }
                    }
                    b"UNSUBSCRIBE" => {
                        let all = argv.len() == 1;
                        if !sub_remove(wr, &mut subs, &argv[1..], proto).await {
                            return true;
                        }
                        if all {
                            // Like Redis: leaving the last subscription drops
                            // the client back into normal command mode.
                            return false;
                        }
                    }
                    b"PING" => {
                        let payload = argv.get(1).cloned().unwrap_or_default();
                        let pong = Frame::Array(Some(vec![
                            Frame::Bulk(Some(b"pong".to_vec())),
                            Frame::Bulk(Some(payload)),
                        ]));
                        if write_frame(wr, &pong, proto).await.is_err() {
                            return true;
                        }
                    }
                    b"QUIT" => {
                        let _ = write_frame(wr, &Frame::Simple("OK".to_string()), proto).await;
                        return true;
                    }
                    _ => {
                        let name = String::from_utf8_lossy(&argv[0]);
                        let e = Frame::Error(format!(
                            "ERR Can't execute '{name}': only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT are allowed in this context"
                        ));
                        if write_frame(wr, &e, proto).await.is_err() {
                            return true;
                        }
                    }
                }
            }
            Some((ch, payload)) = rx.recv() => {
                let msg = pubsub_frame(
                    proto,
                    vec![
                        Frame::Bulk(Some(b"message".to_vec())),
                        Frame::Bulk(Some(ch)),
                        Frame::Bulk(Some(payload)),
                    ],
                );
                if write_frame(wr, &msg, proto).await.is_err() {
                    return true;
                }
            }
        }
    }
}

/// Subscribe to `channels` (deduped), sending one confirmation per channel.
/// Returns false when the connection is already broken.
async fn sub_add(
    wr: &mut OwnedWriteHalf,
    hub: &Arc<PubHub>,
    tx: &mpsc::UnboundedSender<(Vec<u8>, Vec<u8>)>,
    subs: &mut Vec<(Vec<u8>, tokio::task::JoinHandle<()>)>,
    channels: &[Vec<u8>],
    proto: u8,
) -> bool {
    for ch in channels {
        if !subs.iter().any(|(c, _)| c == ch) {
            // The receiver exists before the confirmation is sent, so no
            // message published after the confirmation can be missed.
            let mut bcast = hub
                .subscribe(std::slice::from_ref(ch))
                .pop()
                .expect("subscribe of one channel yields one receiver");
            let tx2 = tx.clone();
            let ch2 = ch.clone();
            let task = tokio::spawn(async move {
                loop {
                    match bcast.recv().await {
                        Ok(msg) => {
                            if tx2.send((ch2.clone(), msg)).is_err() {
                                break;
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
            });
            subs.push((ch.clone(), task));
        }
        let confirmation = pubsub_frame(
            proto,
            vec![
                Frame::Bulk(Some(b"subscribe".to_vec())),
                Frame::Bulk(Some(ch.clone())),
                Frame::Integer(subs.len() as i64),
            ],
        );
        if write_frame(wr, &confirmation, proto).await.is_err() {
            return false;
        }
    }
    true
}

/// Unsubscribe from `channels` (empty = all), one confirmation per channel.
/// Returns false when the connection is already broken.
async fn sub_remove(
    wr: &mut OwnedWriteHalf,
    subs: &mut Vec<(Vec<u8>, tokio::task::JoinHandle<()>)>,
    channels: &[Vec<u8>],
    proto: u8,
) -> bool {
    let targets: Vec<Vec<u8>> = if channels.is_empty() {
        subs.iter().map(|(c, _)| c.clone()).collect()
    } else {
        channels.to_vec()
    };
    for ch in &targets {
        if let Some(pos) = subs.iter().position(|(c, _)| c == ch) {
            let (_, task) = subs.remove(pos);
            task.abort();
        }
        let confirmation = pubsub_frame(
            proto,
            vec![
                Frame::Bulk(Some(b"unsubscribe".to_vec())),
                Frame::Bulk(Some(ch.clone())),
                Frame::Integer(subs.len() as i64),
            ],
        );
        if write_frame(wr, &confirmation, proto).await.is_err() {
            return false;
        }
    }
    true
}

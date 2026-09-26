//! End-to-end command tests against a real TCP server (raw RESP I/O).

use rogis::persist::{Aof, PersistCtl};
use rogis::resp::{self, Frame, RespError};
use rogis::server::{self, PubHub};
use rogis::store::Store;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

async fn spawn_server() -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        server::serve(
            listener,
            Arc::new(Store::new()),
            Arc::new(PubHub::new()),
            Arc::new(Aof::disabled()),
            PersistCtl { save_secs: 0 },
        )
        .await;
    });
    (addr, handle)
}

struct Client {
    stream: TcpStream,
    buf: Vec<u8>,
}

impl Client {
    async fn connect(addr: std::net::SocketAddr) -> Self {
        Self {
            stream: TcpStream::connect(addr).await.unwrap(),
            buf: Vec::new(),
        }
    }

    async fn cmd(&mut self, args: &[&str]) {
        let frame = Frame::Array(Some(
            args.iter()
                .map(|s| Frame::Bulk(Some(s.as_bytes().to_vec())))
                .collect(),
        ));
        let mut out = Vec::new();
        resp::encode(&frame, &mut out);
        self.stream.write_all(&out).await.unwrap();
    }

    async fn cmd_raw(&mut self, raw: &[u8]) {
        self.stream.write_all(raw).await.unwrap();
    }

    async fn read_frame(&mut self) -> Frame {
        loop {
            match resp::decode(&self.buf) {
                Ok((f, n)) => {
                    self.buf.drain(..n);
                    return f;
                }
                Err(RespError::Incomplete) => {
                    let mut tmp = [0u8; 4096];
                    let n = self.stream.read(&mut tmp).await.unwrap();
                    assert!(n > 0, "connection closed while waiting for a frame");
                    self.buf.extend_from_slice(&tmp[..n]);
                }
                Err(RespError::Invalid(e)) => panic!("protocol error from server: {e}"),
            }
        }
    }

    fn bulk(s: &str) -> Frame {
        Frame::Bulk(Some(s.as_bytes().to_vec()))
    }
}

#[tokio::test]
async fn pipelined_set_get() {
    let (addr, _srv) = spawn_server().await;
    let mut c = Client::connect(addr).await;
    // Two commands back-to-back in one write; expect two replies in order.
    let mut out = Vec::new();
    for args in [&["SET", "a", "hello"][..], &["GET", "a"][..]] {
        let frame = Frame::Array(Some(args.iter().map(|s| Client::bulk(s)).collect()));
        resp::encode(&frame, &mut out);
    }
    c.cmd_raw(&out).await;
    assert_eq!(c.read_frame().await, Frame::Simple("OK".to_string()));
    assert_eq!(c.read_frame().await, Client::bulk("hello"));
}

#[tokio::test]
async fn unknown_command_error() {
    let (addr, _srv) = spawn_server().await;
    let mut c = Client::connect(addr).await;
    c.cmd(&["FROBNICATE"]).await;
    assert_eq!(
        c.read_frame().await,
        Frame::Error("ERR unknown command 'FROBNICATE'".to_string())
    );
}

#[tokio::test]
async fn wrongtype_error_over_tcp() {
    let (addr, _srv) = spawn_server().await;
    let mut c = Client::connect(addr).await;
    c.cmd(&["SET", "s", "v"]).await;
    assert_eq!(c.read_frame().await, Frame::Simple("OK".to_string()));
    c.cmd(&["HGET", "s", "f"]).await;
    assert_eq!(
        c.read_frame().await,
        Frame::Error(
            "WRONGTYPE Operation against a key holding the wrong kind of value".to_string()
        )
    );
}

#[tokio::test]
async fn quit_closes_connection() {
    let (addr, _srv) = spawn_server().await;
    let mut c = Client::connect(addr).await;
    c.cmd(&["QUIT"]).await;
    assert_eq!(c.read_frame().await, Frame::Simple("OK".to_string()));
    let mut tmp = [0u8; 16];
    let n = c.stream.read(&mut tmp).await.unwrap();
    assert_eq!(n, 0, "server must close the connection after QUIT");
}

#[tokio::test]
async fn pubsub_end_to_end() {
    let (addr, _srv) = spawn_server().await;
    let mut sub = Client::connect(addr).await;
    let mut publisher = Client::connect(addr).await;

    sub.cmd(&["SUBSCRIBE", "news"]).await;
    assert_eq!(
        sub.read_frame().await,
        Frame::Array(Some(vec![
            Client::bulk("subscribe"),
            Client::bulk("news"),
            Frame::Integer(1),
        ]))
    );

    publisher.cmd(&["PUBLISH", "news", "hello"]).await;
    assert_eq!(publisher.read_frame().await, Frame::Integer(1));

    // Exact wire format of the pushed message.
    let mut tmp = [0u8; 64];
    let expected = b"*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$5\r\nhello\r\n";
    let mut got = Vec::new();
    while got.len() < expected.len() {
        let n = sub.stream.read(&mut tmp).await.unwrap();
        assert!(
            n > 0,
            "subscriber connection closed before the message arrived"
        );
        got.extend_from_slice(&tmp[..n]);
    }
    assert_eq!(got, expected);

    // Unsubscribe from the channel, then quit.
    sub.cmd(&["UNSUBSCRIBE", "news"]).await;
    assert_eq!(
        sub.read_frame().await,
        Frame::Array(Some(vec![
            Client::bulk("unsubscribe"),
            Client::bulk("news"),
            Frame::Integer(0),
        ]))
    );
    sub.cmd(&["QUIT"]).await;
    assert_eq!(sub.read_frame().await, Frame::Simple("OK".to_string()));
}

#[tokio::test]
async fn unsubscribe_all_returns_to_normal_mode() {
    let (addr, _srv) = spawn_server().await;
    let mut c = Client::connect(addr).await;
    c.cmd(&["SUBSCRIBE", "a", "b"]).await;
    assert_eq!(
        c.read_frame().await,
        Frame::Array(Some(vec![
            Client::bulk("subscribe"),
            Client::bulk("a"),
            Frame::Integer(1),
        ]))
    );
    assert_eq!(
        c.read_frame().await,
        Frame::Array(Some(vec![
            Client::bulk("subscribe"),
            Client::bulk("b"),
            Frame::Integer(2),
        ]))
    );
    // Bare UNSUBSCRIBE: confirmations for every channel, then back to normal.
    c.cmd(&["UNSUBSCRIBE"]).await;
    assert_eq!(
        c.read_frame().await,
        Frame::Array(Some(vec![
            Client::bulk("unsubscribe"),
            Client::bulk("a"),
            Frame::Integer(1),
        ]))
    );
    assert_eq!(
        c.read_frame().await,
        Frame::Array(Some(vec![
            Client::bulk("unsubscribe"),
            Client::bulk("b"),
            Frame::Integer(0),
        ]))
    );
    // Normal commands work again.
    c.cmd(&["PING"]).await;
    assert_eq!(c.read_frame().await, Frame::Simple("PONG".to_string()));
    c.cmd(&["SET", "k", "v"]).await;
    assert_eq!(c.read_frame().await, Frame::Simple("OK".to_string()));
}

#[tokio::test]
async fn ping_in_subscriber_mode() {
    let (addr, _srv) = spawn_server().await;
    let mut c = Client::connect(addr).await;
    c.cmd(&["SUBSCRIBE", "x"]).await;
    c.read_frame().await; // confirmation
    c.cmd(&["PING"]).await;
    assert_eq!(
        c.read_frame().await,
        Frame::Array(Some(vec![
            Client::bulk("pong"),
            Frame::Bulk(Some(Vec::new())),
        ]))
    );
    c.cmd(&["PING", "hey"]).await;
    assert_eq!(
        c.read_frame().await,
        Frame::Array(Some(vec![Client::bulk("pong"), Client::bulk("hey"),]))
    );
}

#[tokio::test]
async fn non_pubsub_commands_rejected_in_subscriber_mode() {
    let (addr, _srv) = spawn_server().await;
    let mut c = Client::connect(addr).await;
    c.cmd(&["SUBSCRIBE", "x"]).await;
    c.read_frame().await; // confirmation
    c.cmd(&["SET", "k", "v"]).await;
    assert_eq!(
        c.read_frame().await,
        Frame::Error(
            "ERR Can't execute 'SET': only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT are allowed in this context"
                .to_string()
        )
    );
}

/// Read exactly `n` raw bytes from the socket.
async fn read_exact_bytes(c: &mut Client, n: usize) -> Vec<u8> {
    let mut got = Vec::new();
    let mut tmp = [0u8; 64];
    while got.len() < n {
        let r = c.stream.read(&mut tmp).await.unwrap();
        assert!(r > 0, "connection closed while waiting for bytes");
        got.extend_from_slice(&tmp[..r]);
    }
    got
}

#[tokio::test]
async fn hello_3_negotiates_resp3() {
    let (addr, _srv) = spawn_server().await;
    let mut c = Client::connect(addr).await;
    c.cmd(&["HELLO", "3"]).await;
    match c.read_frame().await {
        Frame::Map(pairs) => {
            let proto = pairs
                .iter()
                .find_map(|(k, v)| match k {
                    Frame::Bulk(Some(b)) if b == b"proto" => Some(v),
                    _ => None,
                })
                .expect("HELLO map must carry a proto entry");
            assert_eq!(*proto, Frame::Integer(3));
        }
        other => panic!("HELLO must reply with a map, got {other:?}"),
    }
}

#[tokio::test]
async fn hello_defaults_to_resp2_map() {
    let (addr, _srv) = spawn_server().await;
    let mut c = Client::connect(addr).await;
    c.cmd(&["HELLO"]).await;
    match c.read_frame().await {
        Frame::Map(pairs) => {
            let proto = pairs
                .iter()
                .find_map(|(k, v)| match k {
                    Frame::Bulk(Some(b)) if b == b"proto" => Some(v),
                    _ => None,
                })
                .expect("HELLO map must carry a proto entry");
            assert_eq!(*proto, Frame::Integer(2));
        }
        other => panic!("HELLO must reply with a map, got {other:?}"),
    }
}

#[tokio::test]
async fn hello_rejects_unsupported_versions_and_auth_over_tcp() {
    let (addr, _srv) = spawn_server().await;
    let mut c = Client::connect(addr).await;
    c.cmd(&["HELLO", "4"]).await;
    assert_eq!(
        c.read_frame().await,
        Frame::Error("ERR unknown protocol version".to_string())
    );
    c.cmd(&["HELLO", "3", "AUTH", "u", "p"]).await;
    assert_eq!(
        c.read_frame().await,
        Frame::Error("ERR AUTH not supported in rogis v0.1".to_string())
    );
    // The connection survives both errors.
    c.cmd(&["PING"]).await;
    assert_eq!(c.read_frame().await, Frame::Simple("PONG".to_string()));
}

#[tokio::test]
async fn hello_3_subscribe_uses_push_frames() {
    let (addr, _srv) = spawn_server().await;
    let mut sub = Client::connect(addr).await;
    let mut publisher = Client::connect(addr).await;

    sub.cmd(&["HELLO", "3"]).await;
    sub.read_frame().await; // the HELLO map

    sub.cmd(&["SUBSCRIBE", "news"]).await;
    // RESP3 confirmations are push frames: wire must start with `>`.
    let expected_confirm = b">3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:1\r\n";
    assert_eq!(
        read_exact_bytes(&mut sub, expected_confirm.len()).await,
        expected_confirm
    );

    publisher.cmd(&["PUBLISH", "news", "hello"]).await;
    assert_eq!(publisher.read_frame().await, Frame::Integer(1));

    // RESP3 message frames are pushes too.
    let expected_msg = b">3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$5\r\nhello\r\n";
    assert_eq!(
        read_exact_bytes(&mut sub, expected_msg.len()).await,
        expected_msg
    );

    // Unsubscribe confirmation is also a push in RESP3.
    sub.cmd(&["UNSUBSCRIBE", "news"]).await;
    let expected_unsub = b">3\r\n$11\r\nunsubscribe\r\n$4\r\nnews\r\n:0\r\n";
    assert_eq!(
        read_exact_bytes(&mut sub, expected_unsub.len()).await,
        expected_unsub
    );
}

#[tokio::test]
async fn hello_2_subscribe_uses_array_frames() {
    let (addr, _srv) = spawn_server().await;
    let mut sub = Client::connect(addr).await;

    sub.cmd(&["HELLO", "2"]).await;
    sub.read_frame().await; // the HELLO map

    sub.cmd(&["SUBSCRIBE", "news"]).await;
    // Explicit RESP2: confirmations stay arrays like the default path.
    let expected = b"*3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:1\r\n";
    assert_eq!(read_exact_bytes(&mut sub, expected.len()).await, expected);
}

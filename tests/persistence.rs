//! Persistence end-to-end: SAVE snapshot reload, and AOF replay without SAVE.

use rogis::persist::{self, Aof, PersistCfg, PersistCtl};
use rogis::resp::{self, Frame, RespError};
use rogis::server::{self, PubHub};
use rogis::store::Store;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

fn tempdir() -> PathBuf {
    static N: AtomicUsize = AtomicUsize::new(0);
    let p = std::env::temp_dir().join(format!(
        "rogis-persist-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&p).unwrap();
    p
}

async fn spawn_server(
    dir: &Path,
    appendonly: bool,
) -> (std::net::SocketAddr, Arc<Aof>, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let aof = Arc::new(
        Aof::open(&PersistCfg {
            dir: dir.to_path_buf(),
            save_secs: 0,
            appendonly,
        })
        .unwrap(),
    );
    let aof2 = Arc::clone(&aof);
    let handle = tokio::spawn(async move {
        server::serve(
            listener,
            Arc::new(Store::new()),
            Arc::new(PubHub::new()),
            aof2,
            PersistCtl { save_secs: 0 },
        )
        .await;
    });
    (addr, aof, handle)
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

    async fn cmd(&mut self, args: &[&str]) -> Frame {
        let frame = Frame::Array(Some(
            args.iter()
                .map(|s| Frame::Bulk(Some(s.as_bytes().to_vec())))
                .collect(),
        ));
        let mut out = Vec::new();
        resp::encode(&frame, &mut out);
        self.stream.write_all(&out).await.unwrap();
        loop {
            match resp::decode(&self.buf) {
                Ok((f, n)) => {
                    self.buf.drain(..n);
                    return f;
                }
                Err(RespError::Incomplete) => {
                    let mut tmp = [0u8; 4096];
                    let n = self.stream.read(&mut tmp).await.unwrap();
                    assert!(n > 0, "connection closed unexpectedly");
                    self.buf.extend_from_slice(&tmp[..n]);
                }
                Err(RespError::Invalid(e)) => panic!("protocol error: {e}"),
            }
        }
    }
}

fn ok() -> Frame {
    Frame::Simple("OK".to_string())
}

#[tokio::test]
async fn snapshot_save_reload() {
    let dir = tempdir();
    let (addr, _aof, srv) = spawn_server(&dir, false).await;
    let mut c = Client::connect(addr).await;

    assert_eq!(c.cmd(&["SET", "greeting", "hello"]).await, ok());
    assert_eq!(c.cmd(&["SET", "temp", "val", "EX", "100"]).await, ok());
    assert_eq!(
        c.cmd(&["HSET", "user", "name", "ada", "age", "36"]).await,
        Frame::Integer(2)
    );
    assert_eq!(
        c.cmd(&["LPUSH", "mylist", "a", "b", "c"]).await,
        Frame::Integer(3)
    );
    assert_eq!(c.cmd(&["INCR", "counter"]).await, Frame::Integer(1));
    assert_eq!(c.cmd(&["INCR", "counter"]).await, Frame::Integer(2));
    assert_eq!(c.cmd(&["SAVE"]).await, ok());
    assert!(dir.join("dump.rogb").exists());

    // Drop everything, reload into a fresh store.
    srv.abort();
    let store = Store::new();
    persist::load(&store, &dir).unwrap();

    assert_eq!(store.get(b"greeting"), Ok(Some(b"hello".to_vec())));
    assert_eq!(store.hget(b"user", b"name"), Ok(Some(b"ada".to_vec())));
    assert_eq!(store.hget(b"user", b"age"), Ok(Some(b"36".to_vec())));
    assert_eq!(
        store.lrange(b"mylist", 0, -1),
        Ok(vec![b"c".to_vec(), b"b".to_vec(), b"a".to_vec()])
    );
    assert_eq!(store.get(b"counter"), Ok(Some(b"2".to_vec())));
    // TTL survived the round-trip (absolute deadline persisted).
    let ttl = store.ttl_ms(b"temp").unwrap();
    assert!(ttl > 0 && ttl <= 100_000, "ttl={ttl}");

    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn save_truncates_aof_no_double_apply() {
    // Write, SAVE, write more, restart: the post-SAVE AOF must not replay
    // the pre-SAVE writes a second time.
    let dir = tempdir();
    let (addr, aof, srv) = spawn_server(&dir, true).await;
    let mut c = Client::connect(addr).await;
    assert_eq!(c.cmd(&["INCR", "n"]).await, Frame::Integer(1));
    assert_eq!(c.cmd(&["SAVE"]).await, ok());
    assert_eq!(c.cmd(&["INCR", "n"]).await, Frame::Integer(2));
    aof.sync().unwrap();

    srv.abort();
    let store = Store::new();
    persist::load(&store, &dir).unwrap();
    assert_eq!(store.get(b"n"), Ok(Some(b"2".to_vec())));

    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn aof_replay_without_save() {
    let dir = tempdir();
    let (addr, aof, srv) = spawn_server(&dir, true).await;
    let mut c = Client::connect(addr).await;

    assert_eq!(c.cmd(&["SET", "k1", "v1"]).await, ok());
    assert_eq!(c.cmd(&["SET", "k2", "v2"]).await, ok());
    assert_eq!(c.cmd(&["DEL", "k2"]).await, Frame::Integer(1));
    assert_eq!(c.cmd(&["EXPIRE", "k1", "100"]).await, Frame::Integer(1));
    assert_eq!(c.cmd(&["HSET", "h", "f1", "a"]).await, Frame::Integer(1));
    assert_eq!(c.cmd(&["LPUSH", "l", "x", "y"]).await, Frame::Integer(2));
    assert_eq!(c.cmd(&["INCR", "n"]).await, Frame::Integer(1));
    // No SAVE: durability comes from the AOF alone.
    assert!(!dir.join("dump.rogb").exists());
    // Force the buffered AOF to disk (the 1s ticker would do it anyway).
    aof.sync().unwrap();

    srv.abort();
    let store = Store::new();
    persist::load(&store, &dir).unwrap();

    assert_eq!(store.get(b"k1"), Ok(Some(b"v1".to_vec())));
    assert_eq!(store.get(b"k2"), Ok(None));
    let ttl = store.ttl_ms(b"k1").unwrap();
    assert!(ttl > 0 && ttl <= 100_000, "ttl={ttl}");
    assert_eq!(store.hget(b"h", b"f1"), Ok(Some(b"a".to_vec())));
    assert_eq!(
        store.lrange(b"l", 0, -1),
        Ok(vec![b"y".to_vec(), b"x".to_vec()])
    );
    assert_eq!(store.get(b"n"), Ok(Some(b"1".to_vec())));

    std::fs::remove_dir_all(&dir).ok();
}

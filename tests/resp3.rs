//! RESP3 wire-correctness over real TCP: nil replies encode as `_\r\n`
//! (never `$-1`/`*-1`, which hang real RESP3 clients like redis-py 8.x),
//! and HGETALL returns a RESP3 map (`%`). HELLO 2 output stays
//! byte-identical to the classic RESP2 shapes.

use rogis::persist::{Aof, PersistCtl};
use rogis::resp::{self, Frame, RespError};
use rogis::server::{self, PubHub};
use rogis::store::Store;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

async fn spawn_server() -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        server::serve(
            listener,
            Arc::new(Store::new()),
            Arc::new(PubHub::new()),
            Arc::new(Aof::disabled()),
            PersistCtl { save_secs: 0 },
        )
        .await;
    });
    addr
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

    /// Read from the socket until at least `expected.len()` bytes are
    /// buffered, then assert the next bytes match `expected` byte-for-byte.
    async fn expect_raw(&mut self, expected: &[u8]) {
        while self.buf.len() < expected.len() {
            let mut tmp = [0u8; 4096];
            let n = self.stream.read(&mut tmp).await.unwrap();
            assert!(n > 0, "connection closed while waiting for reply");
            self.buf.extend_from_slice(&tmp[..n]);
        }
        let got = self.buf[..expected.len()].to_vec();
        self.buf.drain(..expected.len());
        assert_eq!(got.as_slice(), expected, "raw wire bytes mismatch");
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
}

/// Sorted (field, value) pairs out of a map/array HGETALL reply.
fn sorted_pairs(frame: &Frame) -> Vec<(Vec<u8>, Vec<u8>)> {
    let items: Vec<&Frame> = match frame {
        Frame::Map(pairs) => pairs.iter().flat_map(|(k, v)| [k, v]).collect(),
        Frame::Array(Some(items)) => items.iter().collect(),
        other => panic!("expected map or array HGETALL reply, got {other:?}"),
    };
    let mut pairs: Vec<(Vec<u8>, Vec<u8>)> = items
        .as_chunks::<2>()
        .0
        .iter()
        .map(|c| match (c[0], c[1]) {
            (Frame::Bulk(Some(f)), Frame::Bulk(Some(v))) => (f.clone(), v.clone()),
            _ => panic!("expected bulk pairs, got {c:?}"),
        })
        .collect();
    pairs.sort();
    pairs
}

async fn hello3(c: &mut Client) {
    c.cmd(&["HELLO", "3"]).await;
    match c.read_frame().await {
        Frame::Map(_) => {}
        other => panic!("HELLO 3 must reply with a map, got {other:?}"),
    }
}

async fn hello2(c: &mut Client) {
    c.cmd(&["HELLO", "2"]).await;
    match c.read_frame().await {
        Frame::Map(_) => {}
        other => panic!("HELLO 2 must reply with a map, got {other:?}"),
    }
}

#[tokio::test]
async fn resp3_every_nil_reply_is_underscore() {
    let addr = spawn_server().await;
    let mut c = Client::connect(addr).await;
    hello3(&mut c).await;

    // GET on a missing key.
    c.cmd(&["GET", "nope"]).await;
    c.expect_raw(b"_\r\n").await;

    // HGET on a missing field.
    c.cmd(&["HSET", "h", "f", "v"]).await;
    c.read_frame().await;
    c.cmd(&["HGET", "h", "absent"]).await;
    c.expect_raw(b"_\r\n").await;

    // SET NX refused on an existing key.
    c.cmd(&["SET", "k", "v"]).await;
    c.read_frame().await;
    c.cmd(&["SET", "k", "v2", "NX"]).await;
    c.expect_raw(b"_\r\n").await;

    // SET XX refused on a missing key.
    c.cmd(&["SET", "ghost", "v", "XX"]).await;
    c.expect_raw(b"_\r\n").await;

    // SET with both NX and XX: nil whether or not the key exists.
    c.cmd(&["SET", "ghost2", "v", "NX", "XX"]).await;
    c.expect_raw(b"_\r\n").await;
    c.cmd(&["SET", "k", "v3", "NX", "XX"]).await;
    c.expect_raw(b"_\r\n").await;

    // RPOP on a missing key.
    c.cmd(&["RPOP", "nolist"]).await;
    c.expect_raw(b"_\r\n").await;

    // UNSUBSCRIBE in normal mode carries a nil channel: nested nil must
    // also use `_`, not `$-1`.
    c.cmd(&["UNSUBSCRIBE"]).await;
    c.expect_raw(b"*3\r\n$11\r\nunsubscribe\r\n_\r\n:0\r\n")
        .await;

    // The `_\r\n` frame decodes as the null frame.
    c.cmd(&["GET", "nope"]).await;
    assert_eq!(c.read_frame().await, Frame::Bulk(None));
}

#[tokio::test]
async fn resp3_hgetall_returns_a_map() {
    let addr = spawn_server().await;
    let mut c = Client::connect(addr).await;
    hello3(&mut c).await;

    c.cmd(&["HSET", "h", "b", "2", "a", "1"]).await;
    c.read_frame().await;
    c.cmd(&["HGETALL", "h"]).await;
    let frame = c.read_frame().await;
    assert!(
        matches!(frame, Frame::Map(_)),
        "RESP3 HGETALL must be a map, got {frame:?}"
    );
    assert_eq!(
        sorted_pairs(&frame),
        vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec()),
        ]
    );
}

#[tokio::test]
async fn resp3_hgetall_missing_key_is_empty_map() {
    let addr = spawn_server().await;
    let mut c = Client::connect(addr).await;
    hello3(&mut c).await;

    c.cmd(&["HGETALL", "missing"]).await;
    c.expect_raw(b"%0\r\n").await;
}

#[tokio::test]
async fn resp2_output_stays_byte_identical() {
    let addr = spawn_server().await;
    let mut c = Client::connect(addr).await;
    hello2(&mut c).await;

    // Nil bulk stays `$-1\r\n`.
    c.cmd(&["GET", "nope"]).await;
    c.expect_raw(b"$-1\r\n").await;

    // SET NX refusal stays `$-1\r\n`.
    c.cmd(&["SET", "k", "v"]).await;
    c.read_frame().await;
    c.cmd(&["SET", "k", "v2", "NX"]).await;
    c.expect_raw(b"$-1\r\n").await;

    // HGETALL on a missing key stays an empty flat array.
    c.cmd(&["HGETALL", "missing"]).await;
    c.expect_raw(b"*0\r\n").await;

    // HGETALL on an existing key stays a flat field/value array.
    c.cmd(&["HSET", "h", "b", "2", "a", "1"]).await;
    c.read_frame().await;
    c.cmd(&["HGETALL", "h"]).await;
    let frame = c.read_frame().await;
    assert!(
        matches!(frame, Frame::Array(Some(_))),
        "RESP2 HGETALL must be an array, got {frame:?}"
    );
    assert_eq!(
        sorted_pairs(&frame),
        vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec()),
        ]
    );

    // Nested nil in the UNSUBSCRIBE confirmation stays `$-1\r\n`.
    c.cmd(&["UNSUBSCRIBE"]).await;
    c.expect_raw(b"*3\r\n$11\r\nunsubscribe\r\n$-1\r\n:0\r\n")
        .await;
}

#[tokio::test]
async fn resp2_default_connection_without_hello_is_unchanged() {
    let addr = spawn_server().await;
    let mut c = Client::connect(addr).await;
    // No HELLO at all: proto 2 default.
    c.cmd(&["GET", "nope"]).await;
    c.expect_raw(b"$-1\r\n").await;
    c.cmd(&["HGETALL", "missing"]).await;
    c.expect_raw(b"*0\r\n").await;
}

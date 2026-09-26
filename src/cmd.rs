//! Command dispatch: RESP argv -> store / pubsub / persistence operations.
//!
//! `dispatch` is pure command logic: it never touches the socket. The
//! connection task in `server.rs` interprets [`CmdOut`].

use crate::persist::Aof;
use crate::resp::Frame;
use crate::server::PubHub;
use crate::store::{Store, StoreError};
use std::sync::atomic::{AtomicU64, Ordering};

/// What a command asks the connection task to do next.
#[derive(Debug)]
pub enum CmdOut {
    /// Write this frame and keep reading commands in normal mode.
    Reply(Frame),
    /// Write the HELLO map reply, switch the connection to `proto`, and keep
    /// reading commands in normal mode.
    Hello { proto: u8, reply: Frame },
    /// Enter subscriber mode on these channels (confirmations are sent first).
    Subscribe(Vec<Vec<u8>>),
    /// Write this frame, then close the connection.
    Quit(Frame),
}

use CmdOut::{Hello, Quit, Reply, Subscribe};

/// Convert a decoded client frame into an argv vector.
///
/// Returns `None` when the frame is not an array of bulk strings (the
/// connection task treats that as a protocol error and closes).
pub fn frame_to_argv(frame: &Frame) -> Option<Vec<Vec<u8>>> {
    match frame {
        Frame::Array(Some(items)) => items
            .iter()
            .map(|f| match f {
                Frame::Bulk(Some(b)) => Some(b.clone()),
                _ => None,
            })
            .collect(),
        _ => None,
    }
}

/// Dispatch one command. Command names are matched case-insensitively.
///
/// `proto` is the RESP version negotiated for the connection (2 by default,
/// 3 after `HELLO 3`); it selects protocol-sensitive reply shapes — nils
/// are encoded by the server, and `HGETALL` returns a real RESP3 map on
/// proto 3 (flat array on proto 2, byte-identical to before).
///
/// Write commands that actually changed the dataset are appended to the AOF
/// (via `aof.log`, a no-op when persistence is disabled); reads never are.
pub fn dispatch(argv: &[Vec<u8>], store: &Store, hub: &PubHub, aof: &Aof, proto: u8) -> CmdOut {
    if argv.is_empty() {
        return err("ERR unknown command ''".to_string());
    }
    let name_raw = &argv[0];
    let name = name_raw.to_ascii_uppercase();
    let args = &argv[1..];
    match name.as_slice() {
        b"PING" => match args.len() {
            0 => Reply(Frame::Simple("PONG".to_string())),
            1 => Reply(Frame::Bulk(Some(args[0].clone()))),
            _ => arity(name_raw),
        },
        b"SET" => cmd_set(argv, args, name_raw, store, aof),
        b"GET" => {
            if args.len() != 1 {
                return arity(name_raw);
            }
            match store.get(&args[0]) {
                Ok(Some(v)) => Reply(Frame::Bulk(Some(v))),
                Ok(None) => Reply(Frame::Bulk(None)),
                Err(e) => store_err(e),
            }
        }
        b"DEL" => {
            if args.is_empty() {
                return arity(name_raw);
            }
            let keys: Vec<&[u8]> = args.iter().map(Vec::as_slice).collect();
            match store.del(&keys) {
                Ok(n) => {
                    if n > 0 {
                        aof.log(argv);
                    }
                    Reply(Frame::Integer(n as i64))
                }
                Err(e) => store_err(e),
            }
        }
        b"EXISTS" => {
            if args.is_empty() {
                return arity(name_raw);
            }
            let keys: Vec<&[u8]> = args.iter().map(Vec::as_slice).collect();
            match store.exists(&keys) {
                Ok(n) => Reply(Frame::Integer(n as i64)),
                Err(e) => store_err(e),
            }
        }
        b"EXPIRE" => {
            if args.len() != 2 {
                return arity(name_raw);
            }
            match parse_i64(&args[1]) {
                Some(secs) => do_expire(argv, store, aof, &args[0], secs.saturating_mul(1000)),
                None => not_int(),
            }
        }
        b"PEXPIRE" => {
            if args.len() != 2 {
                return arity(name_raw);
            }
            match parse_i64(&args[1]) {
                Some(ms) => do_expire(argv, store, aof, &args[0], ms),
                None => not_int(),
            }
        }
        b"TTL" => {
            if args.len() != 1 {
                return arity(name_raw);
            }
            match store.ttl_ms(&args[0]) {
                // -2 (missing) and -1 (no expiry) pass through; ms truncates to seconds.
                Ok(ms) if ms >= 0 => Reply(Frame::Integer(ms / 1000)),
                Ok(neg) => Reply(Frame::Integer(neg)),
                Err(e) => store_err(e),
            }
        }
        b"INCR" | b"DECR" | b"INCRBY" => {
            let delta = match name.as_slice() {
                b"INCR" => {
                    if args.len() != 1 {
                        return arity(name_raw);
                    }
                    1
                }
                b"DECR" => {
                    if args.len() != 1 {
                        return arity(name_raw);
                    }
                    -1
                }
                _ => {
                    if args.len() != 2 {
                        return arity(name_raw);
                    }
                    match parse_i64(&args[1]) {
                        Some(n) => n,
                        None => return not_int(),
                    }
                }
            };
            match store.incrby(&args[0], delta) {
                Ok(n) => {
                    aof.log(argv);
                    Reply(Frame::Integer(n))
                }
                Err(e) => store_err(e),
            }
        }
        b"HSET" => {
            // args = key + field/value pairs: even count, at least one pair.
            if args.len() < 3 || args.len().is_multiple_of(2) {
                return arity(name_raw);
            }
            let pairs: Vec<(Vec<u8>, Vec<u8>)> = args[1..]
                .as_chunks::<2>()
                .0
                .iter()
                .map(|c| (c[0].clone(), c[1].clone()))
                .collect();
            match store.hset(&args[0], &pairs) {
                Ok(n) => {
                    aof.log(argv);
                    Reply(Frame::Integer(n as i64))
                }
                Err(e) => store_err(e),
            }
        }
        b"HGET" => {
            if args.len() != 2 {
                return arity(name_raw);
            }
            match store.hget(&args[0], &args[1]) {
                Ok(Some(v)) => Reply(Frame::Bulk(Some(v))),
                Ok(None) => Reply(Frame::Bulk(None)),
                Err(e) => store_err(e),
            }
        }
        b"HGETALL" => {
            if args.len() != 1 {
                return arity(name_raw);
            }
            match store.hgetall(&args[0]) {
                Ok(pairs) => {
                    // RESP3 clients (redis-py's HGETALL callback is the
                    // identity function) need a real map; RESP2 keeps the
                    // classic flat field/value array, byte-identical.
                    if proto == 3 {
                        Reply(Frame::Map(
                            pairs
                                .into_iter()
                                .map(|(f, v)| (Frame::Bulk(Some(f)), Frame::Bulk(Some(v))))
                                .collect(),
                        ))
                    } else {
                        let mut items = Vec::with_capacity(pairs.len() * 2);
                        for (f, v) in pairs {
                            items.push(Frame::Bulk(Some(f)));
                            items.push(Frame::Bulk(Some(v)));
                        }
                        Reply(Frame::Array(Some(items)))
                    }
                }
                Err(e) => store_err(e),
            }
        }
        b"HDEL" => {
            if args.len() < 2 {
                return arity(name_raw);
            }
            let fields: Vec<&[u8]> = args[1..].iter().map(Vec::as_slice).collect();
            match store.hdel(&args[0], &fields) {
                Ok(n) => {
                    if n > 0 {
                        aof.log(argv);
                    }
                    Reply(Frame::Integer(n as i64))
                }
                Err(e) => store_err(e),
            }
        }
        b"LPUSH" => {
            if args.len() < 2 {
                return arity(name_raw);
            }
            match store.lpush(&args[0], &args[1..]) {
                Ok(n) => {
                    aof.log(argv);
                    Reply(Frame::Integer(n as i64))
                }
                Err(e) => store_err(e),
            }
        }
        b"RPOP" => {
            if args.len() != 1 {
                return arity(name_raw);
            }
            match store.rpop(&args[0]) {
                Ok(Some(v)) => {
                    aof.log(argv);
                    Reply(Frame::Bulk(Some(v)))
                }
                Ok(None) => Reply(Frame::Bulk(None)),
                Err(e) => store_err(e),
            }
        }
        b"LRANGE" => {
            if args.len() != 3 {
                return arity(name_raw);
            }
            let (start, stop) = match (parse_i64(&args[1]), parse_i64(&args[2])) {
                (Some(s), Some(e)) => (s, e),
                _ => return not_int(),
            };
            match store.lrange(&args[0], start, stop) {
                Ok(elems) => Reply(Frame::Array(Some(
                    elems.into_iter().map(|e| Frame::Bulk(Some(e))).collect(),
                ))),
                Err(e) => store_err(e),
            }
        }
        b"SETNX" => {
            if args.len() != 2 {
                return arity(name_raw);
            }
            match store.setnx(&args[0], args[1].clone()) {
                Ok(true) => {
                    aof.log(argv);
                    Reply(Frame::Integer(1))
                }
                Ok(false) => Reply(Frame::Integer(0)),
                Err(e) => store_err(e),
            }
        }
        b"PUBLISH" => {
            if args.len() != 2 {
                return arity(name_raw);
            }
            let n = hub.publish(&args[0], args[1].clone());
            Reply(Frame::Integer(n as i64))
        }
        b"SUBSCRIBE" => {
            if args.is_empty() {
                return arity(name_raw);
            }
            Subscribe(args.to_vec())
        }
        b"UNSUBSCRIBE" => {
            // Normal (non-subscriber) mode: confirm with count 0. One
            // confirmation array per channel; no channels -> nil channel.
            let mut items = Vec::new();
            if args.is_empty() {
                push_unsub(&mut items, None, 0);
            } else {
                for ch in args {
                    push_unsub(&mut items, Some(ch.clone()), 0);
                }
            }
            Reply(Frame::Array(Some(items)))
        }
        b"SAVE" => {
            if !args.is_empty() {
                return arity(name_raw);
            }
            match aof.snapshot_reset(store) {
                Ok(()) => Reply(Frame::Simple("OK".to_string())),
                Err(e) => err(format!("ERR snapshot failed: {e}")),
            }
        }
        b"QUIT" => {
            if !args.is_empty() {
                return arity(name_raw);
            }
            Quit(Frame::Simple("OK".to_string()))
        }
        b"HELLO" => cmd_hello(args),
        _ => err(format!(
            "ERR unknown command '{}'",
            String::from_utf8_lossy(name_raw)
        )),
    }
}

/// Monotonic `id` source for HELLO replies. The `id` in a HELLO reply only
/// needs to be unique, and a HELLO handshake is negotiated once per
/// connection — a single global atomic counter is the simplest correct
/// source, no per-connection state required.
static HELLO_ID: AtomicU64 = AtomicU64::new(1);

/// `HELLO [protover [AUTH user pass] [SETNAME name]]`, case-insensitive.
///
/// The connection task switches the connection to the negotiated protocol
/// version from the returned [`CmdOut::Hello`].
fn cmd_hello(args: &[Vec<u8>]) -> CmdOut {
    // AUTH is rejected in any position: a client that believes it sent
    // credentials must never be silently unprotected. The version slot is
    // positional, so `HELLO AUTH user pass` is checked explicitly before
    // version parsing; AUTH appearing as an *option value* (e.g. after
    // SETNAME) is still accepted.
    if args
        .first()
        .is_some_and(|v| v.eq_ignore_ascii_case(b"AUTH"))
    {
        return err("ERR AUTH not supported in rogis v0.1".to_string());
    }
    // Protocol version: absent means RESP2.
    let proto: u8 = match args.first() {
        None => 2,
        Some(v) => match v.as_slice() {
            b"2" => 2,
            b"3" => 3,
            // Note: real Redis replies `NOPROTO unsupported protocol version`,
            // but `ERR unknown protocol version` states the actual problem
            // more plainly to the client author debugging a handshake.
            _ => return err("ERR unknown protocol version".to_string()),
        },
    };
    // Options after the version, in any order. Parsed sequentially so that
    // option *values* (e.g. `SETNAME auth`) are never mistaken for options.
    let mut i = 1;
    while i < args.len() {
        match args[i].to_ascii_uppercase().as_slice() {
            b"AUTH" => {
                // Credentials must never be silently ignored: a client that
                // thinks it authenticated is a security hole.
                return err("ERR AUTH not supported in rogis v0.1".to_string());
            }
            b"SETNAME" => {
                // No client tracking in v0.1: accept the option and drop the
                // name, keeping the handshake shape clients expect.
                i += 1;
                if args.get(i).is_none() {
                    return err("ERR syntax error".to_string());
                }
            }
            _ => return err("ERR syntax error".to_string()),
        }
        i += 1;
    }
    let mut pairs = Vec::with_capacity(7);
    let bulk = |s: &str| Frame::Bulk(Some(s.as_bytes().to_vec()));
    pairs.push((bulk("server"), bulk("rogis")));
    pairs.push((bulk("version"), bulk(env!("CARGO_PKG_VERSION"))));
    pairs.push((bulk("proto"), Frame::Integer(proto as i64)));
    pairs.push((
        bulk("id"),
        Frame::Integer(HELLO_ID.fetch_add(1, Ordering::Relaxed) as i64),
    ));
    pairs.push((bulk("mode"), bulk("standalone")));
    pairs.push((bulk("role"), bulk("master")));
    pairs.push((bulk("modules"), Frame::Array(Some(Vec::new()))));
    Hello {
        proto,
        reply: Frame::Map(pairs),
    }
}

/// `SET key val [EX s|PX ms] [NX|XX]` — options case-insensitive.
fn cmd_set(
    argv: &[Vec<u8>],
    args: &[Vec<u8>],
    name_raw: &[u8],
    store: &Store,
    aof: &Aof,
) -> CmdOut {
    if args.len() < 2 {
        return arity(name_raw);
    }
    let mut ex_ms: Option<u64> = None;
    let mut nx = false;
    let mut xx = false;
    let mut i = 2;
    while i < args.len() {
        match args[i].to_ascii_uppercase().as_slice() {
            b"EX" => {
                i += 1;
                let Some(raw) = args.get(i) else {
                    return err("ERR syntax error".to_string());
                };
                let secs = parse_i64(raw)
                    .filter(|&s| s > 0)
                    .and_then(|s| (s as u64).checked_mul(1000));
                match secs {
                    Some(ms) => ex_ms = Some(ms),
                    None => return err("ERR invalid expire time in 'set' command".to_string()),
                }
            }
            b"PX" => {
                i += 1;
                let Some(raw) = args.get(i) else {
                    return err("ERR syntax error".to_string());
                };
                match parse_i64(raw).filter(|&ms| ms > 0) {
                    Some(ms) => ex_ms = Some(ms as u64),
                    None => return err("ERR invalid expire time in 'set' command".to_string()),
                }
            }
            b"NX" => nx = true,
            b"XX" => xx = true,
            _ => return err("ERR syntax error".to_string()),
        }
        i += 1;
    }
    // Both NX and XX set: the store resolves it exactly like Redis
    // (nil whether or not the key exists).
    match store.set(&args[0], args[1].clone(), ex_ms, nx, xx) {
        Ok(true) => {
            aof.log(argv);
            Reply(Frame::Simple("OK".to_string()))
        }
        Ok(false) => Reply(Frame::Bulk(None)),
        Err(e) => store_err(e),
    }
}

/// Shared by EXPIRE (seconds) and PEXPIRE (milliseconds).
fn do_expire(argv: &[Vec<u8>], store: &Store, aof: &Aof, key: &[u8], ms: i64) -> CmdOut {
    if ms <= 0 {
        // Redis deletes the key immediately on a non-positive timeout.
        let refs = [key];
        return match store.del(&refs) {
            Ok(n) => {
                if n > 0 {
                    aof.log(argv);
                }
                Reply(Frame::Integer(n as i64))
            }
            Err(e) => store_err(e),
        };
    }
    match store.expire_ms(key, ms as u64) {
        Ok(true) => {
            aof.log(argv);
            Reply(Frame::Integer(1))
        }
        Ok(false) => Reply(Frame::Integer(0)),
        Err(e) => store_err(e),
    }
}

/// One `unsubscribe` confirmation triple appended to a reply array.
fn push_unsub(items: &mut Vec<Frame>, channel: Option<Vec<u8>>, count: i64) {
    items.push(Frame::Bulk(Some(b"unsubscribe".to_vec())));
    items.push(Frame::Bulk(channel));
    items.push(Frame::Integer(count));
}

/// Strict `i64` parse, matching the store's integer rules.
fn parse_i64(bytes: &[u8]) -> Option<i64> {
    std::str::from_utf8(bytes).ok()?.parse().ok()
}

fn err(msg: String) -> CmdOut {
    Reply(Frame::Error(msg))
}

fn arity(name: &[u8]) -> CmdOut {
    let lower = String::from_utf8_lossy(name).to_ascii_lowercase();
    err(format!(
        "ERR wrong number of arguments for '{lower}' command"
    ))
}

fn wrongtype() -> CmdOut {
    err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
}

fn not_int() -> CmdOut {
    err("ERR value is not an integer or out of range".to_string())
}

fn store_err(e: StoreError) -> CmdOut {
    match e {
        StoreError::WrongType => wrongtype(),
        StoreError::NotInteger => not_int(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persist::PersistCfg;
    use crate::resp::encode;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Ctx {
        store: Store,
        hub: PubHub,
        aof: Aof,
    }

    impl Ctx {
        fn new() -> Self {
            Self {
                store: Store::new(),
                hub: PubHub::new(),
                aof: Aof::disabled(),
            }
        }
    }

    fn run(ctx: &Ctx, args: &[&str]) -> CmdOut {
        run_proto(ctx, args, 2)
    }

    /// Dispatch with an explicit negotiated protocol version. Plain `run`
    /// stays RESP2, so every pre-existing test pins the RESP2 wire shape.
    fn run_proto(ctx: &Ctx, args: &[&str], proto: u8) -> CmdOut {
        let argv: Vec<Vec<u8>> = args.iter().map(|s| s.as_bytes().to_vec()).collect();
        dispatch(&argv, &ctx.store, &ctx.hub, &ctx.aof, proto)
    }

    /// Encoded wire bytes of a Reply/Quit outcome.
    fn wire(out: &CmdOut) -> Vec<u8> {
        let mut buf = Vec::new();
        match out {
            CmdOut::Reply(f) | CmdOut::Quit(f) | CmdOut::Hello { reply: f, .. } => {
                encode(f, &mut buf)
            }
            CmdOut::Subscribe(_) => panic!("expected Reply/Quit/Hello, got Subscribe"),
        }
        buf
    }

    fn tempdir() -> PathBuf {
        static N: AtomicUsize = AtomicUsize::new(0);
        let p = std::env::temp_dir().join(format!(
            "rogis-cmd-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn ping() {
        let ctx = Ctx::new();
        assert_eq!(wire(&run(&ctx, &["PING"])), b"+PONG\r\n");
        assert_eq!(wire(&run(&ctx, &["PiNg"])), b"+PONG\r\n");
        assert_eq!(wire(&run(&ctx, &["PING", "hello"])), b"$5\r\nhello\r\n");
        assert_eq!(
            wire(&run(&ctx, &["PING", "a", "b"])),
            b"-ERR wrong number of arguments for 'ping' command\r\n"
        );
    }

    #[test]
    fn unknown_command() {
        let ctx = Ctx::new();
        assert_eq!(
            wire(&run(&ctx, &["FROBNICATE"])),
            b"-ERR unknown command 'FROBNICATE'\r\n"
        );
        assert_eq!(
            wire(&run(&ctx, &["nope", "x", "y"])),
            b"-ERR unknown command 'nope'\r\n"
        );
    }

    #[test]
    fn set_get_roundtrip() {
        let ctx = Ctx::new();
        assert_eq!(wire(&run(&ctx, &["SET", "k", "v"])), b"+OK\r\n");
        assert_eq!(wire(&run(&ctx, &["GET", "k"])), b"$1\r\nv\r\n");
        assert_eq!(wire(&run(&ctx, &["GET", "missing"])), b"$-1\r\n");
        assert_eq!(wire(&run(&ctx, &["sEt", "k2", "v2"])), b"+OK\r\n");
    }

    #[test]
    fn set_nx_xx() {
        let ctx = Ctx::new();
        assert_eq!(wire(&run(&ctx, &["SET", "a", "1", "NX"])), b"+OK\r\n");
        assert_eq!(wire(&run(&ctx, &["SET", "a", "2", "NX"])), b"$-1\r\n");
        assert_eq!(wire(&run(&ctx, &["GET", "a"])), b"$1\r\n1\r\n");
        assert_eq!(wire(&run(&ctx, &["SET", "b", "1", "XX"])), b"$-1\r\n");
        assert_eq!(wire(&run(&ctx, &["GET", "b"])), b"$-1\r\n");
        assert_eq!(wire(&run(&ctx, &["SET", "a", "3", "xx"])), b"+OK\r\n");
        assert_eq!(wire(&run(&ctx, &["GET", "a"])), b"$1\r\n3\r\n");
    }

    #[test]
    fn set_nx_xx_both_flags_always_nil_like_redis() {
        // Redis returns nil for SET with both NX and XX whether or not the
        // key exists; the store implements exactly that.
        let ctx = Ctx::new();
        assert_eq!(wire(&run(&ctx, &["SET", "m", "v", "NX", "XX"])), b"$-1\r\n");
        assert_eq!(wire(&run(&ctx, &["GET", "m"])), b"$-1\r\n");
        assert_eq!(wire(&run(&ctx, &["SET", "e", "1"])), b"+OK\r\n");
        assert_eq!(wire(&run(&ctx, &["SET", "e", "2", "NX", "XX"])), b"$-1\r\n");
        assert_eq!(wire(&run(&ctx, &["GET", "e"])), b"$1\r\n1\r\n");
    }

    #[test]
    fn set_option_errors() {
        let ctx = Ctx::new();
        assert_eq!(
            wire(&run(&ctx, &["SET", "k", "v", "EX"])),
            b"-ERR syntax error\r\n"
        );
        assert_eq!(
            wire(&run(&ctx, &["SET", "k", "v", "EX", "xyz"])),
            b"-ERR invalid expire time in 'set' command\r\n"
        );
        assert_eq!(
            wire(&run(&ctx, &["SET", "k", "v", "EX", "0"])),
            b"-ERR invalid expire time in 'set' command\r\n"
        );
        assert_eq!(
            wire(&run(&ctx, &["SET", "k", "v", "EX", "-5"])),
            b"-ERR invalid expire time in 'set' command\r\n"
        );
        assert_eq!(
            wire(&run(&ctx, &["SET", "k", "v", "PX", "0"])),
            b"-ERR invalid expire time in 'set' command\r\n"
        );
        assert_eq!(
            wire(&run(&ctx, &["SET", "k", "v", "WHAT"])),
            b"-ERR syntax error\r\n"
        );
        assert_eq!(
            wire(&run(&ctx, &["SET", "onlykey"])),
            b"-ERR wrong number of arguments for 'set' command\r\n"
        );
    }

    #[test]
    fn get_arity_and_wrongtype() {
        let ctx = Ctx::new();
        assert_eq!(
            wire(&run(&ctx, &["GET"])),
            b"-ERR wrong number of arguments for 'get' command\r\n"
        );
        assert_eq!(
            wire(&run(&ctx, &["GET", "a", "b"])),
            b"-ERR wrong number of arguments for 'get' command\r\n"
        );
        assert_eq!(wire(&run(&ctx, &["HSET", "h", "f", "v"])), b":1\r\n");
        assert_eq!(
            wire(&run(&ctx, &["GET", "h"])),
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        );
    }

    #[test]
    fn del_and_exists() {
        let ctx = Ctx::new();
        assert_eq!(wire(&run(&ctx, &["SET", "a", "1"])), b"+OK\r\n");
        assert_eq!(wire(&run(&ctx, &["SET", "b", "2"])), b"+OK\r\n");
        assert_eq!(wire(&run(&ctx, &["DEL", "a", "b", "c"])), b":2\r\n");
        assert_eq!(wire(&run(&ctx, &["EXISTS", "a", "b"])), b":0\r\n");
        assert_eq!(
            wire(&run(&ctx, &["DEL"])),
            b"-ERR wrong number of arguments for 'del' command\r\n"
        );
        assert_eq!(
            wire(&run(&ctx, &["EXISTS"])),
            b"-ERR wrong number of arguments for 'exists' command\r\n"
        );
    }

    #[test]
    fn expire_pexpire() {
        let ctx = Ctx::new();
        assert_eq!(wire(&run(&ctx, &["SET", "k", "v"])), b"+OK\r\n");
        assert_eq!(wire(&run(&ctx, &["EXPIRE", "k", "100"])), b":1\r\n");
        assert_eq!(wire(&run(&ctx, &["EXPIRE", "missing", "100"])), b":0\r\n");
        assert_eq!(
            wire(&run(&ctx, &["EXPIRE", "k", "xyz"])),
            b"-ERR value is not an integer or out of range\r\n"
        );
        // EXPIRE works on any type.
        assert_eq!(wire(&run(&ctx, &["HSET", "h", "f", "v"])), b":1\r\n");
        assert_eq!(wire(&run(&ctx, &["EXPIRE", "h", "100"])), b":1\r\n");
        assert_eq!(wire(&run(&ctx, &["SET", "k2", "v"])), b"+OK\r\n");
        assert_eq!(wire(&run(&ctx, &["PEXPIRE", "k2", "5000"])), b":1\r\n");
        // Non-positive timeout deletes the key immediately (Redis behavior).
        assert_eq!(wire(&run(&ctx, &["EXPIRE", "k", "0"])), b":1\r\n");
        assert_eq!(wire(&run(&ctx, &["GET", "k"])), b"$-1\r\n");
        assert_eq!(wire(&run(&ctx, &["PEXPIRE", "k2", "-1"])), b":1\r\n");
        assert_eq!(wire(&run(&ctx, &["GET", "k2"])), b"$-1\r\n");
    }

    #[test]
    fn ttl_semantics() {
        let ctx = Ctx::new();
        assert_eq!(wire(&run(&ctx, &["TTL", "missing"])), b":-2\r\n");
        assert_eq!(wire(&run(&ctx, &["SET", "k", "v"])), b"+OK\r\n");
        assert_eq!(wire(&run(&ctx, &["TTL", "k"])), b":-1\r\n");
        // TTL in seconds, truncated: 100s expiry -> 99 or 100 depending on
        // sub-second timing, never anything else.
        assert_eq!(
            wire(&run(&ctx, &["SET", "t", "v", "EX", "100"])),
            b"+OK\r\n"
        );
        match run(&ctx, &["TTL", "t"]) {
            CmdOut::Reply(Frame::Integer(n)) => assert!((99..=100).contains(&n), "ttl={n}"),
            other => panic!("expected integer TTL, got {other:?}"),
        }
        // Sub-second TTL truncates to 0.
        assert_eq!(wire(&run(&ctx, &["SET", "u", "v"])), b"+OK\r\n");
        assert_eq!(wire(&run(&ctx, &["PEXPIRE", "u", "500"])), b":1\r\n");
        assert_eq!(wire(&run(&ctx, &["TTL", "u"])), b":0\r\n");
    }

    #[test]
    fn incr_decr_incrby() {
        let ctx = Ctx::new();
        assert_eq!(wire(&run(&ctx, &["INCR", "c"])), b":1\r\n");
        assert_eq!(wire(&run(&ctx, &["INCRBY", "c", "4"])), b":5\r\n");
        assert_eq!(wire(&run(&ctx, &["DECR", "c"])), b":4\r\n");
        assert_eq!(
            wire(&run(&ctx, &["DECR", "c", "x"])),
            b"-ERR wrong number of arguments for 'decr' command\r\n"
        );
        assert_eq!(
            wire(&run(&ctx, &["INCRBY", "c", "xyz"])),
            b"-ERR value is not an integer or out of range\r\n"
        );
        assert_eq!(wire(&run(&ctx, &["SET", "s", "abc"])), b"+OK\r\n");
        assert_eq!(
            wire(&run(&ctx, &["INCR", "s"])),
            b"-ERR value is not an integer or out of range\r\n"
        );
        // Overflow is a NotInteger, and the value is left untouched.
        assert_eq!(
            wire(&run(&ctx, &["SET", "big", "9223372036854775807"])),
            b"+OK\r\n"
        );
        assert_eq!(
            wire(&run(&ctx, &["INCR", "big"])),
            b"-ERR value is not an integer or out of range\r\n"
        );
        assert_eq!(
            wire(&run(&ctx, &["GET", "big"])),
            b"$19\r\n9223372036854775807\r\n"
        );
    }

    #[test]
    fn hash_ops() {
        let ctx = Ctx::new();
        assert_eq!(
            wire(&run(&ctx, &["HSET", "h", "a", "1", "b", "2"])),
            b":2\r\n"
        );
        assert_eq!(wire(&run(&ctx, &["HSET", "h", "a", "10"])), b":0\r\n");
        assert_eq!(
            wire(&run(&ctx, &["HSET", "h", "a", "10", "c", "3"])),
            b":1\r\n"
        );
        assert_eq!(
            wire(&run(&ctx, &["HSET", "h", "a"])),
            b"-ERR wrong number of arguments for 'hset' command\r\n"
        );
        assert_eq!(wire(&run(&ctx, &["HGET", "h", "a"])), b"$2\r\n10\r\n");
        assert_eq!(wire(&run(&ctx, &["HGET", "h", "z"])), b"$-1\r\n");
        assert_eq!(wire(&run(&ctx, &["HGETALL", "missing"])), b"*0\r\n");
        match run(&ctx, &["HGETALL", "h"]) {
            CmdOut::Reply(Frame::Array(Some(items))) => {
                let mut pairs: Vec<(Vec<u8>, Vec<u8>)> = items
                    .as_chunks::<2>()
                    .0
                    .iter()
                    .map(|c| match (&c[0], &c[1]) {
                        (Frame::Bulk(Some(f)), Frame::Bulk(Some(v))) => (f.clone(), v.clone()),
                        _ => panic!("expected bulk pairs"),
                    })
                    .collect();
                pairs.sort();
                assert_eq!(
                    pairs,
                    vec![
                        (b"a".to_vec(), b"10".to_vec()),
                        (b"b".to_vec(), b"2".to_vec()),
                        (b"c".to_vec(), b"3".to_vec()),
                    ]
                );
            }
            other => panic!("expected array, got {other:?}"),
        }
        assert_eq!(wire(&run(&ctx, &["HDEL", "h", "a", "z"])), b":1\r\n");
        assert_eq!(wire(&run(&ctx, &["HGET", "h", "a"])), b"$-1\r\n");
        // Wrong-type paths.
        assert_eq!(wire(&run(&ctx, &["SET", "s", "v"])), b"+OK\r\n");
        assert_eq!(
            wire(&run(&ctx, &["HSET", "s", "f", "v"])),
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        );
        assert_eq!(
            wire(&run(&ctx, &["HGET", "s", "f"])),
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        );
    }

    #[test]
    fn hgetall_resp3_returns_map() {
        let ctx = Ctx::new();
        assert_eq!(
            wire(&run(&ctx, &["HSET", "h", "a", "1", "b", "2"])),
            b":2\r\n"
        );
        match run_proto(&ctx, &["HGETALL", "h"], 3) {
            CmdOut::Reply(Frame::Map(pairs)) => {
                let mut flat: Vec<(Vec<u8>, Vec<u8>)> = pairs
                    .into_iter()
                    .map(|(k, v)| match (k, v) {
                        (Frame::Bulk(Some(f)), Frame::Bulk(Some(v))) => (f, v),
                        _ => panic!("map entries must be (bulk, bulk) pairs"),
                    })
                    .collect();
                flat.sort();
                assert_eq!(
                    flat,
                    vec![
                        (b"a".to_vec(), b"1".to_vec()),
                        (b"b".to_vec(), b"2".to_vec()),
                    ]
                );
            }
            other => panic!("RESP3 HGETALL must be a map, got {other:?}"),
        }
        // Missing key -> empty map on proto 3, empty array on proto 2.
        assert_eq!(
            wire(&run_proto(&ctx, &["HGETALL", "missing"], 3)),
            b"%0\r\n"
        );
        assert_eq!(
            wire(&run_proto(&ctx, &["HGETALL", "missing"], 2)),
            b"*0\r\n"
        );
        // Arity errors are identical on both protocols.
        match run_proto(&ctx, &["HGETALL"], 3) {
            CmdOut::Reply(Frame::Error(e)) => {
                assert_eq!(e, "ERR wrong number of arguments for 'hgetall' command")
            }
            other => panic!("expected arity error, got {other:?}"),
        }
    }

    #[test]
    fn list_ops() {
        let ctx = Ctx::new();
        assert_eq!(wire(&run(&ctx, &["LPUSH", "l", "a", "b", "c"])), b":3\r\n");
        assert_eq!(
            wire(&run(&ctx, &["LRANGE", "l", "0", "-1"])),
            b"*3\r\n$1\r\nc\r\n$1\r\nb\r\n$1\r\na\r\n"
        );
        assert_eq!(
            wire(&run(&ctx, &["LRANGE", "l", "-2", "-1"])),
            b"*2\r\n$1\r\nb\r\n$1\r\na\r\n"
        );
        assert_eq!(wire(&run(&ctx, &["LRANGE", "l", "5", "9"])), b"*0\r\n");
        assert_eq!(
            wire(&run(&ctx, &["LRANGE", "missing", "0", "-1"])),
            b"*0\r\n"
        );
        assert_eq!(
            wire(&run(&ctx, &["LRANGE", "l", "0", "x"])),
            b"-ERR value is not an integer or out of range\r\n"
        );
        assert_eq!(wire(&run(&ctx, &["RPOP", "l"])), b"$1\r\na\r\n");
        assert_eq!(wire(&run(&ctx, &["RPOP", "missing"])), b"$-1\r\n");
        assert_eq!(wire(&run(&ctx, &["SET", "s", "v"])), b"+OK\r\n");
        assert_eq!(
            wire(&run(&ctx, &["LPUSH", "s", "x"])),
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        );
        assert_eq!(
            wire(&run(&ctx, &["LRANGE", "s", "0", "-1"])),
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        );
    }

    #[test]
    fn setnx() {
        let ctx = Ctx::new();
        assert_eq!(wire(&run(&ctx, &["SETNX", "k", "v"])), b":1\r\n");
        assert_eq!(wire(&run(&ctx, &["SETNX", "k", "v2"])), b":0\r\n");
        assert_eq!(wire(&run(&ctx, &["GET", "k"])), b"$1\r\nv\r\n");
    }

    #[test]
    fn publish_without_subscribers() {
        let ctx = Ctx::new();
        assert_eq!(wire(&run(&ctx, &["PUBLISH", "ch", "msg"])), b":0\r\n");
        assert_eq!(
            wire(&run(&ctx, &["PUBLISH", "ch"])),
            b"-ERR wrong number of arguments for 'publish' command\r\n"
        );
    }

    #[test]
    fn publish_with_subscriber_counts_receivers() {
        let ctx = Ctx::new();
        let _rx1 = ctx.hub.subscribe(&[b"ch".to_vec()]);
        let _rx2 = ctx.hub.subscribe(&[b"ch".to_vec()]);
        assert_eq!(wire(&run(&ctx, &["PUBLISH", "ch", "msg"])), b":2\r\n");
        assert_eq!(wire(&run(&ctx, &["PUBLISH", "other", "msg"])), b":0\r\n");
    }

    #[test]
    fn subscribe_returns_subscribe_outcome() {
        let ctx = Ctx::new();
        match run(&ctx, &["SUBSCRIBE", "a", "b"]) {
            CmdOut::Subscribe(chs) => assert_eq!(chs, vec![b"a".to_vec(), b"b".to_vec()]),
            other => panic!("expected Subscribe, got {other:?}"),
        }
        // Case-insensitive command name.
        match run(&ctx, &["subscribe", "a"]) {
            CmdOut::Subscribe(chs) => assert_eq!(chs, vec![b"a".to_vec()]),
            other => panic!("expected Subscribe, got {other:?}"),
        }
        assert_eq!(
            wire(&run(&ctx, &["SUBSCRIBE"])),
            b"-ERR wrong number of arguments for 'subscribe' command\r\n"
        );
    }

    #[test]
    fn unsubscribe_in_normal_mode() {
        let ctx = Ctx::new();
        // No channels: single confirmation with a nil channel, count 0.
        assert_eq!(
            wire(&run(&ctx, &["UNSUBSCRIBE"])),
            b"*3\r\n$11\r\nunsubscribe\r\n$-1\r\n:0\r\n"
        );
        assert_eq!(
            wire(&run(&ctx, &["UNSUBSCRIBE", "c1"])),
            b"*3\r\n$11\r\nunsubscribe\r\n$2\r\nc1\r\n:0\r\n"
        );
    }

    #[test]
    fn save_writes_snapshot() {
        let dir = tempdir();
        let aof = Aof::open(&PersistCfg {
            dir: dir.clone(),
            save_secs: 0,
            appendonly: false,
        })
        .unwrap();
        let ctx = Ctx {
            store: Store::new(),
            hub: PubHub::new(),
            aof,
        };
        assert_eq!(wire(&run(&ctx, &["SET", "k", "v"])), b"+OK\r\n");
        assert_eq!(wire(&run(&ctx, &["SAVE"])), b"+OK\r\n");
        assert!(
            dir.join("dump.rogb").exists(),
            "dump.rogb must exist after SAVE"
        );
        // Round-trip through the real loader.
        let store2 = Store::new();
        crate::persist::load(&store2, &dir).unwrap();
        assert_eq!(store2.get(b"k"), Ok(Some(b"v".to_vec())));
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn save_without_persistence_dir_is_an_error() {
        let ctx = Ctx::new(); // Aof::disabled() -> empty dir
        let out = wire(&run(&ctx, &["SAVE"]));
        assert!(
            out.starts_with(b"-ERR snapshot failed: "),
            "unexpected SAVE reply: {out:?}"
        );
    }

    #[test]
    fn quit_returns_quit_outcome() {
        let ctx = Ctx::new();
        match run(&ctx, &["QUIT"]) {
            CmdOut::Quit(f) => {
                let mut buf = Vec::new();
                encode(&f, &mut buf);
                assert_eq!(buf, b"+OK\r\n");
            }
            other => panic!("expected Quit, got {other:?}"),
        }
    }

    #[test]
    fn aof_logs_only_applied_writes() {
        let dir = tempdir();
        let aof = Aof::open(&PersistCfg {
            dir: dir.clone(),
            save_secs: 0,
            appendonly: true,
        })
        .unwrap();
        let ctx = Ctx {
            store: Store::new(),
            hub: PubHub::new(),
            aof,
        };
        // Applied: logged.
        assert_eq!(wire(&run(&ctx, &["SET", "k", "v"])), b"+OK\r\n");
        // NX fails: not a write, must not be logged.
        assert_eq!(wire(&run(&ctx, &["SET", "k", "v2", "NX"])), b"$-1\r\n");
        // DEL of a missing key: no effect, not logged.
        assert_eq!(wire(&run(&ctx, &["DEL", "missing"])), b":0\r\n");
        // INCR always applies: logged.
        assert_eq!(wire(&run(&ctx, &["INCR", "c"])), b":1\r\n");
        // GET is a read: never logged.
        assert_eq!(wire(&run(&ctx, &["GET", "k"])), b"$1\r\nv\r\n");

        ctx.aof.sync().unwrap();
        let data = fs::read(dir.join("appendonly.rogb")).unwrap();
        let mut frames = Vec::new();
        let mut pos = 0;
        while pos < data.len() {
            let (f, n) = crate::resp::decode(&data[pos..]).unwrap();
            pos += n;
            frames.push(f);
        }
        assert_eq!(
            frames.len(),
            2,
            "only the two applied writes must be logged"
        );
        for (frame, expected) in frames.iter().zip([
            vec![b"SET".to_vec(), b"k".to_vec(), b"v".to_vec()],
            vec![b"INCR".to_vec(), b"c".to_vec()],
        ]) {
            assert_eq!(crate::cmd::frame_to_argv(frame), Some(expected));
        }
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn frame_to_argv_rejects_non_bulk_frames() {
        assert_eq!(frame_to_argv(&Frame::Simple("PING".into())), None);
        assert_eq!(
            frame_to_argv(&Frame::Array(Some(vec![Frame::Integer(1)]))),
            None
        );
        assert_eq!(
            frame_to_argv(&Frame::Array(Some(vec![Frame::Bulk(Some(
                b"GET".to_vec()
            ))]))),
            Some(vec![b"GET".to_vec()])
        );
    }

    /// Decode a HELLO outcome into (proto, reply frame).
    fn hello(ctx: &Ctx, args: &[&str]) -> (u8, Frame) {
        match run(ctx, args) {
            CmdOut::Hello { proto, reply } => (proto, reply),
            other => panic!("expected Hello, got {other:?}"),
        }
    }

    /// Pull one bulk-string value out of a HELLO map reply by key.
    fn map_value<'a>(reply: &'a Frame, key: &[u8]) -> &'a Frame {
        let Frame::Map(pairs) = reply else {
            panic!("expected a Map reply, got {reply:?}");
        };
        pairs
            .iter()
            .find_map(|(k, v)| match k {
                Frame::Bulk(Some(b)) if b == key => Some(v),
                _ => None,
            })
            .unwrap_or_else(|| panic!("map reply missing key {key:?}"))
    }

    #[test]
    fn hello_defaults_to_resp2() {
        let ctx = Ctx::new();
        let (proto, reply) = hello(&ctx, &["HELLO"]);
        assert_eq!(proto, 2);
        assert_eq!(
            map_value(&reply, b"server"),
            &Frame::Bulk(Some(b"rogis".to_vec()))
        );
        assert_eq!(
            map_value(&reply, b"version"),
            &Frame::Bulk(Some(env!("CARGO_PKG_VERSION").as_bytes().to_vec()))
        );
        assert_eq!(map_value(&reply, b"proto"), &Frame::Integer(2));
        assert_eq!(
            map_value(&reply, b"mode"),
            &Frame::Bulk(Some(b"standalone".to_vec()))
        );
        assert_eq!(
            map_value(&reply, b"role"),
            &Frame::Bulk(Some(b"master".to_vec()))
        );
        assert_eq!(map_value(&reply, b"modules"), &Frame::Array(Some(vec![])));
    }

    #[test]
    fn hello_3_map_shape_is_byte_exact() {
        let ctx = Ctx::new();
        let (proto, reply) = hello(&ctx, &["HELLO", "3"]);
        assert_eq!(proto, 3);
        let mut buf = Vec::new();
        encode(&reply, &mut buf);
        let ver = env!("CARGO_PKG_VERSION");
        let ver_len = ver.len();
        // Byte-exact prefix: map header, server, version, proto entries in order.
        let expected_prefix = format!(
            "%7\r\n$6\r\nserver\r\n$5\r\nrogis\r\n$7\r\nversion\r\n${ver_len}\r\n{ver}\r\n$5\r\nproto\r\n:3\r\n"
        );
        assert!(
            buf.starts_with(expected_prefix.as_bytes()),
            "map key order/shape changed: {buf:?}"
        );
    }

    #[test]
    fn hello_ids_are_unique() {
        let ctx = Ctx::new();
        let ids: Vec<i64> = (0..10)
            .map(|_| {
                let (_, reply) = hello(&ctx, &["HELLO", "3"]);
                match map_value(&reply, b"id") {
                    Frame::Integer(id) => *id,
                    other => panic!("id must be an integer, got {other:?}"),
                }
            })
            .collect();
        let mut sorted = ids.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), ids.len(), "HELLO ids must be unique");
    }

    #[test]
    fn hello_rejects_bad_protocol_versions() {
        let ctx = Ctx::new();
        for bad in ["4", "1", "x", "2.5", "03"] {
            match run(&ctx, &["HELLO", bad]) {
                CmdOut::Reply(Frame::Error(e)) => {
                    assert_eq!(e, "ERR unknown protocol version", "for HELLO {bad}")
                }
                other => panic!("HELLO {bad} must be an error, got {other:?}"),
            }
        }
        // The good ones still work.
        assert_eq!(hello(&ctx, &["HELLO", "2"]).0, 2);
        assert_eq!(hello(&ctx, &["HELLO", "3"]).0, 3);
        // Command name is case-insensitive.
        assert_eq!(hello(&ctx, &["hello", "3"]).0, 3);
    }

    #[test]
    fn hello_auth_is_rejected_not_ignored() {
        let ctx = Ctx::new();
        // AUTH in any option position must be a hard error — silently ignoring
        // credentials would be a security hole.
        for args in [
            vec!["HELLO", "3", "AUTH", "user", "pass"],
            vec!["HELLO", "AUTH", "user", "pass"],
            vec!["HELLO", "2", "SETNAME", "app", "AUTH", "u", "p"],
        ] {
            match run(&ctx, &args) {
                CmdOut::Reply(Frame::Error(e)) => {
                    assert_eq!(e, "ERR AUTH not supported in rogis v0.1", "for {args:?}")
                }
                other => panic!("{args:?} must be an AUTH error, got {other:?}"),
            }
        }
    }

    #[test]
    fn hello_setname_is_accepted_and_ignored() {
        let ctx = Ctx::new();
        assert_eq!(hello(&ctx, &["HELLO", "3", "SETNAME", "myapp"]).0, 3);
        assert_eq!(hello(&ctx, &["HELLO", "3", "setname", "myapp"]).0, 3);
        // A name that looks like an option keyword after SETNAME is a value, not an option.
        assert_eq!(hello(&ctx, &["HELLO", "3", "SETNAME", "auth"]).0, 3);
        match run(&ctx, &["HELLO", "3", "SETNAME"]) {
            CmdOut::Reply(Frame::Error(e)) => assert_eq!(e, "ERR syntax error"),
            other => panic!("bare SETNAME must be a syntax error, got {other:?}"),
        }
        match run(&ctx, &["HELLO", "3", "BOGUS"]) {
            CmdOut::Reply(Frame::Error(e)) => assert_eq!(e, "ERR syntax error"),
            other => panic!("unknown HELLO option must be a syntax error, got {other:?}"),
        }
    }
}

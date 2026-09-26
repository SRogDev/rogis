//! rogis-bench: RESP load generator for Rogis (and any RESP2 server).
//!
//! Usage: rogis-bench [--host 127.0.0.1] [--port 6379] [--clients 16]
//!                    [--ops 100000] [--workload mixed]
//!
//! Each client task pipelines `PIPE_DEPTH` commands in flight and records
//! per-op latency into a fixed-bucket histogram (std only).

use std::collections::VecDeque;
use std::time::Instant;

use rogis::resp::{decode, Frame, RespError};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Number of commands kept in flight per client connection.
const PIPE_DEPTH: usize = 8;

/// 32-byte payload used by SET commands.
const VALUE: &[u8] = b"0123456789abcdef0123456789abcdef";

// ---------------------------------------------------------------------------
// Argument parsing
// ---------------------------------------------------------------------------

mod args {
    /// Bench configuration parsed from the CLI.
    #[derive(Debug, Clone, PartialEq)]
    pub struct Args {
        pub host: String,
        pub port: u16,
        pub clients: usize,
        pub ops: u64,
        pub workload: super::workload::Workload,
    }

    impl Default for Args {
        fn default() -> Self {
            Self {
                host: "127.0.0.1".to_string(),
                port: 6379,
                clients: 16,
                ops: 100_000,
                workload: super::workload::Workload::Mixed,
            }
        }
    }

    /// Parse CLI arguments. `argv` excludes the program name.
    pub fn parse(argv: &[String]) -> Result<Args, String> {
        let mut args = Args::default();
        let mut i = 0;
        while i < argv.len() {
            let flag = argv[i].as_str();
            let value = |i: usize, flag: &str| -> Result<&str, String> {
                argv.get(i)
                    .map(String::as_str)
                    .ok_or_else(|| format!("{flag} needs a value"))
            };
            match flag {
                "--host" => {
                    i += 1;
                    args.host = value(i, flag)?.to_string();
                }
                "--port" => {
                    i += 1;
                    args.port = value(i, flag)?
                        .parse::<u16>()
                        .map_err(|_| format!("bad --port value '{}'", argv[i]))?;
                }
                "--clients" => {
                    i += 1;
                    args.clients = value(i, flag)?
                        .parse::<usize>()
                        .map_err(|_| format!("bad --clients value '{}'", argv[i]))?;
                    if args.clients == 0 {
                        return Err("--clients must be at least 1".to_string());
                    }
                }
                "--ops" => {
                    i += 1;
                    args.ops = value(i, flag)?
                        .parse::<u64>()
                        .map_err(|_| format!("bad --ops value '{}'", argv[i]))?;
                    if args.ops == 0 {
                        return Err("--ops must be at least 1".to_string());
                    }
                }
                "--workload" => {
                    i += 1;
                    args.workload = super::workload::Workload::parse(value(i, flag)?)?;
                }
                "--help" | "-h" => return Err("help".to_string()),
                other => return Err(format!("unknown argument: '{other}'")),
            }
            i += 1;
        }
        Ok(args)
    }

    #[allow(dead_code)]
    pub fn usage() -> &'static str {
        "usage: rogis-bench [--host 127.0.0.1] [--port 6379] [--clients 16] [--ops 100000] [--workload mixed|set_get|incr|hset]"
    }
}

// ---------------------------------------------------------------------------
// Deterministic RNG (xorshift64*, no dependencies)
// ---------------------------------------------------------------------------

mod rng {
    /// Tiny deterministic PRNG so benchmark runs are reproducible.
    ///
    /// xorshift64* (Marsaglia). Never seeded with 0.
    pub struct XorShift(u64);

    impl XorShift {
        pub fn new(seed: u64) -> Self {
            Self(if seed == 0 {
                0x9E37_79B9_7F4A_7C15
            } else {
                seed
            })
        }

        pub fn next(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x >> 12;
            x ^= x << 25;
            x ^= x >> 27;
            self.0 = x;
            x.wrapping_mul(0x2545_F491_4F6C_DD1D)
        }

        /// Uniform value in `[0, bound)`.
        pub fn below(&mut self, bound: u64) -> u64 {
            if bound == 0 {
                return 0;
            }
            // Multiply-high: unbiased without a division.
            ((self.next() as u128 * bound as u128) >> 64) as u64
        }
    }
}

// ---------------------------------------------------------------------------
// Fixed-bucket latency histogram (std only)
// ---------------------------------------------------------------------------

mod hist {
    /// Log-spaced histogram over latencies in nanoseconds.
    ///
    /// Bucket edges start at 1µs and multiply by 1.25 up to 30s; anything
    /// larger lands in the overflow bucket.
    pub struct Histogram {
        edges: Vec<u64>,
        counts: Vec<u64>,
        total: u64,
        sum_ns: u64,
        min_ns: u64,
        max_ns: u64,
    }

    impl Default for Histogram {
        fn default() -> Self {
            Self::new()
        }
    }

    impl Histogram {
        pub fn new() -> Self {
            // 1µs * 1.25^k up to 30s: ~70 buckets, 25% relative resolution.
            let mut edges = Vec::new();
            let mut e: f64 = 1_000.0;
            while e <= 30_000_000_000.0 {
                edges.push(e as u64);
                e *= 1.25;
            }
            let counts = vec![0u64; edges.len() + 1];
            Self {
                edges,
                counts,
                total: 0,
                sum_ns: 0,
                min_ns: u64::MAX,
                max_ns: 0,
            }
        }

        /// Index of the bucket a sample belongs to.
        #[allow(dead_code)]
        pub fn bucket_index(&self, ns: u64) -> usize {
            // First edge >= ns; overflow bucket when ns exceeds every edge.
            self.edges.partition_point(|&edge| edge < ns)
        }

        pub fn record(&mut self, ns: u64) {
            let idx = self.bucket_index(ns);
            self.counts[idx] += 1;
            self.total += 1;
            self.sum_ns = self.sum_ns.saturating_add(ns);
            self.min_ns = self.min_ns.min(ns);
            self.max_ns = self.max_ns.max(ns);
        }

        pub fn count(&self) -> u64 {
            self.total
        }

        #[allow(dead_code)]
        pub fn mean_ns(&self) -> f64 {
            if self.total == 0 {
                0.0
            } else {
                self.sum_ns as f64 / self.total as f64
            }
        }

        #[allow(dead_code)]
        pub fn min_ns(&self) -> u64 {
            if self.total == 0 {
                0
            } else {
                self.min_ns
            }
        }

        #[allow(dead_code)]
        pub fn max_ns(&self) -> u64 {
            self.max_ns
        }

        /// Upper edge of the bucket holding the `p`-th percentile
        /// (`p` in 0.0..=1.0). Returns 0 on an empty histogram.
        pub fn percentile(&self, p: f64) -> u64 {
            if self.total == 0 {
                return 0;
            }
            let rank = (p.clamp(0.0, 1.0) * self.total as f64).ceil() as u64;
            let rank = rank.max(1);
            let mut cum = 0u64;
            for (i, &c) in self.counts.iter().enumerate() {
                cum += c;
                if cum >= rank {
                    return *self.edges.get(i).unwrap_or(&self.max_ns);
                }
            }
            self.max_ns
        }

        pub fn merge(&mut self, other: &Histogram) {
            debug_assert_eq!(self.edges, other.edges);
            for (a, b) in self.counts.iter_mut().zip(other.counts.iter()) {
                *a += b;
            }
            self.total += other.total;
            self.sum_ns = self.sum_ns.saturating_add(other.sum_ns);
            if other.total > 0 {
                self.min_ns = self.min_ns.min(other.min_ns);
                self.max_ns = self.max_ns.max(other.max_ns);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Workload command generation (pure; no I/O)
// ---------------------------------------------------------------------------

mod workload {
    use super::rng::XorShift;
    use super::VALUE;

    /// Benchmark workloads.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Workload {
        SetGet,
        Incr,
        Hset,
        Mixed,
    }

    impl Workload {
        pub fn parse(s: &str) -> Result<Workload, String> {
            match s {
                "set_get" => Ok(Workload::SetGet),
                "incr" => Ok(Workload::Incr),
                "hset" => Ok(Workload::Hset),
                "mixed" => Ok(Workload::Mixed),
                other => Err(format!(
                    "unknown workload '{other}' (expected set_get|incr|hset|mixed)"
                )),
            }
        }

        pub fn name(&self) -> &'static str {
            match self {
                Workload::SetGet => "set_get",
                Workload::Incr => "incr",
                Workload::Hset => "hset",
                Workload::Mixed => "mixed",
            }
        }
    }

    fn key(rng: &mut XorShift, prefix: &[u8], space: u64) -> Vec<u8> {
        let mut k = Vec::with_capacity(prefix.len() + 8);
        k.extend_from_slice(prefix);
        k.extend_from_slice(rng.below(space).to_string().as_bytes());
        k
    }

    fn cmd(parts: &[&[u8]]) -> Vec<Vec<u8>> {
        parts.iter().map(|p| p.to_vec()).collect()
    }

    /// Generate one command argv for this workload using `rng`.
    pub fn next_cmd(workload: Workload, rng: &mut XorShift, _op_index: u64) -> Vec<Vec<u8>> {
        match workload {
            Workload::SetGet => {
                let k = key(rng, b"bench:k:", 10_000);
                if rng.below(2) == 0 {
                    cmd(&[b"SET", &k, VALUE])
                } else {
                    cmd(&[b"GET", &k])
                }
            }
            Workload::Incr => {
                let k = key(rng, b"bench:c:", 8);
                cmd(&[b"INCR", &k])
            }
            Workload::Hset => {
                let k = key(rng, b"bench:h:", 1_000);
                if rng.below(2) == 0 {
                    cmd(&[
                        b"HSET", &k, b"f0", VALUE, b"f1", VALUE, b"f2", VALUE, b"f3", VALUE,
                    ])
                } else {
                    cmd(&[b"HGETALL", &k])
                }
            }
            Workload::Mixed => {
                // 40% SET, 30% GET, 10% INCR, 10% HSET, 10% EXPIRE.
                match rng.below(100) {
                    0..=39 => {
                        let k = key(rng, b"bench:k:", 10_000);
                        cmd(&[b"SET", &k, VALUE])
                    }
                    40..=69 => {
                        let k = key(rng, b"bench:k:", 10_000);
                        cmd(&[b"GET", &k])
                    }
                    70..=79 => {
                        let k = key(rng, b"bench:c:", 8);
                        cmd(&[b"INCR", &k])
                    }
                    80..=89 => {
                        let k = key(rng, b"bench:h:", 1_000);
                        cmd(&[b"HSET", &k, b"f0", VALUE, b"f1", VALUE])
                    }
                    _ => {
                        let k = key(rng, b"bench:k:", 10_000);
                        cmd(&[b"EXPIRE", &k, b"60"])
                    }
                }
            }
        }
    }

    #[allow(dead_code)]
    pub fn all() -> [Workload; 4] {
        [
            Workload::SetGet,
            Workload::Incr,
            Workload::Hset,
            Workload::Mixed,
        ]
    }
}

// ---------------------------------------------------------------------------
// RESP client plumbing
// ---------------------------------------------------------------------------

/// Encode one command as a RESP array of bulk strings.
fn encode_cmd(argv: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    out.extend_from_slice(format!("*{}\r\n", argv.len()).as_bytes());
    for arg in argv {
        out.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
        out.extend_from_slice(arg);
        out.extend_from_slice(b"\r\n");
    }
    out
}

/// Read and discard (counting) one RESP reply frame from the stream.
async fn read_reply(stream: &mut TcpStream, buf: &mut Vec<u8>) -> Result<Option<Frame>, String> {
    loop {
        match decode(buf) {
            Ok((frame, n)) => {
                buf.drain(..n);
                return Ok(Some(frame));
            }
            Err(RespError::Incomplete) => {
                let mut chunk = [0u8; 8192];
                match stream.read(&mut chunk).await {
                    Ok(0) => return Ok(None),
                    Ok(n) => buf.extend_from_slice(&chunk[..n]),
                    Err(e) => return Err(format!("read error: {e}")),
                }
            }
            Err(RespError::Invalid(msg)) => return Err(format!("protocol error: {msg}")),
        }
    }
}

/// A reply counts as an error when it is an `Error` frame.
fn is_error_reply(frame: &Frame) -> bool {
    matches!(frame, Frame::Error(_))
}

/// One load-generating client: pipelines PIPE_DEPTH commands, measures
/// per-op latency from send to reply receipt.
async fn client_task(
    args: args::Args,
    client_id: usize,
    ops: u64,
    barrier: std::sync::Arc<tokio::sync::Barrier>,
) -> ClientStats {
    let mut stats = ClientStats::default();
    let mut stream = match TcpStream::connect((args.host.as_str(), args.port)).await {
        Ok(s) => s,
        Err(e) => {
            stats.protocol_errors = ops;
            eprintln!("rogis-bench: client {client_id}: connect failed: {e}");
            return stats;
        }
    };
    let _ = stream.set_nodelay(true);
    let mut rng = rng::XorShift::new(
        0x1234_5678_9ABC_DEF0 ^ (client_id as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15),
    );
    barrier.wait().await;

    let mut in_flight: VecDeque<Instant> = VecDeque::with_capacity(PIPE_DEPTH);
    let mut buf: Vec<u8> = Vec::new();
    let mut sent = 0u64;
    let mut recvd = 0u64;

    while recvd < ops {
        // Fill the pipeline.
        while sent < ops && in_flight.len() < PIPE_DEPTH {
            let cmd = workload::next_cmd(args.workload, &mut rng, sent);
            let bytes = encode_cmd(&cmd);
            if let Err(e) = stream.write_all(&bytes).await {
                eprintln!("rogis-bench: client {client_id}: write failed: {e}");
                stats.protocol_errors += ops - recvd;
                return stats;
            }
            in_flight.push_back(Instant::now());
            sent += 1;
        }
        // Drain one reply; replies arrive in send order.
        match read_reply(&mut stream, &mut buf).await {
            Ok(Some(frame)) => {
                recvd += 1;
                if let Some(t0) = in_flight.pop_front() {
                    let ns = u64::try_from(t0.elapsed().as_nanos()).unwrap_or(u64::MAX);
                    stats.hist.record(ns);
                }
                if is_error_reply(&frame) {
                    stats.errors += 1;
                }
            }
            Ok(None) => {
                eprintln!("rogis-bench: client {client_id}: server closed connection");
                stats.protocol_errors += ops - recvd;
                return stats;
            }
            Err(e) => {
                eprintln!("rogis-bench: client {client_id}: {e}");
                stats.protocol_errors += ops - recvd;
                return stats;
            }
        }
    }
    stats
}

#[derive(Default)]
struct ClientStats {
    hist: hist::Histogram,
    errors: u64,
    protocol_errors: u64,
}

#[tokio::main]
async fn main() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let args = match args::parse(&argv) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("rogis-bench: {e}");
            eprintln!("{}", args::usage());
            std::process::exit(2);
        }
    };

    // Split total ops across clients as evenly as possible.
    let base = args.ops / args.clients as u64;
    let extra = (args.ops % args.clients as u64) as usize;

    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(args.clients + 1));
    let mut handles = Vec::with_capacity(args.clients);
    for id in 0..args.clients {
        let a = args.clone();
        let b = barrier.clone();
        let ops = base + usize::from(id < extra) as u64;
        handles.push(tokio::spawn(
            async move { client_task(a, id, ops, b).await },
        ));
    }
    barrier.wait().await;
    let start = Instant::now();
    let mut hist = hist::Histogram::new();
    let mut errors = 0u64;
    let mut proto_errors = 0u64;
    for h in handles {
        match h.await {
            Ok(s) => {
                hist.merge(&s.hist);
                errors += s.errors;
                proto_errors += s.protocol_errors;
            }
            Err(e) => {
                eprintln!("rogis-bench: client task panicked: {e}");
                proto_errors += 1;
            }
        }
    }
    let elapsed = start.elapsed();
    let done = hist.count();
    let ops_per_sec = if elapsed.as_secs_f64() > 0.0 {
        done as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };

    println!(
        "workload={} clients={} ops={} done={} elapsed={:.2}s ops/s={:.0} p50={} p99={} errors={} proto_errors={}",
        args.workload.name(),
        args.clients,
        args.ops,
        done,
        elapsed.as_secs_f64(),
        ops_per_sec,
        fmt_ns(hist.percentile(0.5)),
        fmt_ns(hist.percentile(0.99)),
        errors,
        proto_errors,
    );
    // Machine-readable line for benches/parity.sh.
    println!(
        "RESULT workload={} ops_per_sec={:.0} p50_ns={} p99_ns={} errors={} proto_errors={}",
        args.workload.name(),
        ops_per_sec,
        hist.percentile(0.5),
        hist.percentile(0.99),
        errors,
        proto_errors,
    );
    if proto_errors > 0 {
        std::process::exit(1);
    }
}

fn fmt_ns(ns: u64) -> String {
    if ns < 1_000 {
        format!("{ns}ns")
    } else if ns < 1_000_000 {
        format!("{:.1}µs", ns as f64 / 1_000.0)
    } else if ns < 1_000_000_000 {
        format!("{:.2}ms", ns as f64 / 1_000_000.0)
    } else {
        format!("{:.2}s", ns as f64 / 1_000_000_000.0)
    }
}

// ---------------------------------------------------------------------------
// Tests (TDD)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::args;
    use super::hist::Histogram;
    use super::rng::XorShift;
    use super::workload::{next_cmd, Workload};

    fn argv(parts: &[&str]) -> Vec<String> {
        parts.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn args_default() {
        let a = args::parse(&[]).unwrap();
        assert_eq!(a.host, "127.0.0.1");
        assert_eq!(a.port, 6379);
        assert_eq!(a.clients, 16);
        assert_eq!(a.ops, 100_000);
        assert_eq!(a.workload, Workload::Mixed);
    }

    #[test]
    fn args_overrides() {
        let a = args::parse(&argv(&[
            "--host",
            "10.0.0.5",
            "--port",
            "6390",
            "--clients",
            "4",
            "--ops",
            "1000",
            "--workload",
            "set_get",
        ]))
        .unwrap();
        assert_eq!(a.host, "10.0.0.5");
        assert_eq!(a.port, 6390);
        assert_eq!(a.clients, 4);
        assert_eq!(a.ops, 1000);
        assert_eq!(a.workload, Workload::SetGet);
    }

    #[test]
    fn args_workload_names() {
        for (name, w) in [
            ("set_get", Workload::SetGet),
            ("incr", Workload::Incr),
            ("hset", Workload::Hset),
            ("mixed", Workload::Mixed),
        ] {
            assert_eq!(Workload::parse(name).unwrap(), w, "parse {name}");
            assert_eq!(
                args::parse(&argv(&["--workload", name])).unwrap().workload,
                w
            );
        }
    }

    #[test]
    fn args_rejects_bad_input() {
        assert!(args::parse(&argv(&["--port", "abc"])).is_err());
        assert!(args::parse(&argv(&["--port", "99999"])).is_err());
        assert!(args::parse(&argv(&["--clients", "0"])).is_err());
        assert!(args::parse(&argv(&["--ops", "0"])).is_err());
        assert!(args::parse(&argv(&["--workload", "bogus"])).is_err());
        assert!(args::parse(&argv(&["--host"])).is_err());
        assert!(args::parse(&argv(&["--frobnicate"])).is_err());
    }

    #[test]
    fn rng_is_deterministic() {
        let mut a = XorShift::new(42);
        let mut b = XorShift::new(42);
        for _ in 0..1000 {
            assert_eq!(a.next(), b.next());
        }
        let mut c = XorShift::new(43);
        assert_ne!(a.next(), c.next());
    }

    #[test]
    fn rng_below_is_in_range() {
        let mut r = XorShift::new(7);
        for _ in 0..10_000 {
            assert!(r.below(100) < 100);
        }
        let mut r = XorShift::new(7);
        for _ in 0..100 {
            assert_eq!(r.below(1), 0);
        }
    }

    #[test]
    fn histogram_empty() {
        let h = Histogram::new();
        assert_eq!(h.count(), 0);
        assert_eq!(h.percentile(0.5), 0);
        assert_eq!(h.percentile(0.99), 0);
    }

    #[test]
    fn histogram_percentiles_bracket_true_values() {
        let mut h = Histogram::new();
        // 1µs..100ms in exact 1µs steps: true p50 ≈ 50ms/2... use a clean set:
        // values 1000, 2000, ..., 100_000 ns (100 samples).
        for i in 1..=100u64 {
            h.record(i * 1000);
        }
        assert_eq!(h.count(), 100);
        let p50 = h.percentile(0.5);
        let p99 = h.percentile(0.99);
        // Buckets are log-spaced (×1.25), so the reported edge must be within
        // one bucket width above the true quantile.
        assert!(p50 >= 50_000, "p50 edge {p50} must cover true 50_000");
        assert!(p50 <= 50_000 * 125 / 100, "p50 edge {p50} too coarse");
        assert!(p99 >= 99_000, "p99 edge {p99} must cover true 99_000");
        assert!(p99 <= 99_000 * 125 / 100, "p99 edge {p99} too coarse");
    }

    #[test]
    fn histogram_merge_and_min_max() {
        let mut a = Histogram::new();
        let mut b = Histogram::new();
        a.record(5_000);
        a.record(7_000);
        b.record(50_000);
        a.merge(&b);
        assert_eq!(a.count(), 3);
        assert_eq!(a.min_ns(), 5_000);
        assert_eq!(a.max_ns(), 50_000);
        assert!(a.mean_ns() > 20_000.0 && a.mean_ns() < 21_000.0);
    }

    #[test]
    fn workload_set_get_shape() {
        let mut rng = XorShift::new(1);
        let mut saw_set = false;
        let mut saw_get = false;
        for i in 0..200 {
            let cmd = next_cmd(Workload::SetGet, &mut rng, i);
            match cmd[0].as_slice() {
                b"SET" => {
                    saw_set = true;
                    assert_eq!(cmd.len(), 3);
                    assert_eq!(cmd[2].len(), 32);
                }
                b"GET" => {
                    saw_get = true;
                    assert_eq!(cmd.len(), 2);
                }
                other => panic!("unexpected command {other:?}"),
            }
        }
        assert!(saw_set && saw_get, "set_get must emit both SET and GET");
    }

    #[test]
    fn workload_incr_shape() {
        let mut rng = XorShift::new(2);
        for i in 0..50 {
            let cmd = next_cmd(Workload::Incr, &mut rng, i);
            assert_eq!(cmd[0].as_slice(), b"INCR");
            assert_eq!(cmd.len(), 2);
        }
    }

    #[test]
    fn workload_hset_shape() {
        let mut rng = XorShift::new(3);
        let mut saw_hset = false;
        let mut saw_hgetall = false;
        for i in 0..200 {
            let cmd = next_cmd(Workload::Hset, &mut rng, i);
            match cmd[0].as_slice() {
                b"HSET" => {
                    saw_hset = true;
                    assert!(
                        cmd.len() >= 4,
                        "HSET needs command + key + field/value pairs"
                    );
                    assert_eq!(
                        (cmd.len() - 2) % 2,
                        0,
                        "HSET needs field/value pairs after the key"
                    );
                }
                b"HGETALL" => {
                    saw_hgetall = true;
                    assert_eq!(cmd.len(), 2);
                }
                other => panic!("unexpected command {other:?}"),
            }
        }
        assert!(saw_hset && saw_hgetall);
    }

    #[test]
    fn workload_mixed_covers_all_ops() {
        let mut rng = XorShift::new(4);
        let mut seen = std::collections::HashSet::new();
        for i in 0..2000 {
            let cmd = next_cmd(Workload::Mixed, &mut rng, i);
            seen.insert(cmd[0].clone());
        }
        for op in [b"SET".as_slice(), b"GET", b"INCR", b"HSET", b"EXPIRE"] {
            assert!(seen.contains(op), "mixed never emitted {op:?}");
        }
    }

    #[test]
    fn workload_deterministic() {
        let mut a = XorShift::new(9);
        let mut b = XorShift::new(9);
        for i in 0..500 {
            assert_eq!(
                next_cmd(Workload::Mixed, &mut a, i),
                next_cmd(Workload::Mixed, &mut b, i)
            );
        }
    }

    #[test]
    fn encode_cmd_wire_format() {
        let bytes = super::encode_cmd(&[b"SET".to_vec(), b"k".to_vec(), b"v".to_vec()]);
        assert_eq!(bytes, b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n");
    }
}

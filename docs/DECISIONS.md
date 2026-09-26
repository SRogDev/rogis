# Rogis — Phase 0 Spike Decisions

> Evidence, not intuition. All spikes are disposable prototypes in `~/workspace/rogis-spikes/`
> (never pushed). Environment: 2-core / 7.7 GB VM, Rust 1.98.1, release builds.
> Spike code deliberately skips TDD (per plan: spikes are exempt; product code is not).

## A. Crate evaluation — recommendations

| Category | Pick | Version (2026-09-26) | Why |
|---|---|---|---|
| Async runtime | **tokio** (multi-thread flavor) | 1.53.1 | De facto standard; no serious alternative for this workload. Decided by research. |
| HNSW index | **hnsw_rs** | 0.3.4 | Only pure-Rust crate with true incremental insert + parallel insert/search + dump/reload. See §B. |
| Concurrent KV | **std sharded `Mutex<HashMap>`** (16+ shards) | std | Matches dashmap/parking_lot throughput (§D), zero extra deps, and each shard behaves like a mini single-threaded Redis — simplest correct mental model for the locked sharded-store design. |
| RESP parsing | **hand-rolled parser** (hot path) + `redis-protocol` as conformance oracle in dev-deps | redis-protocol 6.0.0 | Hand-rolled is ~10x faster and 40 lines (§C). RESP2 command shape (array of bulk strings) is genuinely simple; full RESP2/3 only needed for response *encoding*, which is trivial to hand-roll. Use the crate in tests to verify our wire bytes decode identically. |
| Embeddings (phase 2) | **ort** | 2.0.0-rc.13 (note: 2.0 still RC) | See §E. |

### HNSW alternatives considered and rejected

- **instant-distance 0.6.1** (Dirkjan Ochtman): compact and readable, but its public API
  (`Builder::build(points, values)`) is **batch-only — no incremental insert exists**.
  For a cache workload (`SEMSET` = continuous incremental inserts), adopting it means
  rebuilding the whole graph per insert — disqualifying at scale. Confirmed by source
  inspection + independent field reports (ctxd ADR: "rebuilt the entire graph on every
  insert… intolerable at tens of thousands"). Measured batch-build numbers in §B.
- **usearch 2.26.2** (Rust bindings over C++): very fast, but pulls a native C++
  dependency into every contributor's build. Revisit if hnsw_rs becomes a bottleneck;
  pure Rust keeps the contributor story clean for v0.1.
- **fast-hnsw** (nataliyakosmyna): claims 1.5–3.7× over hnsw_rs in self-published
  benchmarks, MIT. Single-author, young — watch, don't depend on yet.

## B. HNSW measurements (the differentiator)

Setup: `hnsw_rs` 0.3.4, cosine distance, M=16, ef_construction=200, synthetic unit
vectors, release build, 2-core VM. `instant-distance` 0.6.1 with its default builder.

### Insert scaling (dim=128) — hnsw_rs incremental vs instant-distance batch

| n | hnsw_rs incremental (per 1k batch) | instant-distance full rebuild |
|---|---|---|
| 1,000 | 1.3 s | **10.1 s** |
| 5,000 | 3.8 s | **70.0 s** |
| 10,000 | 7.3 s | (not run — extrapolated >5 min) |
| 20,000 | 16.6 s | (not run) |

Single-vector incremental insert latency at n=20k: **~18.6 ms**.

Two independent disqualifiers for `instant-distance`: (1) its public API is
batch-only (`Builder::build(all_points, all_values)`) — no incremental insert
exists, so every `SEMSET` would rebuild the whole graph; (2) it is ~8× slower
than hnsw_rs at build even in batch mode, scaling superlinearly. The ctxd
project's field report ("rebuilt the entire graph on every insert… intolerable
at tens of thousands") is confirmed by measurement.

hnsw_rs per-insert cost grows sublinearly (1.3 ms → ~17 ms from n=1k to n=20k at
dim=128, ef_construction=200). ms-scale writes are acceptable for a cache whose
writes happen on LLM-call misses (seconds + cents): the index write is never the
expensive part of a miss. Bulk loads should use `parallel_insert`.

### 100k vectors @ 768 dims (hnsw_rs)

<!--RESULTS-BIG-->

## C. RESP parsing

Synthetic pipeline of 10,000 `SET` commands (410 KB), release build:

| Parser | Throughput |
|---|---|
| `redis-protocol` 6.0.0 `decode` (owned frames) | ~2.9M cmd/s |
| `redis-protocol` 6.0.0 `decode_bytes` (borrowed, `bytes` feature) | ~2.4M cmd/s |
| hand-rolled zero-copy parser (arrays of bulk strings) | **~30.7M cmd/s** |

Parsing will not be the bottleneck (network I/O dominates far earlier), but the
hand-rolled parser is 40 lines, zero-copy, and 10× faster. Decision: hand-roll the
request parser; use `redis-protocol` as a test oracle for wire compatibility.

## D. Concurrent map strategies

8 threads, 50/50 read/write mix, 10k keys, release build:

| Strategy | Throughput |
|---|---|
| `dashmap` 6.2.1 | ~18.0M ops/s |
| `std Mutex<HashMap>` (single global lock) | ~10.6M ops/s |
| `parking_lot 0.12 RwLock<HashMap>` | ~19.3M ops/s |
| **16-shard `Mutex<HashMap>` (Rogis design)** | **~19.9M ops/s** |

The sharded design matches the best concurrent maps with zero extra dependencies and
no lock-ordering discipline to get wrong. Caveat: 2-core VM — on 16 cores the global
lock would collapse further while sharding scales. Validates the locked decision:
sharded store, per-shard locks, Tokio multi-thread.

## E. ONNX probe (phase-2 feasibility)

`ort` 2.0.0-rc.13 + all-MiniLM-L6-v2 (90 MB), CPU, 2-core VM, release build.
The `ort-sys` build script downloads prebuilt ONNX Runtime from GitHub releases —
worked through the proxy without intervention.

| Measurement | Result |
|---|---|
| Tokenizer load | 0.16 s |
| Session load (90 MB model) | 1.52 s |
| Output dim | 384 ✓ (L2 norm 5.37 — sane) |
| Inference latency, short sentence, CPU | **~130 ms** |

Implication for phase 2: server-side embedding is feasible (build works, model
loads fast), but 130 ms CPU inference would **dominate** a `SEMGET`-by-text path
whose HNSW lookup is sub-millisecond. Phase-2 options: keep embeddings
client-side (locked for MVP), batch server-side inference, or ship a smaller /
quantized model. The number to beat is the client's own embedding call — which
they already pay today.

## F. Central thesis verdict

Per-op serialization tax for one 768-dim vector (release build):

| Path | Cost |
|---|---|
| JSON round-trip (what Python libs over Redis actually do) | **~72 µs** (42 µs ser + 31 µs de) |
| bincode round-trip (best-case binary) | ~2.3 µs |
| Native f32 array (zero-copy reinterpret, what Rogis does) | ~0 |

A "semantic cache as a library over Redis" pays this tax on **every** vector
store/retrieve, *plus* a network round-trip (hundreds of µs localhost, ms remote)
that a native engine avoids entirely. Even against best-case bincode, native
removes ~2.3 µs of pure overhead per op; against the JSON that real Python
stacks use, ~72 µs per op. **Thesis validated on the serialization axis.**
The dominant term in practice is the network hop — end-to-end vs real Redis
still to measure in Phase 1 (no `redis-server` binary in this environment).

## G. Remaining uncertainties

- End-to-end comparison against **real Redis** (storing the vector as a serialized
  string) was not possible: no `redis-server` binary in this environment. Phase 1
  must run the parity benchmark (same machine, same RESP protocol) before claiming wins.
- HNSW numbers above are on synthetic uniform data on 2 cores; recall/latency on
  real embedding distributions (clustered) and larger core counts still to measure.
- `ort` 2.0 is still release-candidate — pin the RC for phase 2 and re-evaluate at
  2.0 stable.

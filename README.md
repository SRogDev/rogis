# Rogis

**The cache-database designed for AI-agent traffic, not human traffic.**

Redis was designed in 2009 for web apps with humans behind them — punctual, structured, low-semantic-redundancy requests. Most new traffic being built looks different: AI agents that iterate, retry, explore multiple paths for the same task, and generate natural-language queries instead of structured keys.

Rogis starts from a simple thesis: there is room for a cache-database built from scratch for that access pattern — high redundancy, fuzzy instead of exact queries, and a much higher cost of a miss (a miss can trigger an LLM call of several seconds and cents, not just a millisecond SQL query).

## Two non-negotiable layers

- **Deterministic layer (Redis parity):** a drop-in Redis replacement for what agents also need — session state, rate limiting, queues, locks, pub/sub. Speaks RESP; works with existing clients (`redis-py`, `ioredis`, `go-redis`).
- **Semantic layer (the differentiator):** native vector storage inside the engine — no generic byte serialization — with similarity search as a first-class primitive.

> Rogis is a cache, deliberately. Not a general-purpose vector DB for large-scale RAG, not a multi-model database, not an agent orchestration framework.

## Status

- **Phase 0 done (2026-09-26):** technical spikes → `docs/DECISIONS.md` (tokio multi-thread, hnsw_rs 0.3.4 + simdeez_f SIMD, 16-shard `Mutex<HashMap>`, hand-rolled RESP + redis-protocol oracle, client-computed embeddings in MVP).
- **Phase 1 done (2026-09-26):** deterministic layer — ~5,700 lines of Rust, real RESP2/RESP3, snapshot + AOF persistence, **123/123 tests**, Clippy clean, verified against real `redis-py`, ~68k ops/s.
- **Next:** semantic engine (HNSW, `SEMSET`/`SEMGET`) — not started. Definition of done: one-command start on 6379 with real Redis clients + `docs/INTEGRATION.md`.

## Quickstart

```bash
cargo run --release --bin rogis -- --port 6379   # one-command start, speaks RESP on 6379
```

```bash
redis-cli -p 6379
127.0.0.1:6379> SET hello rogis
OK
```

```python
import redis
r = redis.Redis(host="127.0.0.1", port=6379, decode_responses=True)
r.set("hello", "rogis")  # True
```

Flags: `--port`, `--dir ./data`, `--save 60`, `--appendonly yes|no`. See `docs/INTEGRATION.md` (locks, cache-aside, rate limiting, sessions, queues, pub/sub) and `benches/parity.sh` (throughput harness).

## License

No license file yet.

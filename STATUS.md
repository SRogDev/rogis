# STATUS — rogis

> Single source of truth for where this project stands. Last updated: 2026-10-06.
> Read this before starting work. Update it in the same PR when reality changes.

## Done
- 2026-09-26 — Phase 0 spikes → `docs/DECISIONS.md` (tokio multi-thread, hnsw_rs 0.3.4 + simdeez_f SIMD, 16-shard Mutex<HashMap>, hand-rolled RESP + redis-protocol oracle, client-computed embeddings in MVP).
- 2026-09-26 — Phase 1 deterministic layer DONE: ~5,700 lines Rust, real RESP2/RESP3, snapshot + AOF, 123/123 tests, Clippy clean; verified against real redis-py; ~68k ops/s.

## In progress / blocked
- Semantic engine next (HNSW, SEMSET/SEMGET) — not started.

## Next
- Definition of done: one-command start on 6379 with real redis clients + `docs/INTEGRATION.md`.
- Then: semantic engine, benchmarks vs real Redis, public launch.

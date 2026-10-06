# rogis — Project Brief

> Full project context in one file. Hand this to ANOTHER AI (GPT, etc.) for planning
> and ideation, then bring the refined specs back. Keep this file accurate — it is the handoff doc.
> For the current timeline see `STATUS.md`. For how to work in this repo see `AGENTS.md`.

## One-liner
Rogis is Roger's own Redis: an AI-agent-oriented cache-database — a deterministic Redis superset (RESP, drop-in) with a native semantic engine.

## Problem & audience
Agents need a cache-database that speaks Redis but also understands vectors natively; bolting vectors onto Redis is awkward and expensive.

## Product (what it is / is not)
Network service like Redis (drop-in RESP compatibility) + vectors first-class (HNSW, SEMSET/SEMGET). Deterministic Redis superset first (parity), semantic engine second. Semantic miss = plain no-hit. Audience: production startups + open-source community (NOT his SaaS initially).

## Key decisions (locked)
- License: MIT. Positioning: 'Redis for agents' (parity first).
- Network service like Redis; Redis-style persistence from MVP.
- Client-computed embeddings in MVP (server-side later).
- Real reproducible benchmarks (script in repo) — no invented numbers.
- Critical invariants in `docs/DECISIONS.md`: RESP3 nil encoding (`_\r\n`, never `$-1`/`*-1`; `HGETALL` returns a real RESP3 map on proto 3) and atomic snapshot+AOF truncation (replay-twice bug).
- Command semantics match real Redis exactly (`SET k v NX XX` with both flags → nil; bare UNSUBSCRIBE in subscriber mode → normal mode).

## Stack
Rust (tokio, hnsw_rs 0.3.4 + simdeez_f SIMD), hand-rolled RESP codec, redis-protocol oracle tests.

## Business model
Open-source community play first; monetization later. Audience: production startups.

## Open questions
- Semantic engine design (HNSW integration, SEMSET/SEMGET semantics).
- Benchmark story vs real Redis for the launch.

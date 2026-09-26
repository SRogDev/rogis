# Rogis — Build Plan

> Living document. Decisions locked on 2026-09-26 during planning with Roger.
> Full strategic context (competitive map, command specs, benchmark design, risks):
> `docs/rogis-plan-estrategico.pdf` (Spanish).

## Product thesis

Redis was built to serve humans through applications. **Rogis is built to serve AI agents acting on their own.** That implies concrete, verifiable technical decisions:

| Dimension | Human traffic (classic Redis) | Agent traffic (Rogis) |
|---|---|---|
| Query shape | Exact structured key (`user:123`) | Mix of exact keys and natural language / prompts |
| Redundancy | Low — each request usually distinct | High — an agent retries and explores variations of the same task |
| Cost of a miss | Low (a DB query, ms) | High (an LLM call, seconds + $) |
| Volume per unit of work | 1 request → 1 user action | 1 agent task → 10–50 internal calls |
| Expected output format | JSON/HTML for UI rendering | Compact text ready for the context window |
| Concurrent coordination | Occasional locks | Multiple agents/sub-agents operating in parallel on the same state |

## Locked decisions (2026-09-26)

| Decision | Choice | Rationale |
|---|---|---|
| Deployment model | Independent network service, like Redis | Drop-in replacement story; clients already speak RESP over TCP |
| Concurrency | **Multi-threaded from day one.** Sharded store (N shards by key hash, per-shard locks — each shard behaves like a mini single-threaded Redis); Tokio multi-thread runtime; cross-shard multi-key ops documented as non-atomic in v0.1 (Redis Cluster precedent); HNSW index per namespace with RW lock | Rust's ownership model makes data races compile-time errors — the reason Redis (C) avoided threads doesn't apply. Agents mean many concurrent clients; a slow vector search must never block simple GETs |
| Embeddings | MVP: client sends **pre-computed vectors**; protocol designed so server-side embedding fits in phase 2 (`SEMSET` accepts text or vector); embedded ONNX (phase 2) | Embedded ONNX is the biggest complexity multiplier in the plan; fastest path to a v0.1 the community can try. Every serious agent stack already computes embeddings |
| Persistence | Redis-style (snapshot + append-only log equivalent) from MVP | Positioned as a Redis replacement — persistence is expected |
| Positioning | "Redis for agents" — broad, deterministic parity first | Build order: parity layer, then semantic differentiator |
| Audience | Production startups; **open-source community project** (MIT) — dozens of contributors, not a SaaS initially | Docs/benchmark quality bar is high from day one |
| Semantic miss | Plain `no hit` — Rogis never orchestrates LLM calls | Simpler, less coupling; the client decides what a miss means |
| Benchmarks | Real and reproducible: **runnable script in the repo first**, published post second | Credibility of open source must rest on verifiable evidence, not claims |
| License | MIT | Maximizes adoption by startups and contributors |
| Deterministic MVP commands | Strings + TTL, counters (`INCR`/`DECR`), hashes, lists, basic locks (`SETNX` + TTL), pub/sub | Chosen by real usage in agent apps (LangChain/LangGraph/custom loops) |
| Deferred to phase 2 | Streams (`XADD`/`XREAD`), embedded embeddings, threshold auto-tuning (`SEMTUNE`), clustering/HA, SDKs beyond RESP compat | Hard narrow v0.1 line — no aspiration creep |

## Semantic commands (tentative names — finalize during API design)

| Command | Function |
|---|---|
| `SEMSET` | Store a (text or vector) → value pair, indexed by embedding. Params: logical key, text or vector, value, TTL, namespace/collection |
| `SEMGET` | Semantic similarity search over stored entries. Params: query text or vector, similarity threshold, top-k |
| `SEMSTATS` | Semantic cache metrics: hit rate, index size, score distribution. Optional namespace |
| `SEMTUNE` | Assisted threshold tuning from recent traffic (phase 2) |

Design notes: no fixed default threshold in docs — expose empirical measurement tooling instead (optimal threshold isn't portable across embedding models). Namespaces give separate semantic indexes with separate thresholds. Consider a "prompt-ready" compact plaintext output option for `SEMGET` (cache for agents, not humans).

## Refine in parallel (not blocking Phase 0)

- Threshold measurement tooling and per-namespace thresholds
- `SEMGET` prompt-ready output format
- External embedding providers (OpenAI, Voyage, Cohere) vs local-first in phase 2
- `SEMSET` content-based invalidation vs plain TTL in v1
- TTL precision: milliseconds like Redis, or seconds enough for agents
- Agent-traffic dataset: synthetic (LLM-generated retries) vs captured from a real agent
- Re-run competitors' public benchmarks ourselves (BetterDB/RedisVL); dedicated search for Rust engine-level semantic-cache projects on crates.io/GitHub
- Order the 5 risks by severity: which one kills the project if false vs only forces a pitch adjustment

## Definition of done (Roger's acceptance)

When Phase 1 is finished, Rogis must be **ready for trial integration into a real app**:
- `cargo run --release` (documented one-command start) brings up the server on `6379` speaking RESP — any real Redis client (`redis-cli`, `redis-py`, `ioredis`, `go-redis`) connects with zero client changes.
- `docs/INTEGRATION.md`: copy-paste snippets for (a) drop-in deterministic usage and (b) the semantic-cache loop (`SEMSET` / `SEMGET` with client-computed vectors, hit → return, miss → LLM → store).
- The parity benchmark ships as a runnable script in `benches/` (anyone can re-run it).

## Roadmap

| Phase | Content | Exit criteria |
|---|---|---|
| **0 — Technical spikes** | Time-boxed, disposable prototypes: evaluate crates (RESP, HNSW, concurrent maps, tokio); prototype: native vector storage latency vs serialized-in-Redis; ONNX latency probe | Section-6-equivalent decisions closed **with evidence, not intuition** |
| 1 — Deterministic layer | Implement MVP command set + parity benchmark vs real Redis/Valkey | Not significantly below Redis on SET/GET/HSET/INCR throughput/latency (same machine, same protocol) |
| 2 — Semantic layer | `SEMSET`/`SEMGET` + HNSW + quality & latency benchmarks | Reproducible benchmark showing the win vs Redis + external semantic-cache library |
| 3 — Publish | Open-source repo, docs, published benchmark, pilot users | ≥1 external team trying it on a real project, not just passive interest |

## Risks (validate early)

1. **The "native engine" latency win vs "library over Redis" is real and measurable** — the central technical thesis. Validate with a ~1-day prototype before building the rest; if the gain is marginal, the pitch weakens.
2. **Real demand for "one cache for everything"** vs teams being fine with Redis + a semantic library — talk to 3–5 real agent builders before locking the final pitch.
3. **Rust crate maturity** for RESP, HNSW, ONNX — 2–3 day spike evaluating candidates per category before committing.
4. **Solo-dev scope** — hard narrow v0.1; better a small MVP that works than an ambitious one that never ships.
5. **Name/pitch collision** — dedicated name + pitch search before publishing the open-source repo.

## Methodology

- All Rust work: strict Clippy, no unjustified `unsafe`, tests for new behavior (via the `gentle-ai` workflow).
- Spikes are **disposable by design** — findings land in `docs/DECISIONS.md`; spike code is not product code.
- Benchmarks: every performance claim ships with a runnable harness in `benches/` from day one.

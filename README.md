# Rogis

**The cache-database designed for AI-agent traffic, not human traffic.**

Redis was designed in 2009 for a specific pattern: web apps with humans behind them — punctual, structured, low-semantic-redundancy requests. That no longer describes most new traffic being built: AI agents that iterate, retry, explore multiple paths for the same task, and generate natural-language queries instead of structured keys.

Rogis starts from a simple thesis: there is room for a cache-database built from scratch for that access pattern — high redundancy, fuzzy instead of exact queries, and a much higher cost of a miss than in a traditional stack (a miss can trigger an LLM call of several seconds and cents of a dollar, not just a millisecond SQL query).

## Two non-negotiable layers

- **Deterministic layer (Redis parity):** a drop-in Redis replacement for what AI agents also need — session state, rate limiting, queues, locks, pub/sub. Speaks RESP; works with existing clients (`redis-py`, `ioredis`, `go-redis`). If this fails, adopters need Redis + Rogis, and the project loses its reason to exist.
- **Semantic layer (the real differentiator):** native vector storage inside the engine — no generic byte serialization — with similarity search as a first-class primitive, not a wrapper library on top.

> Rogis is a cache, deliberately. Not a general-purpose vector DB for large-scale RAG, not a multi-model database, not an agent orchestration framework.

## Status

**Phase 0 — technical spikes** (in progress). See [PLAN.md](PLAN.md) for the full build plan and [docs/](docs/) for the strategic planning document.

## License

MIT — see [LICENSE](LICENSE).

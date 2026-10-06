# AGENTS.md — rogis

Working agreement for any AI agent operating in this repo (Muse, GPT, Codex, Cursor, etc.).
Owner: Roger (SRogDev). Discussion language: Spanish. Implementation plans for coding agents: English.

## How Roger works — read this first
- Incremental MVPs over big up-front design. Architecture emerges from concrete needs; no speculative complexity.
- "Learning by building": implement the minimum that works, learn when real problems arise, ship artifacts.
- Always inspect the repo before making architectural or file-level decisions.
- Reuse existing systems instead of creating parallel abstractions.
- Many functional, basic MVPs first; polish later.
- Explanations: only what's needed to build and solve the immediate problem. No theory dumps.
- Be an auditor, not a cheerleader: evidence-only assessments, uncomfortable findings stated plainly, always paired with concrete next options.
- Corrections are final: adopt Roger's version without debate and keep moving.
- No unprompted initiatives outside the approved scope — offer options instead.

## Engineering discipline
- ALL coding work: ODD routing (route by size) → TDD (test evidence) → RDD (diff self-review before delivery). No exceptions. (Muse: use the `gentle-ai` workspace skill.)
- ALL frontend/UI work: design-system-first — resolve style, palette, fonts, UX guidelines BEFORE implementing. No exceptions. (Muse: use the `ui-ux-pro-max` workspace skill, run its design-system search first.)
- Python: `uv` ONLY — `uv venv`, deps in `pyproject.toml [project]`, `uv sync`, `uv.lock` committed. Never `python -m venv` + pip, never bare `requirements.txt`.
- JS/TS: check the latest Next.js version before installing (16.x is current as of 2026-09); use the official codemod for upgrades. Prefer Biome over ESLint/Prettier.
- Conventional commits: `feat:` / `fix:` / `chore:` / `docs:` / `refactor:` / `test:`.

## PR workflow & history (mandatory)
- Every change ships as a PR against `main`. Muse is authorized to merge his own PRs.
- One PR = one logical change. Squash-merge so `main` stays linear and readable — one clean commit per change.
- Never push directly to `main`.
- PR description must include verifiable evidence: test counts, build results, commit SHAs. Never invent numbers — write "not measured" when unknown.
- When a phase/milestone completes, update STATUS.md in the same PR.

## Context docs (keep them current)
- `README.md` — human/contributor-facing overview.
- `docs/PROJECT_BRIEF.md` — full project context in one file, written to be handed to ANOTHER AI for planning/ideation. Keep it accurate; it is the handoff doc.
- `STATUS.md` — timeline: done / doing / next. Read it before starting work; update it when reality changes.

## Stack
- Rust. Roger's own Redis: deterministic Redis superset (RESP, drop-in) + native semantic engine (vectors first-class, HNSW, SEMSET/SEMGET).
- tokio multi-thread, hnsw_rs 0.3.4 + simdeez_f SIMD, 16-shard Mutex<HashMap>, hand-rolled RESP + redis-protocol oracle.
- License: MIT. Audience: production startups + open-source community (not his SaaS initially).
- Positioning: Redis for agents (parity first). Semantic miss = plain no-hit. Real reproducible benchmarks (script in repo).
- CRITICAL invariants (see `docs/DECISIONS.md`): (1) on RESP3, nils must be `_\r\n` — never `$-1`/`*-1` (redis-py 8.x blocks forever on them); `HGETALL` must return a real RESP3 map (`%`) on proto 3; (2) restart loads `dump.rogb` AND replays `appendonly.rogb` — every snapshot must atomically truncate the AOF or pre-snapshot writes replay twice.
- Command semantics: `SET k v NX XX` (both flags) returns nil whether or not the key exists — that IS real Redis behavior. Bare UNSUBSCRIBE in subscriber mode returns to normal command mode.

## Repo map
- `src/` — server, RESP codec (`resp::encode_with_proto`), store, AOF/snapshot, semantic engine
- `tests/` — 123 tests (must stay green); verified against real redis-py
- `benches/` — reproducible benchmarks (~68k ops/s measured)
- `docs/` — `DECISIONS.md` (read before touching RESP/persistence), `INTEGRATION.md`
- Definition of done: one-command start on 6379 with real redis clients + `docs/INTEGRATION.md`
- Commands: `cargo build --release` / `cargo test` / `cargo clippy`

## What NOT to do
- No speculative abstractions, "just in case" features, or parallel systems.
- Don't reformat whole files for style; keep diffs reviewable.
- Never commit secrets, `.env` files, or credentials.
- Don't invent metrics, benchmarks, or test results.

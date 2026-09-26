# Rogis integration guide (v0.1)

Rogis speaks RESP2 and RESP3 (negotiated per connection via `HELLO`) and
implements the Redis commands listed in
[Command support](#command-support-v01) below. If your app already talks to
Redis, point it at Rogis — no client changes needed.

On RESP3 connections nil replies use the RESP3 null type (`_\r\n`, never
`$-1`/`*-1`) and `HGETALL` returns a real RESP3 map (`%`), matching real
Redis — this is what RESP3-default clients like redis-py 8.x expect.

> **Verification note (2026-09-26):** every wire command shown in this guide
> was executed against a locally built rogis v0.1.0
> (`./target/release/rogis --port 6470 --dir /tmp/rogis-integ-data`) over a
> raw TCP socket speaking RESP, and every reply was asserted by an automated
> script (37 checks, all passing). `redis-py` is **not** installed in this
> environment, so Python snippets are labeled *"commands verified over raw
> RESP; client-library call shapes per official docs"*. The `ioredis` /
> `go-redis` one-liners are **untested examples** — same protocol, not
> executed here.

---

## 1. Drop-in replacement

### redis-py — zero code changes

*Commands verified over raw RESP; client-library call shapes per official docs.*

```python
import redis

r = redis.Redis(host="127.0.0.1", port=6379, decode_responses=True)

r.set("hello", "rogis")   # True            (wire: SET hello rogis        -> +OK)
r.get("hello")            # "rogis"         (wire: GET hello              -> $5 rogis)
r.incr("visits")          # 1               (wire: INCR visits             -> :1)
r.hset("user:7", mapping={"name": "Ada", "role": "admin"})  # 2
r.hgetall("user:7")       # {"name": "Ada", "role": "admin"}
```

Swap `port=6379` for wherever Rogis listens. Everything else stays identical.

### redis-cli session (transcript of verified commands)

```text
$ redis-cli -p 6379
127.0.0.1:6379> PING
PONG
127.0.0.1:6379> SET hello rogis
OK
127.0.0.1:6379> GET hello
"rogis"
127.0.0.1:6379> INCR visits
(integer) 1
127.0.0.1:6379> INCR visits
(integer) 2
127.0.0.1:6379> HSET user:7 name Ada role admin
(integer) 2
127.0.0.1:6379> HGETALL user:7
1) "name"
2) "Ada"
3) "role"
4) "admin"
127.0.0.1:6379> LPUSH jobs a b
(integer) 2
127.0.0.1:6379> RPOP jobs
"a"
127.0.0.1:6379> EXPIRE hello 60
(integer) 1
127.0.0.1:6379> TTL hello
(integer) 60
```

> Hash field order is not guaranteed (same as real Redis); the transcript
> above shows one possible order.

### ioredis (Node.js) — untested example

```ts
import Redis from "ioredis";
const r = new Redis(6379, "127.0.0.1"); // just host/port
await r.set("hello", "rogis");
await r.get("hello"); // "rogis"
```

### go-redis (Go) — untested example

```go
rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"}) // just Addr
rdb.Set(ctx, "hello", "rogis", 0)
rdb.Get(ctx, "hello") // "rogis"
```

---

## 2. Copy-paste patterns

Each pattern is a complete, runnable snippet. The wire exchange under every
snippet is exactly what was executed and verified against Rogis.

### Distributed lock

*Commands verified over raw RESP; client-library call shapes per official docs.*

```python
import secrets, time
import redis

r = redis.Redis(host="127.0.0.1", port=6379, decode_responses=True)

def acquire(lock: str, ttl_ms: int = 30_000) -> str | None:
    token = secrets.token_hex(16)
    # SET key token NX PX 30000 -> "OK" on acquire, None when contended
    if r.set(lock, token, nx=True, px=ttl_ms):
        return token
    return None

def release(lock: str, token: str) -> None:
    # v0.1 has no scripting: this check-and-delete is NOT atomic.
    # Safe enough for cooperative workers; see note below.
    if r.get(lock) == token:
        r.delete(lock)

token = acquire("lock:job-7")
if token:
    try:
        run_the_job()          # your critical section
    finally:
        release("lock:job-7", token)
```

Verified wire exchange:

```text
SET lock:job-7 tok-abc NX PX 30000   -> +OK     (acquired)
SET lock:job-7 tok-other NX PX 30000 -> $-1     (nil: someone else holds it)
DEL lock:job-7                       -> :1      (released)
```

> **Note:** real Redis users release with a Lua script so check-and-delete is
> atomic. Rogis v0.1 has no `EVAL`; for strict mutual exclusion, hold the
> lock only from a single releaser process, or re-check `GET` right before
> acting.

### Cache-aside

*Commands verified over raw RESP; client-library call shapes per official docs.*

```python
import json
import redis

r = redis.Redis(host="127.0.0.1", port=6379, decode_responses=True)

def get_user(user_id: int) -> dict:
    key = f"cache:user:{user_id}"
    hit = r.get(key)
    if hit is not None:
        return json.loads(hit)          # cache hit
    user = db.load_user(user_id)        # cache miss -> source of truth
    r.set(key, json.dumps(user), ex=300)  # fill with 5-min TTL
    return user
```

Verified wire exchange:

```text
GET cache:user:42                              -> $-1          (miss)
SET cache:user:42 {"id":42,"name":"Ada"} EX 300 -> +OK          (fill)
GET cache:user:42                              -> {"id":42,"name":"Ada"}  (hit)
```

### Rate limiting (fixed window)

*Commands verified over raw RESP; client-library call shapes per official docs.*

```python
import redis

r = redis.Redis(host="127.0.0.1", port=6379, decode_responses=True)

LIMIT, WINDOW_S = 100, 60

def allowed(client_id: str) -> bool:
    key = f"rl:api:{client_id}"
    count = r.incr(key)          # 1 on first hit in the window
    if count == 1:
        r.expire(key, WINDOW_S)  # arm the window only once
    return count <= LIMIT
```

Verified wire exchange:

```text
INCR rl:api:1          -> :1
EXPIRE rl:api:1 60     -> :1
INCR rl:api:1          -> :2
TTL rl:api:1           -> :60
```

### Session state (hashes)

*Commands verified over raw RESP; client-library call shapes per official docs.*

```python
import redis

r = redis.Redis(host="127.0.0.1", port=6379, decode_responses=True)

SESSION_TTL_S = 1800

def touch_session(session_id: str, user_id: int) -> None:
    key = f"sess:{session_id}"
    # No HINCRBY in v0.1: read-modify-write for counters inside hashes.
    n = int(r.hget(key, "login_count") or 0) + 1
    r.hset(key, mapping={"user_id": user_id, "login_count": n})
    r.expire(key, SESSION_TTL_S)   # sliding expiration

def get_session(session_id: str) -> dict:
    return r.hgetall(f"sess:{session_id}")
```

Verified wire exchange:

```text
HSET sess:xyz user_id 42 login_count 1 -> :2   (2 new fields)
HGET sess:xyz login_count               -> "1"
HSET sess:xyz login_count 2             -> :0   (field existed)
EXPIRE sess:xyz 1800                    -> :1
HGETALL sess:xyz                        -> {user_id: 42, login_count: 2}
```

> `HINCRBY` is not in v0.1; counters inside hashes use read-modify-write as
> shown. Top-level counters should use `INCR` (atomic) instead.

### Task queue (FIFO via LPUSH / RPOP)

*Commands verified over raw RESP; client-library call shapes per official docs.*

```python
import redis

r = redis.Redis(host="127.0.0.1", port=6379, decode_responses=True)

def enqueue(queue: str, *jobs: str) -> int:
    return r.lpush(queue, *jobs)   # producers push left

def dequeue(queue: str, timeout: int = 5) -> str | None:
    # v0.1 has no blocking pop; poll instead.
    import time
    deadline = time.time() + timeout
    while time.time() < deadline:
        job = r.rpop(queue)        # consumers pop right -> FIFO
        if job is not None:
            return job
        time.sleep(0.05)
    return None

enqueue("queue:email", "welcome", "receipt", "reminder")
print(dequeue("queue:email"))  # "welcome"
print(dequeue("queue:email"))  # "receipt"
```

Verified wire exchange:

```text
LPUSH queue:email welcome receipt reminder -> :3
RPOP queue:email                           -> "welcome"
RPOP queue:email                           -> "receipt"
LRANGE queue:email 0 -1                     -> ["reminder"]
```

> `BLPOP`/`BRPOP` are not in v0.1 — poll with `RPOP` as shown, or
> `SUBSCRIBE` to a "new job" channel and `RPOP` on notification.

### Pub/sub notifications

*Commands verified over raw RESP; client-library call shapes per official docs.*

Subscriber (its own connection — a subscribed connection can't run other
commands):

```python
import redis

r = redis.Redis(host="127.0.0.1", port=6379, decode_responses=True)
sub = r.pubsub()
sub.subscribe("alerts")
for message in sub.listen():
    if message["type"] == "message":
        print("alert:", message["data"])  # "deploy done"
        break
sub.unsubscribe("alerts")
```

Publisher (any other connection):

```python
r.publish("alerts", "deploy done")  # -> 1 subscriber received it
```

Verified wire exchange:

```text
# subscriber connection
SUBSCRIBE alerts            -> [subscribe, alerts, 1]
# publisher connection
PUBLISH alerts "deploy done" -> :1
# subscriber connection receives
                            -> [message, alerts, "deploy done"]
UNSUBSCRIBE alerts           -> [unsubscribe, alerts, 0]
```

---

## Command support (v0.1)

| Area | Commands |
|---|---|
| Connection | `PING`, `QUIT`, `HELLO` |
| Strings | `SET` (`EX`/`PX`/`NX`/`XX`), `GET`, `SETNX`, `INCR`, `DECR`, `INCRBY` |
| Keys | `DEL`, `EXISTS`, `EXPIRE`, `PEXPIRE`, `TTL` |
| Hashes | `HSET`, `HGET`, `HGETALL`, `HDEL` |
| Lists | `LPUSH`, `RPOP`, `LRANGE` |
| Pub/sub | `PUBLISH`, `SUBSCRIBE`, `UNSUBSCRIBE` |
| Persistence | `SAVE` |

Behavioral notes: command names are case-insensitive; `SET` with both `NX`
and `XX` returns nil exactly like Redis; hash field order is arbitrary;
`SAVE` writes `dump.rogb` to `--dir`.

---

## Semantic cache loop (Phase 2 — not implemented in v0.1)

This section describes the **intended** future flow. It does not work yet.

The plan: Rogis stores vectors natively (no byte-serialization tax) and
exposes similarity search as a first-class primitive, so an agent can ask
"have I already computed something *like* this?" instead of only exact-key
lookups:

```text
# Phase 2 sketch (DOES NOT WORK in v0.1)
SEMSET agent:cache <768 floats...> "the computed answer"   # store vector + payload
SEMGET agent:cache <768 floats...>                          # nearest neighbor
# -> hit:  [score 0.97, "the computed answer"]
# -> miss: (nil)  -> fall back to the expensive LLM call, then SEMSET the result
```

Key design points (from `docs/DECISIONS.md`): vectors are computed
**client-side** in the MVP (embedded ONNX comes in a later phase), and a
semantic miss is a plain "no hit" — no LLM orchestration inside the server.

**Honest status:** `SEMSET` / `SEMGET` currently return errors:

```text
SEMSET k 0.1 0.2 -> -ERR unknown command 'SEMSET'
SEMGET k 0.1 0.2 -> -ERR unknown command 'SEMGET'
```

(verified against the v0.1 binary). Until Phase 2 lands, approximate the
pattern with exact keys (`SET`/`GET`) or do the similarity search in your
own process and use Rogis as the payload store.

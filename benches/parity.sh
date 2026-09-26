#!/usr/bin/env bash
# parity.sh — Rogis vs real-Redis throughput parity check.
#
# Builds release binaries, starts rogis on 127.0.0.1:6390, runs
# rogis-bench for every workload, prints a results table. If
# `redis-server` is installed, starts it on 6391 and runs the IDENTICAL
# bench commands, then prints a side-by-side comparison table.
#
# Exits 0 in both cases. Always kills started servers and removes temp dirs.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
export PATH="$HOME/.cargo/bin:$PATH"

BENCH_OPS="${BENCH_OPS:-20000}"
BENCH_CLIENTS="${BENCH_CLIENTS:-16}"
WORKLOADS="set_get incr hset mixed"
ROGis_PORT=6390
REDIS_PORT=6391

TMP="$(mktemp -d)"
ROGis_DATA="$TMP/rogis-data"
REDIS_DATA="$TMP/redis-data"
mkdir -p "$ROGis_DATA" "$REDIS_DATA"
ROGis_PID=""
REDIS_PID=""

cleanup() {
    if [ -n "$ROGis_PID" ]; then kill "$ROGis_PID" 2>/dev/null || true; fi
    if [ -n "$REDIS_PID" ]; then kill "$REDIS_PID" 2>/dev/null || true; fi
    rm -rf "$TMP"
}
trap cleanup EXIT

echo "== building release =="
cargo build --release --bins 2>&1 | tail -1

# Wait until PING -> PONG on host:port (timeout $3 seconds).
wait_ping() {
    python3 - "$1" "$2" "$3" <<'EOF'
import socket, sys, time
host, port, timeout = sys.argv[1], int(sys.argv[2]), float(sys.argv[3])
deadline = time.time() + timeout
req = b"*1\r\n$4\r\nPING\r\n"
while time.time() < deadline:
    try:
        s = socket.create_connection((host, port), timeout=1)
        s.sendall(req)
        data = s.recv(64)
        s.close()
        if data.startswith(b"+PONG"):
            sys.exit(0)
    except OSError:
        pass
    time.sleep(0.1)
print(f"no PONG from {host}:{port} within {timeout}s", file=sys.stderr)
sys.exit(1)
EOF
}

# Run every workload against port $2, store RESULT lines in $TMP/$1.<workload>.
run_all() {
    local name="$1" port="$2"
    echo "== benching $name on 127.0.0.1:$port (ops=$BENCH_OPS clients=$BENCH_CLIENTS) =="
    for w in $WORKLOADS; do
        if out=$(./target/release/rogis-bench --host 127.0.0.1 --port "$port" \
                --clients "$BENCH_CLIENTS" --ops "$BENCH_OPS" --workload "$w"); then
            echo "$out" | grep '^RESULT ' > "$TMP/$name.$w"
            echo "$out" | grep -v '^RESULT '
        else
            echo "WARN: bench workload=$w on $name failed; skipping"
        fi
    done
}

# Print the results table for server $1 from $TMP/$1.<workload> files.
print_table() {
    local name="$1"
    printf '\n%-10s %12s %12s %12s %8s\n' workload ops/s p50 p99 errors
    for w in $WORKLOADS; do
        local f="$TMP/$name.$w"
        [ -f "$f" ] || continue
        awk -v w="$w" '
        {
            for (i = 1; i <= NF; i++) {
                split($i, kv, "="); d[kv[1]] = kv[2]
            }
            printf "%-10s %12.0f %12s %12s %8s\n", w, d["ops_per_sec"], \
                fmt(d["p50_ns"]), fmt(d["p99_ns"]), d["errors"] + d["proto_errors"]
        }
        function fmt(ns,   v) {
            if (ns < 1000) return ns "ns"
            if (ns < 1000000) { v = ns / 1000; return sprintf("%.1fµs", v) }
            if (ns < 1000000000) { v = ns / 1000000; return sprintf("%.2fms", v) }
            v = ns / 1000000000; return sprintf("%.2fs", v)
        }' "$f"
    done
}

echo "== starting rogis on $ROGis_PORT =="
./target/release/rogis --port "$ROGis_PORT" --dir "$ROGis_DATA" --save 0 \
    >"$TMP/rogis.log" 2>&1 &
ROGis_PID=$!
if ! wait_ping 127.0.0.1 "$ROGis_PORT" 15; then
    echo "rogis.log:"; cat "$TMP/rogis.log"
    exit 1
fi

run_all rogis "$ROGis_PORT"
echo "== rogis results =="
print_table rogis

if command -v redis-server >/dev/null 2>&1; then
    echo "== starting redis-server on $REDIS_PORT =="
    redis-server --port "$REDIS_PORT" --save '' --appendonly no --dir "$REDIS_DATA" \
        >"$TMP/redis.log" 2>&1 &
    REDIS_PID=$!
    if ! wait_ping 127.0.0.1 "$REDIS_PORT" 15; then
        echo "redis.log:"; cat "$TMP/redis.log"
        exit 1
    fi
    run_all redis "$REDIS_PORT"
    echo "== redis results =="
    print_table redis
    printf '\n%-10s %14s %14s %10s\n' workload "rogis ops/s" "redis ops/s" "rogis/redis"
    for w in $WORKLOADS; do
        [ -f "$TMP/rogis.$w" ] && [ -f "$TMP/redis.$w" ] || continue
        ro=$(awk '{for(i=1;i<=NF;i++){split($i,kv,"="); if(kv[1]=="ops_per_sec") print kv[2]}}' "$TMP/rogis.$w")
        re=$(awk '{for(i=1;i<=NF;i++){split($i,kv,"="); if(kv[1]=="ops_per_sec") print kv[2]}}' "$TMP/redis.$w")
        ratio=$(awk -v a="$ro" -v b="$re" 'BEGIN{printf "%.2fx", (b>0?a/b:0)}')
        printf '%-10s %14s %14s %10s\n' "$w" "$ro" "$re" "$ratio"
    done
else
    echo
    echo "NOTE: redis-server not available in this environment — parity vs real Redis not measured; results are self-consistent rogis numbers only."
fi

echo
echo "done."

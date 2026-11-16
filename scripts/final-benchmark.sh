#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PORT="${OMAP_BENCH_PORT:-19092}"
SERVER_XMX="${OMAP_SERVER_XMX:-8g}"
CLIENT_XMX="${OMAP_CLIENT_XMX:-2g}"
WRITER_RPS="${1:-${OMAP_WRITER_RPS:-0}}"
H2_VERSION="2.5.252"

echo "[1/4] Building h2omap..."
mvn -B -q package

H2_JAR="$HOME/.m2/repository/com/h2database/h2/${H2_VERSION}/h2-${H2_VERSION}.jar"
if [[ ! -f "$H2_JAR" ]]; then
    M2_REPO="$(mvn -B -q help:evaluate -Dexpression=settings.localRepository -DforceStdout 2>/dev/null || true)"
    if [[ -n "$M2_REPO" && -f "$M2_REPO/com/h2database/h2/${H2_VERSION}/h2-${H2_VERSION}.jar" ]]; then
        H2_JAR="$M2_REPO/com/h2database/h2/${H2_VERSION}/h2-${H2_VERSION}.jar"
    else
        echo "ERROR: h2-${H2_VERSION}.jar not found in local Maven repository." >&2
        exit 1
    fi
fi

SRV_LOG="$(mktemp -t h2omap_bench_srv.XXXXXX)"
SRV_PID=""
cleanup() {
    if [[ -n "$SRV_PID" ]] && kill -0 "$SRV_PID" 2>/dev/null; then
        kill "$SRV_PID" 2>/dev/null || true
        wait "$SRV_PID" 2>/dev/null || true
    fi
    rm -f "$SRV_LOG"
}
trap cleanup EXIT INT TERM

echo "[2/4] Starting standalone OMAP TCP server (-Xmx${SERVER_XMX}, port=${PORT})..."
java -Xmx"${SERVER_XMX}" -cp target/h2omap-0.1.0-SNAPSHOT.jar \
    org.h2.tools.Server -tcp -tcpPort "$PORT" -ifNotExists >"$SRV_LOG" 2>&1 &
SRV_PID=$!

echo "[3/4] Waiting for TCP port ${PORT}..."
READY=0
for _ in $(seq 1 60); do
    if ! kill -0 "$SRV_PID" 2>/dev/null; then
        echo "ERROR: OMAP TCP server exited unexpectedly:" >&2
        cat "$SRV_LOG" >&2
        exit 1
    fi
    if command -v nc >/dev/null 2>&1; then
        if nc -z 127.0.0.1 "$PORT" >/dev/null 2>&1; then
            READY=1
            break
        fi
    elif (echo >/dev/tcp/127.0.0.1/"$PORT") >/dev/null 2>&1; then
        READY=1
        break
    fi
    sleep 0.2
done

if [[ "$READY" -ne 1 ]]; then
    echo "ERROR: Timed out waiting for TCP port ${PORT}." >&2
    cat "$SRV_LOG" >&2
    exit 1
fi

echo "[4/4] Running final benchmark (10,000,000 rows, OMAP only, 16 readers + 1 writer, 50 rounds, writer_rps=${WRITER_RPS})..."
java -Xmx"${CLIENT_XMX}" \
    -cp "${H2_JAR}:target/test-classes" \
    com.hyd.h2omap.bench.ConcurrentQueryBench \
    16 10000000 50 true omap \
    "jdbc:h2:tcp://127.0.0.1:${PORT}/mem:omap10m;DB_CLOSE_DELAY=-1;QUERY_CACHE_SIZE=0" \
    "${WRITER_RPS}"

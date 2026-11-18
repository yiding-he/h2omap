#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PORT="${OMAP_BENCH_PORT:-19092}"
SERVER_XMX="${OMAP_SERVER_XMX:-8g}"
CLIENT_XMX="${OMAP_CLIENT_XMX:-2g}"
DATA_DIR="${OMAP_BENCH_DATA_DIR:-$ROOT_DIR/target/bench-db}"
H2_VERSION="2.5.252"
ROWS=5000000
READERS=16
ROUNDS=50

WRITER_RPS="${OMAP_WRITER_RPS:-0}"
PERSIST=0
KEEP_DATA=0
for arg in "$@"; do
    case "$arg" in
        --persist) PERSIST=1 ;;
        --keep-data) KEEP_DATA=1 ;;
        ''|*[!0-9]*) echo "ERROR: unknown argument '$arg' (expected a writer rate or --persist/--keep-data)." >&2; exit 2 ;;
        *) WRITER_RPS="$arg" ;;
    esac
done

if [[ "$PERSIST" -eq 1 ]]; then
    DB_PATH="$DATA_DIR/h2omapdb"
    URL="jdbc:h2:tcp://127.0.0.1:${PORT}/${DB_PATH};QUERY_CACHE_SIZE=0"
    STORAGE="file (${DB_PATH})"
    TOTAL_STEPS=5
else
    URL="jdbc:h2:tcp://127.0.0.1:${PORT}/mem:omap${ROWS};DB_CLOSE_DELAY=-1;QUERY_CACHE_SIZE=0"
    STORAGE="in-memory"
    TOTAL_STEPS=4
fi

if [[ "$PERSIST" -eq 1 ]]; then
    rm -rf "$DATA_DIR"
    mkdir -p "$DATA_DIR"
fi

echo "[1/${TOTAL_STEPS}] Building h2omap..."
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
    if [[ "$PERSIST" -eq 1 && "$KEEP_DATA" -ne 1 ]]; then
        rm -rf "$DATA_DIR"
    fi
}
trap cleanup EXIT INT TERM

stop_server() {
    if [[ -n "$SRV_PID" ]] && kill -0 "$SRV_PID" 2>/dev/null; then
        kill "$SRV_PID" 2>/dev/null || true
        wait "$SRV_PID" 2>/dev/null || true
    fi
    SRV_PID=""
}

echo "[2/${TOTAL_STEPS}] Starting standalone OMAP TCP server (-Xmx${SERVER_XMX}, port=${PORT}, storage=${STORAGE})..."
java -Xmx"${SERVER_XMX}" -cp target/h2omap-0.1.0-SNAPSHOT.jar \
    org.h2.tools.Server -tcp -tcpPort "$PORT" -ifNotExists >"$SRV_LOG" 2>&1 &
SRV_PID=$!

echo "[3/${TOTAL_STEPS}] Waiting for TCP port ${PORT}..."
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

echo "[4/${TOTAL_STEPS}] Running benchmark (rows=${ROWS}, engine=omap, storage=${STORAGE}, readers=${READERS}, rounds=${ROUNDS}, writer=true, writer_rps=${WRITER_RPS})..."
java -Xmx"${CLIENT_XMX}" \
    -cp "${H2_JAR}:target/test-classes" \
    com.hyd.h2omap.bench.ConcurrentQueryBench \
    "${READERS}" "${ROWS}" "${ROUNDS}" true omap "${URL}" "${WRITER_RPS}"

if [[ "$PERSIST" -eq 1 ]]; then
    echo "[5/${TOTAL_STEPS}] Verifying persisted rows after restart..."
    java -cp "$H2_JAR" org.h2.tools.Shell -url "$URL" -user sa -sql 'SHUTDOWN' >/dev/null 2>&1 || true
    sleep 2
    stop_server

    # 写线程每轮先 INSERT 后 DELETE，停止点可能落在两条语句之间，所以净行数是 ROWS 或 ROWS + 1。
    COUNT="$(java -cp target/h2omap-0.1.0-SNAPSHOT.jar org.h2.tools.Shell \
        -url "jdbc:h2:${DB_PATH}" -user sa -sql 'SELECT COUNT(*) FROM T' 2>&1 \
        | awk '/^[0-9]+$/{print; exit}')"
    SIZE_MB="$(du -sm "$DATA_DIR" | cut -f1)"
    if [[ -z "$COUNT" ]]; then
        echo "ERROR: cannot read persisted row count (data dir kept at ${DB_PATH})." >&2
        exit 1
    fi
    if [[ "$COUNT" -ne "$ROWS" && "$COUNT" -ne $((ROWS + 1)) ]]; then
        echo "ERROR: recovered row count ${COUNT} != expected ${ROWS} (data dir kept at ${DB_PATH}, ${SIZE_MB} MB)." >&2
        exit 1
    fi
    echo "Recovered rows=${COUNT} (expected ${ROWS} 或 ${ROWS}+1), data dir ${SIZE_MB} MB."
fi

#!/usr/bin/env bash
set -euo pipefail

# 重建 residentBytes 驻留预算测试环境：
#   文件库 <数据目录>/benchdb，900 万行 10 列表 T（磁盘约 1 GB），
#   生成后 ALTER TABLE SET RESIDENT_BYTES '100m'，驻留预算 100 MiB 并持久化。
#
# 可用环境变量覆盖：OMAP_RESIDENT_ROWS / OMAP_RESIDENT_BUDGET / OMAP_RESIDENT_DATA_DIR / OMAP_RESIDENT_XMX
# 用法：./scripts/resident-env.sh

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

ROWS="${OMAP_RESIDENT_ROWS:-9000000}"
BUDGET="${OMAP_RESIDENT_BUDGET:-100m}"
DATA_DIR="${OMAP_RESIDENT_DATA_DIR:-/Volumes/Yotta/Temp/h2omap}"
XMX="${OMAP_RESIDENT_XMX:-8g}"
MAIN_JAR="$ROOT_DIR/target/h2omap-0.1.0-SNAPSHOT.jar"
DB_URL="jdbc:h2:${DATA_DIR}/benchdb"

echo "[1/4] Building h2omap..."
mvn -B -q package

echo "[2/4] Resetting data dir ${DATA_DIR} (rows=${ROWS})..."
rm -rf "${DATA_DIR:?}"/benchdb*
mkdir -p "$DATA_DIR"

echo "[3/4] Generating data (fixed seed, one table T, file-mode DB)..."
java -Xmx"$XMX" -cp "${MAIN_JAR}:target/test-classes" \
    com.hyd.h2omap.bench.DataGenerator "$ROWS" "$DB_URL" omap

echo "[4/4] Setting resident budget ${BUDGET} and verifying..."
java -Xmx"$XMX" -cp "$MAIN_JAR" org.h2.tools.Shell -url "$DB_URL" -user sa \
    -sql "ALTER TABLE T SET RESIDENT_BYTES '${BUDGET}'"

COUNT="$(java -Xmx"$XMX" -cp "$MAIN_JAR" org.h2.tools.Shell -url "$DB_URL" -user sa \
    -sql 'SELECT COUNT(*) FROM T' 2>&1 | awk '/^[0-9]+$/{print; exit}')"
WITH_LINE="$(java -Xmx"$XMX" -cp "$MAIN_JAR" org.h2.tools.Shell -url "$DB_URL" -user sa \
    -sql 'SCRIPT NODATA TABLE T' 2>&1 | grep 'residentBytes=' | head -1 | sed 's/^ *//')"
SIZE="$(du -sh "${DATA_DIR}/benchdb.omap" | cut -f1)"

echo "--- verify rows=${COUNT} expected=${ROWS}"
echo "--- verify catalog: ${WITH_LINE}"
echo "--- data dir size: ${SIZE}"
if [[ "$COUNT" != "$ROWS" ]]; then
    echo "ERROR: row count ${COUNT} != ${ROWS}" >&2
    exit 1
fi
if [[ "$WITH_LINE" != *"residentBytes="* ]]; then
    echo "ERROR: residentBytes not persisted in catalog" >&2
    exit 1
fi

echo "environment ready at ${DATA_DIR}/benchdb budget=${BUDGET}"
echo "server: java -Xmx${XMX} -cp ${MAIN_JAR} org.h2.tools.Server -tcp -tcpPort 19092 -ifNotExists"
echo "client: jdbc:h2:tcp://127.0.0.1:19092${DATA_DIR}/benchdb;QUERY_CACHE_SIZE=0"

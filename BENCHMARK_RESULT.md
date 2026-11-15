# 最终性能测试

## 测试环境与基线

| 项 | 值 |
| --- | --- |
| 代码基线 | `main` (`384b768`) |
| JDK | `21.0.11`（字节码目标 Java 17） |
| 物理内存 | `16.0 GB` |
| 引擎 | 仅 OMAP（`com.hyd.h2omap.engine.OmapTableEngine`） |
| 部署模式 | 独立 TCP 服务端进程 + 外部纯 H2 客户端进程（客户端不含 `h2omap` Jar） |
| 数据规模 | 10,000,000 行，9 列（`ID` 主键、`LOW_CARD`、`HIGH_CARD`、`INT_COL`、`DOUBLE_COL`、`TS_COL`、`NULLABLE_COL`、`ARRAY_COL`、`STR_ARRAY_COL`） |
| 存储参数 | `segmentSize=65536`，`compactionMinRows=1` |
| 查询缓存 | 关闭（`QUERY_CACHE_SIZE=0`） |

## 复现命令

一键执行（自动完成构建、启动 `-Xmx6g` 独立 TCP 服务端、定位 H2 Jar、执行 50 轮压测并自动清理服务端进程）：

```bash
./scripts/final-benchmark.sh          # 默认：写线程不限速
./scripts/final-benchmark.sh 50       # 限速：写线程每秒执行 50 轮（每轮含 INSERT+UPDATE+DELETE）
```

若需手动分步执行：

1. 构建产物：

```bash
mvn -B -q package
```

2. 启动独立 TCP 服务端（`-Xmx6g`）：

```bash
java -Xmx6g -cp target/h2omap-0.1.0-SNAPSHOT.jar \
  org.h2.tools.Server -tcp -tcpPort 19092 -ifNotExists
```

3. 启动外部纯 H2 客户端压测（16 并发读 + 1 并发写，1,000 万行，50 轮共 3,200 次查询）：

```bash
java -Xmx2g \
  -cp "$HOME/.m2/repository/com/h2database/h2/2.5.252/h2-2.5.252.jar:target/test-classes" \
  com.hyd.h2omap.bench.ConcurrentQueryBench \
  16 10000000 50 true omap \
  "jdbc:h2:tcp://localhost:19092/mem:omap10m;DB_CLOSE_DELAY=-1;QUERY_CACHE_SIZE=0"
```

## 并发负载说明

- **读负载**：16 个读线程并发执行 50 轮，每轮顺序执行 `q1`–`q4` 共 4 条 SQL，总计 3,200 次查询。
- **写负载**：1 个写线程在并发读期间持续循环执行单行细粒度变更，每轮依次执行：
  1. `INSERT` 1 行新记录（写入开放段）
  2. `UPDATE` 1 行已有记录
  3. `DELETE` 1 行已有记录
- 因 `compactionMinRows=1`，每轮中的 `UPDATE` 与 `DELETE` 各自触发一次段合并（`compact()`）。

## 查询语句

- q1: `SELECT COUNT(*) FROM T WHERE INT_COL * 2 >= 900000`
- q2: `SELECT COUNT(*) FROM T WHERE LOW_CARD IN ('C0','C1','C2','C3','C4','C5')`
- q3: `SELECT SUM(INT_COL) FROM T WHERE INT_COL >= 400000`
- q4: `SELECT COUNT(*) FROM T WHERE LOW_CARD <> 'C9' AND INT_COL >= 10000 AND DOUBLE_COL < 950 AND TS_COL >= TIMESTAMP '2026-02-01' AND NULLABLE_COL IS NOT NULL AND CARDINALITY(ARRAY_COL) >= 1 AND CARDINALITY(STR_ARRAY_COL) >= 1`

## 实测结果

对比两种写负载：不限速写（`./scripts/final-benchmark.sh`）与限速每秒写 50 轮（`./scripts/final-benchmark.sh 50`，每轮含 1 次 INSERT + 1 次 UPDATE + 1 次 DELETE 并触发 2 次段合并）。

| 查询 | 不限速写 | 限速 50 轮/秒 |
| --- | --- | --- |
| `q1` | `82.56 ms` | `61.44 ms` |
| `q2` | `118.96 ms` | `79.35 ms` |
| `q3` | `82.70 ms` | `60.08 ms` |
| `q4` | `717.52 ms` | `476.98 ms` |

### 汇总指标

| 指标 | 不限速写 | 限速 50 轮/秒 |
| --- | --- | --- |
| 读查询总数 | `3,200`（16 线程 × 50 轮 × 4 查询） | `3,200`（16 线程 × 50 轮 × 4 查询） |
| 墙钟时间 | `50,393 ms` | `34,177 ms` |
| 单次查询总平均耗时 | `250.43 ms` | `169.46 ms` |
| 读吞吐 | `63.5 QPS` | `93.6 QPS` |
| 并发写完成轮数 | `418,966` 轮（对应 `837,932` 次段合并） | `1,707` 轮（实际 `49.95` 轮/秒，对应 `3,414` 次段合并） |

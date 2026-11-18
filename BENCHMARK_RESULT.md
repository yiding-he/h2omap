# 最终性能测试

## 测试环境

| 项 | 值 |
| --- | --- |
| 代码基线 | `feature/vectorized-zone-pruning`（`d623873`，含段级 zone map 剪枝、单列 `ORDER BY` + `LIMIT` 的段序早停与压测表新增的 `SORT_TS` 列） |
| JDK | `21.0.11`（Microsoft OpenJDK，字节码目标 Java 17） |
| CPU | Apple M4，10 核 |
| 操作系统 | macOS `26.6.2`（`25G83`） |
| 物理内存 | 16 GB |
| 电源模式 | 低电量模式关闭（`pmset -g` 的 `lowpowermode 0`），全程未变 |
| 服务端堆 | `-Xmx8g`（`scripts/final-benchmark.sh` 的 `OMAP_SERVER_XMX` 默认值），客户端 `-Xmx2g` |
| 引擎 | 仅 OMAP（`com.hyd.h2omap.engine.OmapTableEngine`） |
| 部署模式 | 独立 TCP 服务端进程 + 外部纯 H2 客户端进程（客户端不含 `h2omap` Jar） |
| 数据规模 | 5,000,000 行，10 列（`ID` 主键、`LOW_CARD`、`HIGH_CARD`、`INT_COL`、`DOUBLE_COL`、`TS_COL`、`NULLABLE_COL`、`ARRAY_COL`、`STR_ARRAY_COL`、`SORT_TS`） |
| 存储参数 | `segmentSize=65536`，`compactionMinRows=1` |
| 存储形态 | 内存库（`jdbc:h2:mem:`）；第三档改用文件库，数据目录 `target/bench-db`，500 万行占 `562 MB` |
| 查询缓存 | 关闭（`QUERY_CACHE_SIZE=0`） |

本表为 M4 批的环境；i5-14500 复测批的环境与可比性见「实测结果」末尾的说明。

## 复现命令

三档均一键执行（自动完成构建、启动 `-Xmx8g` 独立 TCP 服务端、定位 H2 Jar、执行 50 轮压测、重启校验并自动清理服务端与数据目录）。脚本一次调用跑一种写负载：

```bash
./scripts/final-benchmark.sh                   # 默认：500 万行、16 读、50 轮、写线程不限速（内存库）
./scripts/final-benchmark.sh 50                # 限速：写线程每秒执行 50 轮（每轮含 INSERT+UPDATE+DELETE），内存库
./scripts/final-benchmark.sh 50 --persist      # 同上，改用文件数据库，结束时重启校验行数并清理数据目录
```

端口与堆可用 `OMAP_BENCH_PORT`、`OMAP_SERVER_XMX`、`OMAP_CLIENT_XMX` 覆盖，写限速也可用 `OMAP_WRITER_RPS` 给出；`--keep-data` 保留文件库数据目录（默认在 `target/bench-db`，可用 `OMAP_BENCH_DATA_DIR` 覆盖）。Windows 用等价脚本 `scripts/final-benchmark.ps1`（`-WriterRps 50 -Persist`，另有 `-Rows`/`-Readers`/`-Rounds`/`-ServerXmx`/`-ClientXmx`/`-DataDir`/`-KeepData`/`-SkipBuild` 可调）。

`ps1` 的 `-SkipBuild` 建议配合单独的 `mvn -B -q package -DskipTests` 使用：脚本内部调用 `mvn` 会命中 `mvn.ps1` 包装脚本并在 `Set-StrictMode` 下因未定义变量抛错，该缺陷尚未修复。`ps1` 的重启校验要求行数严格等于 `-Rows`，而写线程每轮先 `INSERT` 后 `DELETE`、停止点可能落在两条语句之间（净行数为 `Rows` 或 `Rows + 1`），因此偶发假失败；`sh` 的校验接受这两个值。

## 并发负载说明

- **读负载**：16 个读线程并发执行 50 轮，每轮顺序执行 `q1`–`q5` 共 5 条 SQL，总计 4,000 次查询。
- **写负载**：1 个写线程在并发读期间持续循环执行单行细粒度变更，每轮依次执行：
  1. `INSERT` 1 行新记录（写入开放段）
  2. `UPDATE` 1 行已有记录
  3. `DELETE` 1 行已有记录
- 因 `compactionMinRows=1`，每轮中的 `UPDATE` 与 `DELETE` 各自触发一次段合并（`compact()`）。

## 查询语句

- q1: `SELECT COUNT(*) FROM T WHERE INT_COL * 2 >= 900000`
- q2: `SELECT COUNT(*) FROM T WHERE LOW_CARD IN ('C0','C1','C2','C3','C4','C5')`
- q3: `SELECT SUM(INT_COL) FROM T WHERE INT_COL > (SELECT AVG(INT_COL) FROM T T2 WHERE T2.LOW_CARD = T.LOW_CARD)`
- q4: `SELECT COUNT(*) FROM T WHERE LOW_CARD <> 'C9' AND INT_COL >= 10000 AND DOUBLE_COL < 950 AND TS_COL >= TIMESTAMP '2026-02-01' AND NULLABLE_COL IS NOT NULL AND CARDINALITY(ARRAY_COL) >= 1 AND CARDINALITY(STR_ARRAY_COL) >= 1`
- q5: `SELECT ID, INT_COL, SORT_TS FROM T WHERE SORT_TS >= TIMESTAMP '2026-02-24 20:53:20' AND LOW_CARD = 'C3' ORDER BY SORT_TS DESC LIMIT 100`（窗口由行数算出，取末尾 3 天即 259,200 行）

## 实测结果

两批实测：Apple M4 三档（不限速写、限速每秒写 50 轮、文件数据库且限速 50 轮/秒）与本机 i5-14500 复测一档（不限速写、内存库）。每轮写含 1 次 INSERT + 1 次 UPDATE + 1 次 DELETE，并因 `compactionMinRows=1` 触发 2 次段合并。

| 查询 | M4 不限速写 | M4 限速 50 轮/秒 | M4 文件数据库且限速 50 轮/秒 | i5-14500 不限速写 |
| --- | --- | --- | --- | --- |
| `q1` | `34.47 ms` | `26.27 ms` | `27.10 ms` | `25.09 ms` |
| `q2` | `48.15 ms` | `37.14 ms` | `40.73 ms` | `28.90 ms` |
| `q3` | `93.37 ms` | `69.56 ms` | `65.38 ms` | `56.40 ms` |
| `q4` | `168.86 ms` | `121.59 ms` | `120.43 ms` | `109.69 ms` |
| `q5` | `14.07 ms` | `12.78 ms` | `13.07 ms` | `10.75 ms` |

两档限速写各复跑一次以确认抖动幅度：内存库 `26.27 / 39.97 / 66.16 / 122.33 / 13.03 ms`（单次总平均 `53.81 ms`、`294.9 QPS`），文件库 `28.05 / 41.01 / 68.24 / 120.85 / 13.10 ms`（`54.25 ms`、`292.1 QPS`）。同一档两次的偏差最大出现在内存库 `q2`（`37.14 → 39.97 ms`，`+7.6%`），其余项在 `±5%` 以内。

### 汇总指标

| 指标 | M4 不限速写 | M4 限速 50 轮/秒 | M4 文件数据库且限速 50 轮/秒 | i5-14500 不限速写 |
| --- | --- | --- | --- | --- |
| 读查询总数 | `4,000`（16 线程 × 50 轮 × 5 查询） | `4,000` | `4,000` | `4,000` |
| 墙钟时间 | `18,135 ms` | `13,510 ms` | `13,458 ms` | `11,720 ms` |
| 单次查询总平均耗时 | `71.78 ms` | `53.47 ms` | `53.34 ms` | `46.17 ms` |
| 读吞吐 | `220.6 QPS` | `296.1 QPS` | `297.2 QPS` | `341.3 QPS` |
| 并发写完成轮数 | `199,352` 轮（约 `10,993` 轮/秒，对应 `398,704` 次段合并） | `674` 轮（`49.9` 轮/秒，对应 `1,348` 次段合并） | `672` 轮（`49.9` 轮/秒，对应 `1,344` 次段合并） | `16,618` 轮（约 `1,419` 轮/秒，对应 `33,236` 次段合并） |
| 重启校验 | — | — | `5,000,000` 行全部可读，数据目录 `562 MB` | — |

本机复测（最后一列）的环境与可比性：

- 环境：Windows 11、Intel Core i5-14500（14 核 / 20 逻辑处理器）、物理内存 `31.69 GB`、电源计划「平衡」、JDK `21.0.12.1`、服务端堆 `-Xmx8g`、内存库，代码基线 `main`（`aa94df5`）。
- 两批是不同机器的对照值，不构成同机回归。代码基线从 `d623873` 到 `aa94df5` 的提交对 `q1`–`q5` 无执行路径变化：S5 关联 `IN` 下推未被该查询集触发，`VarcharVector` 打包只影响非字典 varchar 列（查询集只引用字典列 `LOW_CARD`），行式路径剪枝修复只在回退路径生效。
- 本机档只跑了不限速写一档，未跑限速与文件库档；本机此前一次同参数运行出现过约 7 倍的环境性退化（机器同时承担其他负载），本次已恢复正常，`q1`–`q4` 与健康基线同量级。

文件库档的重启校验在 `SHUTDOWN` 后由独立 JVM 走本地 URL 读取：表定义从 `.mv.db` 恢复，列段与 WAL 从 `target/bench-db/h2omapdb.omap/` 恢复。校验接受 `5,000,000` 与 `5,000,001` 两个值——写线程每轮先 `INSERT` 后 `DELETE`，停止点可能落在两条语句之间。

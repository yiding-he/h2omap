# 最终性能测试

## 测试环境

| 项 | 值 |
| --- | --- |
| 代码基线 | `feature/array-columnar-layout`（`main` 的 `e80de10` 之后，含 M9 数组列列式存储） |
| JDK | `21.0.12.1`（字节码目标 Java 17） |
| CPU | Intel Core i5-14500，14 核 / 20 逻辑处理器 |
| 操作系统 | Windows 11 `10.0.26300` |
| 物理内存 | 31.69 GB |
| 电源模式 | 最佳性能（Windows 11「设置 > 系统 > 电源」，全程未变） |
| 服务端堆 | `-Xmx8g`（`scripts/final-benchmark.ps1` 的 `-ServerXmx` 默认值），客户端 `-Xmx2g` |
| 引擎 | 仅 OMAP（`com.hyd.h2omap.engine.OmapTableEngine`） |
| 部署模式 | 独立 TCP 服务端进程 + 外部纯 H2 客户端进程（客户端不含 `h2omap` Jar） |
| 数据规模 | 10,000,000 行，9 列（`ID` 主键、`LOW_CARD`、`HIGH_CARD`、`INT_COL`、`DOUBLE_COL`、`TS_COL`、`NULLABLE_COL`、`ARRAY_COL`、`STR_ARRAY_COL`） |
| 存储参数 | `segmentSize=65536`，`compactionMinRows=1` |
| 存储形态 | 内存库（`jdbc:h2:mem:`）；文件模式用于观察写路径代价，数据目录 `target\bench-db`，1000 万行占 `969.4 MB` |
| 查询缓存 | 关闭（`QUERY_CACHE_SIZE=0`） |

`电源模式`指 Windows 11 设置的电源模式；`powercfg /getactivescheme` 返回的旧版电源计划在本机一直是「平衡」，与实测无关，不作为基准条件记录。

## 复现命令

一键执行（自动完成构建、启动 `-Xmx8g` 独立 TCP 服务端、定位 H2 Jar、执行 50 轮压测并自动清理服务端进程）。脚本一次调用跑一种写负载，三种写负载需要分三次调用：

```powershell
./scripts/final-benchmark.ps1                  # 默认：1000 万行、16 读、50 轮、写线程不限速
./scripts/final-benchmark.ps1 -WriterRps 50    # 限速：写线程每秒执行 50 轮（每轮含 INSERT+UPDATE+DELETE）
./scripts/final-benchmark.ps1 -WriterRps 50 -Persist   # 同上，改用文件数据库，结束时重启校验行数
```

其余参数：`-Rows`、`-Readers`、`-Rounds`、`-ServerXmx`、`-ClientXmx`、`-Port`、`-Engine`、`-NoWriter`、`-SkipBuild`。

`-SkipBuild` 建议配合单独的 `mvn -B -q package -DskipTests` 使用：脚本内部调用 `mvn`，在 Windows PowerShell 上会命中 `mvn.ps1` 包装脚本并在 `Set-StrictMode` 下因未定义变量抛错。该缺陷尚未修复。

Linux 用等价脚本 `./scripts/final-benchmark.sh`（同样以位置参数或环境变量给出写限速）。

若需手动分步执行：

```powershell
mvn -B -q package

java -Xmx8g -cp target\h2omap-0.1.0-SNAPSHOT.jar `
  org.h2.tools.Server -tcp -tcpPort 19092 -ifNotExists

java -Xmx2g `
  -cp "$env:USERPROFILE\.m2\repository\com\h2database\h2\2.5.252\h2-2.5.252.jar;target\test-classes" `
  com.hyd.h2omap.bench.ConcurrentQueryBench `
  16 10000000 50 true omap `
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
- q3: `SELECT SUM(INT_COL) FROM T WHERE INT_COL > (SELECT AVG(INT_COL) FROM T T2 WHERE T2.LOW_CARD = T.LOW_CARD)`（M10 之后替换；此前为 `SELECT SUM(INT_COL) FROM T WHERE INT_COL >= 400000`，下方历史表中 `q3` 的数值均属旧查询）
- q4: `SELECT COUNT(*) FROM T WHERE LOW_CARD <> 'C9' AND INT_COL >= 10000 AND DOUBLE_COL < 950 AND TS_COL >= TIMESTAMP '2026-02-01' AND NULLABLE_COL IS NOT NULL AND CARDINALITY(ARRAY_COL) >= 1 AND CARDINALITY(STR_ARRAY_COL) >= 1`

## 实测结果

三个场景各跑一次，每次为独立的服务端进程与全新数据库：不限速写（`./scripts/final-benchmark.ps1`）、限速每秒写 50 轮（`./scripts/final-benchmark.ps1 -WriterRps 50`）与文件数据库且限速 50 轮/秒（再加 `-Persist`）。每轮写含 1 次 INSERT + 1 次 UPDATE + 1 次 DELETE，并因 `compactionMinRows=1` 触发 2 次段合并。

| 查询 | 不限速写 | 限速 50 轮/秒 | 文件数据库且限速 50 轮/秒 |
| --- | --- | --- | --- |
| `q1` | `49.68 ms` | `47.72 ms` | `45.03 ms` |
| `q2` | `57.62 ms` | `56.56 ms` | `54.09 ms` |
| `q3` | `46.41 ms` | `43.61 ms` | `41.79 ms` |
| `q4` | `173.89 ms` | `161.62 ms` | `152.27 ms` |

### 汇总指标

| 指标 | 不限速写 | 限速 50 轮/秒 | 文件数据库且限速 50 轮/秒 |
| --- | --- | --- | --- |
| 读查询总数 | `3,200`（16 线程 × 50 轮 × 4 查询） | `3,200` | `3,200` |
| 墙钟时间 | `16,557 ms` | `15,612 ms` | `14,756 ms` |
| 单次查询总平均耗时 | `81.90 ms` | `77.38 ms` | `73.29 ms` |
| 读吞吐 | `193.3 QPS` | `205.0 QPS` | `216.9 QPS` |
| 并发写完成轮数 | `25,659` 轮（对应 `51,318` 次段合并） | `780` 轮（实际 `50.0` 轮/秒，对应 `1,560` 次段合并） | `737` 轮（实际 `50.0` 轮/秒，对应 `1,474` 次段合并） |
| 重启校验 | — | — | `10,000,000` 行全部可读，数据目录 `969.4 MB` |

## 与上一台机器的记录对照

下表是 `2090985` 在另一台机器上测得的值（CPU 为 i7-12700H、JDK 为 21.0.3、服务端堆 `-Xmx6g`），列在此处供追溯，**与本次数值不可直接比较**：

| 查询 | 不限速写 | 限速 50 轮/秒 | 文件数据库且限速 50 轮/秒 |
| --- | --- | --- | --- |
| `q1` | `66.30 ms` | `53.49 ms` | `51.19 ms` |
| `q2` | `85.62 ms` | `63.63 ms` | `64.30 ms` |
| `q3` | `51.24 ms` | `48.46 ms` | `45.45 ms` |
| `q4` | `683.47 ms` | `487.06 ms` | `370.59 ms` |
| 墙钟时间 | `44,631 ms` | `32,842 ms` | `26,706 ms` |
| 读吞吐 | `71.7 QPS` | `97.4 QPS` | `119.8 QPS` |

两处环境差异（CPU 由 i7-12700H 换为 i5-14500、JDK 由 21.0.3 换为 21.0.12.1）与一次代码变更（M9 数组列列式存储）叠加在同一组对比里，无法从中分离单项贡献。`q4` 的单项归因见 `problems/查询性能优化方向.md` 第五节：同机、同堆（`-Xmx12g`）、同写负载为零的条件下，`q4` 由 `654.27 ms` 降到 `159.60 ms`。

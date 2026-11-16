# 最终性能测试

## 测试环境

| 项 | 值 |
| --- | --- |
| 代码基线 | `feature/subquery-constant-pushdown`（`main` 的 `5428d14` 之后，含 M10 子查询下推与基准 `q3` 替换） |
| JDK | `21.0.12.1`（字节码目标 Java 17） |
| CPU | Intel Core i5-14500，14 核 / 20 逻辑处理器 |
| 操作系统 | Windows 11 `10.0.26300` |
| 物理内存 | 31.69 GB |
| 电源模式 | 最佳性能（Windows 11「设置 > 系统 > 电源」，全程未变） |
| 服务端堆 | `-Xmx8g`（`scripts/final-benchmark.ps1` 的 `-ServerXmx` 默认值），客户端 `-Xmx2g` |
| 引擎 | 仅 OMAP（`com.hyd.h2omap.engine.OmapTableEngine`） |
| 部署模式 | 独立 TCP 服务端进程 + 外部纯 H2 客户端进程（客户端不含 `h2omap` Jar） |
| 数据规模 | 5,000,000 行，9 列（`ID` 主键、`LOW_CARD`、`HIGH_CARD`、`INT_COL`、`DOUBLE_COL`、`TS_COL`、`NULLABLE_COL`、`ARRAY_COL`、`STR_ARRAY_COL`） |
| 存储参数 | `segmentSize=65536`，`compactionMinRows=1` |
| 存储形态 | 内存库（`jdbc:h2:mem:`）；文件模式用于观察写路径代价，数据目录 `target\bench-db`，500 万行占 `484.5 MB` |
| 查询缓存 | 关闭（`QUERY_CACHE_SIZE=0`） |

`电源模式`指 Windows 11 设置的电源模式；`powercfg /getactivescheme` 返回的旧版电源计划在本机一直是「平衡」，与实测无关，不作为基准条件记录。

## 复现命令

一键执行（自动完成构建、启动 `-Xmx8g` 独立 TCP 服务端、定位 H2 Jar、执行 50 轮压测并自动清理服务端进程）。脚本一次调用跑一种写负载，三种写负载需要分三次调用：

```powershell
./scripts/final-benchmark.ps1                  # 默认：500 万行、16 读、50 轮、写线程不限速
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
  16 5000000 50 true omap `
  "jdbc:h2:tcp://localhost:19092/mem:omap5m;DB_CLOSE_DELAY=-1;QUERY_CACHE_SIZE=0"
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

数据规模自本次起改为 500 万行（此前为 1000 万行）；下节「历史记录」保留旧数据供追溯。

| 查询 | 不限速写 | 限速 50 轮/秒 | 文件数据库且限速 50 轮/秒 |
| --- | --- | --- | --- |
| `q1` | `27.85 ms` | `24.87 ms` | `24.14 ms` |
| `q2` | `31.38 ms` | `28.45 ms` | `28.47 ms` |
| `q3` | `63.44 ms` | `56.97 ms` | `50.50 ms` |
| `q4` | `128.89 ms` | `117.92 ms` | `98.60 ms` |

### 汇总指标

| 指标 | 不限速写 | 限速 50 轮/秒 | 文件数据库且限速 50 轮/秒 |
| --- | --- | --- | --- |
| 读查询总数 | `3,200`（16 线程 × 50 轮 × 4 查询） | `3,200` | `3,200` |
| 墙钟时间 | `12,797 ms` | `11,609 ms` | `10,186 ms` |
| 单次查询总平均耗时 | `62.89 ms` | `57.05 ms` | `50.43 ms` |
| 读吞吐 | `250.1 QPS` | `275.6 QPS` | `314.2 QPS` |
| 并发写完成轮数 | `12,089` 轮（对应 `24,178` 次段合并） | `580` 轮（实际 `50.0` 轮/秒，对应 `1,160` 次段合并） | `508` 轮（实际 `49.9` 轮/秒，对应 `1,016` 次段合并） |
| 重启校验 | — | — | `5,000,000` 行全部可读，数据目录 `484.2 MB` |

### `q3` 的执行成本

`q3` 是四条中唯一含子查询的查询：每次执行对内层表做一次分组聚合预计算，再做外层扫描。同样 9 列数据、500 万行，单线程与 16 线程分别执行（各预热后取 5 次）：

| 指标 | 单线程 | 16 线程 |
| --- | --- | --- |
| 单次墙钟 | `39 ms` | `50 ms` |
| 单次堆分配 | `2.5 MB` | `2.5 MB` |

16 线程 × 50 轮（共 800 次执行，与压测读阶段的 `q3` 执行次数一致）期间：`3` 次 Young GC、累计暂停 `103 ms`。作为对照，`q1` 单次分配 `1.4 MB`。

两轮优化的效果：先去掉内层预计算的逐行 `Value` 装箱与逐行哈希（1000 万行单线程 `373 → 208 ms`、16 线程 `2,524 → 1,314 ms`），再把外层关联键的快路径扩展到字典字符串列（`LOW_CARD` 为 `DictVarcharVector`，此前走通用路径逐行装箱）：

| 指标 | 修复前 | 修复后 |
| --- | --- | --- |
| `q3` 单线程（500 万行） | `101 ms` | `39 ms` |
| `q3` 16 线程（500 万行） | `651 ms` | `50 ms` |
| 单次堆分配 | `78.4 MB` | `2.5 MB` |
| 16 线程 × 50 轮的 Young GC | `46` 次，暂停 `314 ms` | `3` 次，暂停 `103 ms` |
| 压测中的 `q3` 单次耗时 | `651 – 707 ms` | `51 – 63 ms` |

## 历史记录

### 本机 1000 万行、M9 基线（`q3` 为替换前旧查询）

| 查询 | 不限速写 | 限速 50 轮/秒 | 文件数据库且限速 50 轮/秒 |
| --- | --- | --- | --- |
| `q1` | `49.68 ms` | `47.72 ms` | `45.03 ms` |
| `q2` | `57.62 ms` | `56.56 ms` | `54.09 ms` |
| `q3`（旧） | `46.41 ms` | `43.61 ms` | `41.79 ms` |
| `q4` | `173.89 ms` | `161.62 ms` | `152.27 ms` |
| 墙钟时间 | `16,557 ms` | `15,612 ms` | `14,756 ms` |
| 读吞吐 | `193.3 QPS` | `205.0 QPS` | `216.9 QPS` |

与现行数据的差异同时来自数据规模（1000 万 → 500 万）、`q3` 查询替换、M10 代码与 `q3` 的两轮优化，不可分离单项贡献。

### 上一台机器（`2090985`）

下表是 `2090985` 在另一台机器上测得的值（CPU 为 i7-12700H、JDK 为 21.0.3、服务端堆 `-Xmx6g`、数据规模 1000 万行、`q3` 为替换前的旧查询），列在此处供追溯，**与本次数值不可直接比较**：

| 查询 | 不限速写 | 限速 50 轮/秒 | 文件数据库且限速 50 轮/秒 |
| --- | --- | --- | --- |
| `q1` | `66.30 ms` | `53.49 ms` | `51.19 ms` |
| `q2` | `85.62 ms` | `63.63 ms` | `64.30 ms` |
| `q3` | `51.24 ms` | `48.46 ms` | `45.45 ms` |
| `q4` | `683.47 ms` | `487.06 ms` | `370.59 ms` |
| 墙钟时间 | `44,631 ms` | `32,842 ms` | `26,706 ms` |
| 读吞吐 | `71.7 QPS` | `97.4 QPS` | `119.8 QPS` |

两处环境差异（CPU 由 i7-12700H 换为 i5-14500、JDK 由 21.0.3 换为 21.0.12.1）与一次代码变更（M9 数组列列式存储）叠加在同一组对比里，无法从中分离单项贡献。`q4` 的单项归因见 `problems/查询性能优化方向.md` 第五节：同机、同堆（`-Xmx12g`）、同写负载为零的条件下，`q4` 由 `654.27 ms` 降到 `159.60 ms`。

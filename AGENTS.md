# AGENTS.md — h2omap 工程说明

本文件面向在本仓库工作的人与代理，记录设计与实现层面的约定、依据和当前状态。使用方式见 `README.md`。

## 项目定位

OMAP 是 H2 的外部表引擎（`org.h2.api.TableEngine`），**不修改 H2 源码**。H2 提供 SQL 前端、JDBC、优化器、元数据；本项目只替换表的存储与索引。

| 需求 | 结论 | 谁来做 |
| --- | --- | --- |
| 纯内存 | H2 已支持 | 本项目只管理自己的内存结构 |
| JDBC 兼容 | 完全复用 H2 | `org.h2.jdbc` 直接可用，不用重写 |
| 列式存储 | 需要实现 | 本项目 |
| OLAP 扫描 | 需要实现 | 列段、zone map、迟物化 |
| 高频细粒度变更 | 需要实现 | 行式 delta 缓冲加定期合并 |
| 事务 | 不支持 | 见「无事务的三个坑」 |

## 工作约定

| 项 | 约定 |
| --- | --- |
| 构建 | `mvn -B -q package` |
| 测试 | `mvn -B test`；一次性运行 Gradle 时加 `--no-daemon`（本仓库用 Maven） |
| JDK | 构建与运行用 JDK 21.0.11；字节码目标 `maven.compiler.release=17` |
| H2 版本 | 固定 `2.5.252`，不跟进 `2.5.259-SNAPSHOT`（`canGetRowCount` 等签名仍在变） |
| 依赖 | 只有 `com.h2database:h2` 与 `junit-jupiter` |
| 判定依据 | `src/test/java/com/hyd/h2omap/OmapTableContractTest.java`（42 个用例）、`src/test/java/com/hyd/h2omap/storage/ColumnarStoreTest.java`（10 个用例）、`src/test/java/com/hyd/h2omap/vector/ValueVectorTest.java`（3 个用例）与 `src/test/java/com/hyd/h2omap/bench/DataScaleTest.java`（3 个用例）。改动表、索引或存储实现后必须跑通；不得为了让改动通过而放宽、跳过或删除用例 |
| 查 H2 源码 | `.h2/` 目录存放本仓库依赖的 H2 `2.5.252` 源码包内容（`.h2/main/org/h2/...`、`.h2/test/org/h2/...`、`.h2/docsrc/html/...`），已在 `.gitignore` 中排除，属本地参考。重新获取：`https://github.com/h2database/h2database/releases/download/version-2.5.252/h2-2026-09-23.zip`，只取其中的 `h2/src/` |

## H2 侧的依据

下文路径以 `.h2/main/org/h2/` 为基准。

### 接入点存在的证据

| 事实 | 位置 |
| --- | --- |
| 官方扩展接口 | `api/TableEngine.java:25`，`Table createTable(CreateTableData data)` |
| 走引擎时不经过 `Store` | `schema/Schema.java`，`createTable()` 里 `tableEngine != null` 直接调 `database.getTableEngine(...).createTable(data)` |
| 基类构造器公开 | `table/TableBase.java:62`，`public TableBase(CreateTableData data)` |
| 索引基类构造器可继承 | `index/Index.java:101`，`protected Index(...)` |
| 语法与文档 | `res/help.csv:970` 有 `[ ENGINE tableEngineName ]`，`:993-998` 说明必须实现 `org.h2.api.TableEngine` |
| 全局默认引擎 | `engine/DbSettings.java:261`，`DEFAULT_TABLE_ENGINE`（不可用于本引擎，见 README） |
| 没有 JPMS 限制 | H2 `2.5.252` jar 的 `MANIFEST.MF` 只有 `Automatic-Module-Name: com.h2database`，没有 `module-info.java` |
| H2 自己的样板 | `.h2/test/org/h2/test/db/TestTableEngines.java`，`TreeSetTable`(:777) 加 `TreeSetIndex`(:896) 加 `IteratorCursor`(:1025) |

`TreeSetTable` 那个样例约 300 行，就是一个可用的外部内存表，可以当骨架参考。

### 换不掉的三件事

| 限制 | 位置 |
| --- | --- |
| 执行是行式 | `expression/Expression.java:113`，`Value getValue(SessionLocal session)` 一次一行 |
| 事务管理器不可替换 | `engine/SessionLocal.java:77`，`public final class SessionLocal`，`commit`、`rollback` 直接调 `TransactionStore` |
| 元数据仍在 MVStore 里 | `engine/Database.java:332`，打开数据库时无条件 `new Store(...)` |

本项目接受这三条限制。第三条约等于“catalog 在内存 MVStore 里”，占用很小，不处理。

## 事务豁免的三个依据

不做事务不需要改造 `SessionLocal`，下面三处代码已经避开了外部引擎表。

| 行为 | 位置 |
| --- | --- |
| 不会被加锁 | `table/Table.java:1423` 的 `isRowLockable()` 默认返回 `false`，只有 `mvstore/db/MVTable.java:654` 改成 `true` |
| savepoint 空转 | `engine/SessionLocal.java`，`rollbackTo()` 里 `if (tx != null)` 判断，`transaction` 为 null 时什么都不做 |
| 事务收尾跳过 | `engine/SessionLocal.java`，`markUsedTablesAsUpdated()` 只处理 `instanceof MVTable` |

## 存储设计

列式和“高频点改”天然矛盾。这里用“列段加行式 delta”，也就是 delta store 的常规做法。段不是严格不可变：合并会把 delta 的改动写回段，删除只打标记，两者都不改变 rowId。

```
OmapTable
├── segments : List<ColumnSegment>      按段号升序，可原地改写
│     每列：primitive 数组 + null 位图 + zone map（可选字典）
│     段级：删除位图（每行 1 bit）
└── delta    : DeltaStore               行式写缓冲
      ├── updated        已冻结段内被更新行的新像
      └── segmentDeletes 已冻结段内已删除行的 rowId

openSegment : SegmentBuilder            当前开放段的行缓冲，写满即冻结成段
rowId = 段号 << 32 | 段内偏移
```

rowId 在 `append` 时就确定，之后不变。段号在开放段创建时预先分配（等于 `nextSegmentId`），段内偏移就是写入顺序，所以冻结成段不改变任何已有 rowId；H2 把 rowId 存进 `Row.key` 并在后续语句里当行标识用，这一条是硬要求。段号单调递增、不回收（`truncate` 也不回收），`segment(int)` 靠段号与首段号的差定位列表下标，因此段从不被删除时两者一致。

写入路径全部是 O(1)，不重写列：

| H2 调用 | 本项目实现 |
| --- | --- |
| `addRow` | 追加进开放段，不动已冻结的列段 |
| `removeRow` | 由 `row.getKey()` 反解 rowId，已冻结段记进 `segmentDeletes`，开放段记进 `openDeletes` |
| `updateRows` | 已冻结段记进 `updated`，开放段整体替换 builder 里的值数组 |
| `truncate` | 清空 segments 和 delta，段号不回收，返回旧行数 |

读取路径：`ColumnarStore.RowIterator` 按段推进，每次 `next()` 自己取读锁，跳过删除位图标记的行；`OmapCursor.next()` 拿到 rowId 后立刻物化并缓存，避免「迭代器说可见、物化时已被删」的空行。`OmapScanIndex.find` 把 H2 的 `first`/`last` 边界翻译成每列的上下界，迭代器用 zone map 跳过不相交的段。

迭代器是创建时刻的快照：构造时固定最后一个段号与开放段的行数。开放段在迭代期间被写满冻结时，迭代器改从段列表读它，rowId 不变；冻结之后新开的段不在本次快照内。

合并（compaction）：`delta` 的改动数达到 `compactionMinRows` 时，在写锁内把新值写回段、把删除写进段的删除位图，然后清空 delta 并重算被触及段的 zone map。段号与段内偏移都不变，所以 rowId 稳定，读路径不需要快照，段也从不被删除。

## 与 H2 的契约

### 必须实现的方法

`Table` 有 15 个抽象方法：`close`、`addIndex`、`removeRow`、`truncate`、`addRow`、`checkSupportAlter`、`getTableType`、`getScanIndex`、`getIndexes`、`getMaxDataModificationId`、`isDeterministic`、`canGetRowCount`、`canDrop`、`getRowCount`、`getRowCountApproximation`。

`Index` 有 10 个：`close`、`add`、`remove`、`find`、`getCost`、`remove`、`truncate`、`needRebuild`、`getRowCount`、`getRowCountApproximation`。

`Cursor` 有 4 个：`get`、`getSearchRow`、`next`、`previous`。

### 必须遵守的硬性约束

下面每一条都会出错，且出错方式不直观。除最后一条外，都有 `OmapTableContractTest` 里的用例检查；最后一条由存储层不提供就地写入路径来保证。

| 约束 | 不遵守的后果 | 依据 |
| --- | --- | --- |
| `Index.getCreateSQL()` 返回 `null` | H2 认为该索引已经提供行顺序，跳过 `ORDER BY` 排序，结果按 rowId 顺序返回 | `Select.java:638` |
| `Table.canTruncate()` 返回 `true` | `TRUNCATE TABLE` 被拒绝，报 `Cannot truncate` | `TruncateTable.java:41` |
| `Table.isRowLockable()` 保持 `false` | 返回 `true` 会让 H2 调 `lockRow`，默认实现直接抛 `UnsupportedException` | `Table.java:1423` |
| `Table.isDeterministic()` 返回 `true` | 返回 `false` 会让 `Query.getNoCache()` 禁用结果集缓存，导致标量子查询按外层行数重复执行 | `Query.java:474`、`Select.java:1707` |
| `getMaxDataModificationId()` 与 `Database.getNextModificationDataId()` 同步并随每次写入单调递增 | 使用独立计数会大于全局 `now` 导致缓存永不生效；不递增则会命中过期缓存读到旧行 | `Database.java:442`、`Query.java:538,551`、`Select.java:1715` |
| `addIndex` 自己把已有行回填进索引 | `ALTER TABLE ADD PRIMARY KEY` 之后，经该索引的点查查不到任何行 | `MVTable.java:368` |
| `updateRows` 按「先前像、后新像」成对读取 `LocalResult` | UPDATE 写到错误的行 | `Table.java:525` |
| 唯一约束失败时自己撤回已写入的行与索引项 | 无事务，异常后脏行留在 delta，计数与内容都不对 | `Table.java:541` |
| 不得就地改写已经交给 H2 的 `Value[]` | 游标返回的 `Row` 直接引用该数组，改写会篡改 H2 正在使用的行 | `Cursor.get()` |

`addIndex` 在 DDL 带 `PRIMARY KEY` 或 `UNIQUE` 时会被调用。本项目用它建 rowId 点查索引，支撑 UPDATE 和 DELETE。

`OmapRowIdIndex.find()` 的区间语义：`first`/`last` 是 `SearchRow`，只填了索引列，其余列为 null（null 元素表示该侧无界）；H2 在倒序时已经把两者交换（`IndexCursor.find()`），所以实现里先取正序区间，再按需 `descendingMap()`。等值查询是区间的退化形式，与区间共用同一条路径。索引条目用 `ConcurrentSkipListMap` 维护，因为 `find` 在存储锁之外迭代，而写路径并发 `put`/`remove`，`TreeMap` 会直接产生 `ConcurrentModificationException`（已由并发用例实证）。

索引不进元数据：重开数据库时 H2 只重放 `CREATE TABLE ... ENGINE` 与 `ALTER TABLE ADD CONSTRAINT`，后者会让 `addIndex` 在空表上重新执行，所以持久化场景下回填实际是空操作。

`Index.needRebuild()` 的返回值对外部引擎表没有行为影响：H2 只在 `MVTable.addIndex` 里读它，那段代码走不到。不要指望它触发重建。

### 无事务的三个坑

1. 失败的部分更新不会回滚。默认 `updateRows` 在异常时调 `session.rollbackTo(rollback)`，而 `transaction` 为 null 时它什么都不做。所以 `addRow` 与 `updateRows` 在索引报错时必须自己把存储与索引两项都退回去。
2. 并发写要自己保。`addRow` 和 `removeRow` 没有锁契约，多个连接会并发进入，delta 内部必须自己加锁。
3. `SELECT ... FOR UPDATE` 依赖 `isRowLockable()`，保持 `false` 就不会走到锁逻辑，语句能正常执行但不加锁。

## 已知天花板

行式求值无法绕过。列向量在 `Cursor.get()` 处被拆成 `Row`，也就是 `Value[]`，每行每列一次装箱。

可以缓解的两点：

- 用 `SET LAZY_QUERY_EXECUTION TRUE` 走流式结果，避免 `LocalResult` 全量物化（`.h2/main/org/h2/result/LocalResult.java:68` 是 `ArrayList<Value[]>`）。
- 只物化被引用的列，其余列留在向量里。

## 模块结构

```
src/main/java/com/hyd/h2omap/
├── engine/        接入 H2：TableEngine 与 WITH 参数
├── table/         OmapTable，继承 TableBase
├── index/         扫描索引、rowId 索引、游标
├── storage/       列段、开放段缓冲、delta、rowId 编码
├── persistence/   WAL 增量日志、增量列段快照与重启恢复
├── vector/        列向量（专用类型）与 ValueVector 回退、null 位图
└── stats/         zone map
```

```
src/test/java/com/hyd/h2omap/
├── OmapTableContractTest.java    H2 接入契约
├── storage/ColumnarStoreTest.java 存储层
├── vector/ValueVectorTest.java   回退向量
└── bench/                        测试数据生成与规模验证
```

`bench/DataGenerator.java` 是固定种子（20260923）的数据生成器，同时建 OMAP 表 `T` 与 MVStore 对比表 `T_MV`，两表数据完全一致。表结构 9 列：`ID` 主键、`LOW_CARD`（10 取值）、`HIGH_CARD`、`INT_COL`（部分连续部分随机）、`DOUBLE_COL`、`TS_COL`、`NULLABLE_COL`（约 5% null）、`ARRAY_COL`、`STR_ARRAY_COL`。

命令行用法：`java -cp "target/h2omap-0.1.0-SNAPSHOT.jar:<h2 jar>:target/test-classes" com.hyd.h2omap.bench.DataGenerator [rows] [url] [omap|mv]`，默认 100 万行、内存库 `omapbench`、OMAP 引擎。第三参数选 `omap` 或 `mv`，两种引擎分开跑，每次只建一张表 `T`；命令行走 `benchSetup`（先插满预热，再 `TRUNCATE` 清空重插，最后跑查询），MVStore 作为基准只在测试条件变化时才重跑。`DataScaleTest` 用 1 万行加 `segmentSize=1000`、`compactionMinRows=1000` 跑默认测试，同时建两表做签名对比；`DataScaleLargeTest` 用 100 万行，需 `-Domap.scale=large -DargLine=-Xmx3g` 才运行。

`bench/ConcurrentQueryBench.java` 用于多线程并发查询基准（`[threads] [rows] [rounds] [withWriter] [engine] [url]`），支持进程内直连与独立 TCP 服务端进程两种模式：

- 独立服务端启动：`java -Xmx3g -cp target/h2omap-0.1.0-SNAPSHOT.jar org.h2.tools.Server -tcp -tcpPort 19092 -ifNotExists`。
- 外部纯 H2 客户端压测（16 读 + 1 写 + 100 万行，客户端无需引入 `h2omap` Jar）：`java -Xmx1g -cp "$HOME/.m2/repository/com/h2database/h2/2.5.252/h2-2.5.252.jar:target/test-classes" com.hyd.h2omap.bench.ConcurrentQueryBench 16 1000000 5 true omap "jdbc:h2:tcp://localhost:19092/mem:omapconcurrent;DB_CLOSE_DELAY=-1;QUERY_CACHE_SIZE=0"`。

`src/test/java/com/hyd/h2omap/OmapTableContractTest.java` 是 H2 接入契约的集成测试；`src/test/java/com/hyd/h2omap/storage/ColumnarStoreTest.java` 直接驱动存储层，断言段数与 rowId 稳定性；`src/test/java/com/hyd/h2omap/vector/ValueVectorTest.java` 断言回退向量的内存估算随值内容变化。

## 当前状态与后续阶段

M0（接入契约验证）、M1（段真正产生）、M2（剪枝与流式扫描）、M3（合并与并发）与 M4（索引区间查询）已完成：42 个契约用例、10 个存储层用例、3 个向量用例与 3 个规模用例全部通过，另有 1 个 100 万行用例需显式开启。生产代码改动见 git 历史。后续每个阶段都以可运行的集成测试作为结束条件。

| 阶段 | 目标 | 状态 |
| --- | --- | --- |
| M0 接入契约验证 | 真实调用下 CREATE/INSERT/SELECT/UPDATE/DELETE/TRUNCATE/DROP 全通 | 完成 |
| M1 段真正产生 | rowId 插入时确定；`append` 累积到 `segmentSize` 冻结成 `ColumnSegment`；读路径先段后 delta | 完成 |
| M2 剪枝与流式扫描 | `OmapScanIndex.find` 用 zone map 剪枝；扫描按段惰性推进 | 完成 |
| M3 合并与并发 | `compact()` 就地写回段；删除位图；读迭代器快照 | 完成（后台合并线程未做） |
| M4 索引 | `OmapRowIdIndex` 区间查询；唯一约束；`addIndex` 回填的规模验证 | 完成 |
| M5 编码与性能 | Varchar 字典编码、列裁剪物化、与 MVStore 表的量化对比 | 未开始 |
| M6 判决点 | 给出「行式物化开销占总耗时比例」的实测结论 | 未开始 |

已确认的设计决策：

- rowId 在插入时按「预分配的段号 + 段内偏移」编码，冻结与合并都不改变已有 rowId。
- 非 Int/Long/Double/Varchar 的列用 `ValueVector` 原样存放，保证段机制对所有类型生效，专用向量按需逐列补。
- 冻结同步在 `append` 的写锁内完成，不交给后台线程。
- 合并采用「就地写回」而不是「写新段」：段内增加删除位图，delta 的新值写回列向量，rowId 全程不变，因此不需要段集合快照与引用计数。
- 合并同步在写锁内触发（`append`/`remove`/`update` 后判断 delta 大小），不交给后台线程。
- 读迭代器采用创建时刻快照：固定最后一个段号与开放段行数，开放段冻结后从段列表继续读，冻结后新开的段不可见。
- zone map 比较失败（JSON、几何等不可比较的类型）时该列退化为「恒相交」，不参与剪枝；剪枝只是优化，不能让它阻止建段。
- 主键与唯一索引用 `ConcurrentSkipListMap` 维护「索引键 → rowId」，`find` 按 `subMap`/`tailMap`/`headMap` 取区间后在存储锁之外迭代。`getCost` 沿用 `getCostRangeIndex` 且不额外加码——加码会让范围查询永远选扫描索引，区间分支不会被调用。
- 两个索引的 `getCreateSQL()` 都保持返回 `null`：区间分支虽然按索引列有序返回，但顺序保证由 H2 的 `Select.getIndexSorts` 判断，交给扫描加排序更稳妥。
- 性能数字只在 M5 交付，M0—M5 只要求集成测试与内部计数器断言。

## 尚未实现

- 低基数列的字典编码。
- 并发写的后台合并线程：现在合并同步在写锁内触发。
- 数组列的按元素存储与剪枝：目前整值存放在 `ValueVector` 里，zone map 只能整值比较，无法按元素过滤。
- 行数超过 `Integer.MAX_VALUE` 时 `collectVisibleRowIds` 的数组分配会溢出（用 `(int) Math.min(rowCount, Integer.MAX_VALUE)` 分配，实际写入会越界）。

## 待核对

基线是 `2.5.252`，`Table`、`Index`、`Cursor` 的签名已逐条用 `javap` 核对。剩下两点：

- `CreateTableData` 位于 `org.h2.command.ddl`，却被 `org.h2.api.TableEngine` 的签名引用。这是事实上的公开接口，H2 升级时需要复核。
- `OmapTableEngine` 目前只解析 `WITH` 参数，不解析表名里的 schema 前缀。

## 参考

- H2 架构文档：`.h2/docsrc/html/architecture.html`（第 7 到 8 层已过时，PageStore 已删除）
- MVStore 设计：`.h2/docsrc/html/mvstore.html`
- 外部表引擎样例：`.h2/test/org/h2/test/db/TestTableEngines.java`

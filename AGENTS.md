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
| 判定依据 | `src/test/java/com/hyd/h2omap/OmapTableContractTest.java`，22 个用例。改动表或索引实现后必须跑通；不得为了让改动通过而放宽、跳过或删除用例 |
| 查 H2 源码 | 仓库内没有 `h2/` 目录。下载 `https://maven.aliyun.com/repository/public/com/h2database/h2/2.5.252/h2-2.5.252-sources.jar` 解开即可（实测可用），下文行号均以此为准 |

## H2 侧的依据

下文路径以 `h2/src/main/org/h2/` 为基准。

### 接入点存在的证据

| 事实 | 位置 |
| --- | --- |
| 官方扩展接口 | `api/TableEngine.java:25`，`Table createTable(CreateTableData data)` |
| 走引擎时不经过 `Store` | `schema/Schema.java`，`createTable()` 里 `tableEngine != null` 直接调 `database.getTableEngine(...).createTable(data)` |
| 基类构造器公开 | `table/TableBase.java:62`，`public TableBase(CreateTableData data)` |
| 索引基类构造器可继承 | `index/Index.java:101`，`protected Index(...)` |
| 语法与文档 | `res/help.csv:970` 有 `[ ENGINE tableEngineName ]`，`:993-998` 说明必须实现 `org.h2.api.TableEngine` |
| 全局默认引擎 | `engine/DbSettings.java:261`，`DEFAULT_TABLE_ENGINE`（不可用于本引擎，见 README） |
| 没有 JPMS 限制 | `h2/pom.xml:255` 只有 `Automatic-Module-Name: com.h2database`，没有 `module-info.java` |
| H2 自己的样板 | `src/test/org/h2/test/db/TestTableEngines.java`，`TreeSetTable`(:777) 加 `TreeSetIndex`(:896) 加 `IteratorCursor`(:1025) |

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

列式和“高频点改”天然矛盾。这里用“不可变列段加行式 delta”，也就是 delta store 的常规做法。

```
OmapTable
├── segments : List<ColumnSegment>      不可变、只读、按插入顺序切分
│     每列：primitive 数组 + null 位图 + zone map（可选字典）
├── delta    : DeltaStore               行式写缓冲
│     ├── insertLog  追加写
│     ├── deleteSet  已删除的 rowId
│     └── updateMap  rowId 到新行
└── rowId = 段号 << 32 | 段内偏移，delta 行用最高位区分
```

写入路径全部是 O(1) 到 O(log n)，不重写列：

| H2 调用 | 本项目实现 |
| --- | --- |
| `addRow` | 追加进 `delta.insertLog`，不动 segments |
| `removeRow` | 由 `row.getKey()` 反解 rowId，记进 `deleteSet` |
| `updateRow` | 旧 rowId 打删除标记，新行进 `updateMap` |
| `truncate` | 清空 segments 和 delta，返回旧行数 |

读取路径：`find()` 先用 zone map 剪掉不相关的段，再合并 segments 与 delta，并按 `deleteSet` 和 `updateMap` 过滤，输出时物化 `Row`。

合并（compaction）：`delta` 超过阈值后，把 delta 与受影响段重写成新段，旧段整段丢弃。

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
| `Table.isRowLockable()` 保持 `false` | 返回 `true` 会让 H2 调 `lockRow`，默认实现直接抛 `UnsupportedException` | `Table.java:513` |
| `Table.isDeterministic()` 返回 `false` | H2 不缓存该表参与的结果集，每次重算（代价，不是错误） | `Query.java:474` |
| `getMaxDataModificationId()` 随每次写入严格递增 | H2 会命中过期的缓存结果，读到旧行 | `Select.java:1706` |
| `addIndex` 自己把已有行回填进索引 | `ALTER TABLE ADD PRIMARY KEY` 之后，经该索引的点查查不到任何行 | `MVTable.java:368` |
| `updateRows` 按「先前像、后新像」成对读取 `LocalResult` | UPDATE 写到错误的行 | `Table.java:525` |
| 唯一约束失败时自己撤回已写入的行与索引项 | 无事务，异常后脏行留在 delta，计数与内容都不对 | `ConstraintUnique.java:119` |
| 不得就地改写已经交给 H2 的 `Value[]` | 游标返回的 `Row` 直接引用该数组，改写会篡改 H2 正在使用的行 | `Cursor.get()` |

`addIndex` 在 DDL 带 `PRIMARY KEY` 或 `UNIQUE` 时会被调用。本项目用它建 rowId 点查索引，支撑 UPDATE 和 DELETE。

索引不进元数据：重开数据库时 H2 只重放 `CREATE TABLE ... ENGINE` 与 `ALTER TABLE ADD CONSTRAINT`，后者会让 `addIndex` 在空表上重新执行，所以持久化场景下回填实际是空操作。

`Index.needRebuild()` 的返回值对外部引擎表没有行为影响：H2 只在 `MVTable.addIndex` 里读它，那段代码走不到。不要指望它触发重建。

### 无事务的三个坑

1. 失败的部分更新不会回滚。默认 `updateRows` 在异常时调 `session.rollbackTo(rollback)`，而 `transaction` 为 null 时它什么都不做。所以 `addRow` 与 `updateRows` 在索引报错时必须自己把存储与索引两项都退回去。
2. 并发写要自己保。`addRow` 和 `removeRow` 没有锁契约，多个连接会并发进入，delta 内部必须自己加锁。
3. `SELECT ... FOR UPDATE` 依赖 `isRowLockable()`，保持 `false` 就不会走到锁逻辑，语句能正常执行但不加锁。

## 已知天花板

行式求值无法绕过。列向量在 `Cursor.get()` 处被拆成 `Row`，也就是 `Value[]`，每行每列一次装箱。

可以缓解的两点：

- 用 `SET LAZY_QUERY_EXECUTION TRUE` 走流式结果，避免 `LocalResult` 全量物化（`result/LocalResult.java:67` 是 `ArrayList<Value[]>`）。
- 只物化被引用的列，其余列留在向量里。

## 模块结构

```
src/main/java/com/hyd/h2omap/
├── engine/        接入 H2：TableEngine 与 WITH 参数
├── table/         OmapTable，继承 TableBase
├── index/         扫描索引、rowId 索引、游标
├── storage/       列段、delta、rowId 编码
├── vector/        列向量与 null 位图
└── stats/         zone map
```

`src/test/java/com/hyd/h2omap/OmapTableContractTest.java` 是 H2 接入契约的集成测试。

## 当前状态与后续阶段

M0（接入契约验证）已完成：22 个契约用例通过，生产代码改动见 git 历史。后续每个阶段都以可运行的集成测试作为结束条件。

| 阶段 | 目标 |
| --- | --- |
| M1 段真正产生 | 段号与列表下标解耦；`append` 累积到 `segmentSize` 冻结成 `ColumnSegment`；读路径先段后 delta |
| M2 剪枝与流式扫描 | `OmapScanIndex.find` 用 zone map 剪枝；扫描按段惰性推进 |
| M3 合并与并发 | `compact()`；后台合并线程；段集合快照与引用计数 |
| M4 索引 | `OmapRowIdIndex` 区间查询；唯一约束；`addIndex` 回填的规模验证 |
| M5 编码与性能 | Varchar 字典编码、列裁剪物化、与 MVStore 表的量化对比 |
| M6 判决点 | 给出「行式物化开销占总耗时比例」的实测结论 |

已确认的设计决策：

- 合并时的 rowId 稳定性采用「读游标固定段集合快照、合并发布新快照、旧快照引用计数归零后回收」。
- 性能数字只在 M5 交付，M0—M4 只要求集成测试与内部计数器断言。

## 尚未实现

- `ColumnarStore.compact()`：把 delta 合并成新列段，目前是空操作。
- `ColumnarStore` 的段列表从未被填充，`ColumnSegment.build` 没有调用方，所有数据停留在 delta。
- `OmapScanIndex.find()`：zone map 剪枝，现在是全量 rowId 扫描。
- `OmapRowIdIndex.find()`：区间查询，现在只支持精确匹配，其余退化成全表扫描。
- 低基数列的字典编码。
- 并发写的合并线程。
- `RowId` 的段号与定位方式不一致：`ColumnarStore.nextSegmentId()` 的意图是单调递增编号（该方法目前无调用方），而 `segment(int segmentId)` 直接把它当成 `segments.get(segmentId)` 的下标。只有段从不被删除时两者才一致，`compact()` 一旦丢弃旧段就会错位。M1 必须先定下编号语义。

## 待核对

基线是 `2.5.252`，`Table`、`Index`、`Cursor` 的签名已逐条用 `javap` 核对。剩下两点：

- `CreateTableData` 位于 `org.h2.command.ddl`，却被 `org.h2.api.TableEngine` 的签名引用。这是事实上的公开接口，H2 升级时需要复核。
- `OmapTableEngine` 目前只解析 `WITH` 参数，不解析表名里的 schema 前缀。

## 参考

- H2 架构文档：`h2/src/docsrc/html/architecture.html`（第 7 到 8 层已过时，PageStore 已删除）
- MVStore 设计：`h2/src/docsrc/html/mvstore.html`
- 外部表引擎样例：`h2/src/test/org/h2/test/db/TestTableEngines.java`

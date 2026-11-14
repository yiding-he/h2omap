# OMAP（On Memory Analyze Processing）

基于 H2 的纯内存列式存储。走 JDBC，不做事务，面向 OLAP，同时要接住高频细粒度变更。

本目录是独立 Maven 模块，通过 `org.h2.api.TableEngine` 接入 H2，**不修改 H2 源码**。

## 需求与结论

| 需求 | 结论 | 谁来做 |
| --- | --- | --- |
| 纯内存 | H2 已支持 | 本项目只管理自己的内存结构 |
| JDBC 兼容 | 完全复用 H2 | `org.h2.jdbc` 直接可用，不用重写 |
| 列式存储 | 需要实现 | 本项目 |
| OLAP 扫描 | 需要实现 | 列段、zone map、迟物化 |
| 高频细粒度变更 | 需要实现 | 行式 delta 缓冲加定期合并 |
| 事务 | 不支持 | 见「无事务的三个坑」 |

一句话：H2 提供 SQL 前端、JDBC 和元数据，本项目只替换表的存储与索引。

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
| 全局默认引擎 | `engine/DbSettings.java:261`，`DEFAULT_TABLE_ENGINE` |
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
| 不会被加锁 | `table/Table.java:1423` 的 `isRowLockable()` 默认返回 `false`，只有 `mvstore/db/MVTable.java:654` 覆盖成 `true` |
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

### 需要特别处理的三个

| 方法 | 处理方式 | 原因 |
| --- | --- | --- |
| `Table.updateRows` | 覆盖 | 默认实现会把 UPDATE 结果全量物化进 `LocalResult`，再做两趟遍历（`table/Table.java:525`） |
| `Table.isRowLockable` | 保持 `false` | 返回 `true` 会让 H2 调 `lockRow`，默认实现直接抛 `UnsupportedException` |
| `Table.isDeterministic` | 返回 `false` | 有 delta 时扫描结果随写入变化 |

`addIndex` 在 DDL 带 `PRIMARY KEY` 或 `UNIQUE` 时会被调用。本项目用它建 rowId 点查索引，支撑 UPDATE 和 DELETE。

### 无事务的三个坑

1. 失败的部分更新不会回滚。默认 `updateRows` 在异常时调 `session.rollbackTo(rollback)`，而 `transaction` 为 null 时它什么都不做。
2. 并发写要自己保。`addRow` 和 `removeRow` 没有锁契约，多个连接会并发进入，delta 内部必须自己加锁。
3. `SELECT ... FOR UPDATE` 依赖 `isRowLockable()`，保持 `false` 就不会走到锁逻辑。

## 已知天花板

行式求值无法绕过。列向量在 `Cursor.get()` 处被拆成 `Row`，也就是 `Value[]`，每行每列一次装箱。

可以缓解的两点：

- 用 `SET LAZY_QUERY_EXECUTION TRUE` 走流式结果，避免 `LocalResult` 全量物化（`result/LocalResult.java:67` 是 `ArrayList<Value[]>`）。
- 只物化被引用的列，其余列留在向量里。

## 项目结构

```
omap/
├── pom.xml
├── README.md
└── src/main/java/com/hyd/h2omap/
    ├── engine/        接入 H2：TableEngine 与 WITH 参数
    ├── table/         OmapTable，继承 TableBase
    ├── index/         扫描索引、rowId 索引、游标
    ├── storage/       列段、delta、rowId 编码
    ├── vector/        列向量与 null 位图
    └── stats/         zone map
```

`src/main/java` 下只有骨架，方法体和 TODO 标出了待实现的位置。测试代码暂不提供。

## 构建

```
cd omap
mvn -q package
```

依赖 `com.h2database:h2`。默认指向 Maven Central 上的 `2.5.252`。

本地 `h2/` 目录里的源码是 `2.5.259-SNAPSHOT`，骨架的签名按这份源码写。如果要对着本地源码开发，先在 `h2/` 下执行 `mvn install`，再把 `omap/pom.xml` 里的 `h2.version` 改成 `2.5.259-SNAPSHOT`。

## 用法

```
-- 单表指定引擎
CREATE TABLE EVENTS(TS TIMESTAMP, HOST VARCHAR, COST DOUBLE)
  ENGINE 'com.hyd.h2omap.engine.OmapTableEngine'
  WITH 'segmentSize=65536'
```

```
-- 或者设成默认引擎
SET DEFAULT_TABLE_ENGINE 'com.hyd.h2omap.engine.OmapTableEngine'
```

注意：使用 `ENGINE` 需要管理员权限（`command/ddl/CreateTable.java:76-78`）。

表定义会随元数据持久化，数据不会。纯内存场景下这是预期行为，重启后需要重新导入。

## 待核对

骨架的签名按本地 `h2/` 源码（`2.5.259-SNAPSHOT`）写。改用 Maven Central 的 `2.5.252` 时，编译前先核对下面几处：

- `Table.updateRows(SessionLocal, LocalResult, Runnable)` 这个重载是否存在。不存在时改为覆盖 `updateRow(SessionLocal, Row, Row)`。
- `Index.getCost(SessionLocal, int[], TableFilter[], int, SortOrder, AllColumnsForPlan, boolean)` 的参数个数。
- `CreateTableData` 位于 `org.h2.command.ddl`，却被 `org.h2.api.TableEngine` 的签名引用。这是事实上的公开接口，H2 升级时需要复核。
- `OmapTableEngine` 目前只解析 `WITH` 参数，不解析表名里的 schema 前缀。

## 尚未实现

骨架里能跑的部分只到“结构就位”，下面几块是 TODO：

- `ColumnarStore.compact()`：把 delta 合并成新列段，目前是空操作。
- `OmapScanIndex.find()`：zone map 剪枝，现在是全量 rowId 扫描。
- `OmapRowIdIndex.find()`：区间查询，现在只支持精确匹配，其余退化成全表扫描。
- `addIndex`：已有数据回填索引。
- 低基数列的字典编码。
- 并发写的合并线程。

## 参考

- H2 架构文档：`h2/src/docsrc/html/architecture.html`（第 7 到 8 层已过时，PageStore 已删除）
- MVStore 设计：`h2/src/docsrc/html/mvstore.html`
- 外部表引擎样例：`h2/src/test/org/h2/test/db/TestTableEngines.java`

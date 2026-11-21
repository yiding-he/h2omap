# AGENTS.md — h2omap 工程说明

本文件面向在本仓库工作的人与代理，记录设计与实现层面的约定、依据和当前状态。使用方式见 `README.md`。

## 项目定位

OMAP 是 H2 的外部表引擎（`org.h2.api.TableEngine`）。H2 提供 SQL 前端、JDBC、优化器、元数据；本项目替换表的存储与索引，并局部替换 6 个 H2 类以支撑向量化下推与默认引擎，见「对 H2 的局部替换与打包」。

| 需求 | 结论 | 谁来做 |
| --- | --- | --- |
| 纯内存 | H2 已支持 | 本项目只管理自己的内存结构 |
| JDBC 兼容 | 完全复用 H2 | `org.h2.jdbc` 直接可用，不用重写 |
| 列式存储 | 需要实现 | 本项目 |
| OLAP 扫描 | 需要实现 | 列段、zone map、迟物化 |
| 高频细粒度变更 | 需要实现 | 行式 delta 缓冲加定期合并 |
| 向量化扫描下推 | 需要改动 H2 | 替换后的 `Select` 在 `queryWithoutCache()` 里调用 `vectorized/` 的两个入口 |
| 默认引擎 | 需要改动 H2 | 替换后的 `Schema.createTable()` 把普通用户表交给本引擎 |
| 事务 | 不支持 | 见「无事务的三个坑」 |

## 工作约定

| 项 | 约定 |
| --- | --- |
| 构建 | `mvn -B -q package`；`maven-shade-plugin` 产出单 Jar `target/h2omap-0.1.0-SNAPSHOT.jar`，其中已排除被替换的 H2 同名类 |
| 测试 | `mvn -B test`；一次性运行 Gradle 时加 `--no-daemon`（本仓库用 Maven） |
| 性能测试 | 不对原版 H2（`ENGINE "org.h2.mvstore.db.Store"`）做耗时测量。H2 自身的执行缺陷（关联子查询不做去关联化、派生表被编译成参数化视图等）会让这类测量跑到小时级甚至无法结束，得不出可用数据。MVStore 只用作**结果正确性**的对照：契约与规模用例里的 `T_MV` 表用于逐行比对，不记录耗时；需要性能数字时只测 OMAP 侧，并与历史 OMAP 数字对照 |
| JDK | 构建与运行用 JDK 21.0.11；字节码目标 `maven.compiler.release=17` |
| H2 版本 | 固定 `2.5.252`，不跟进 `2.5.259-SNAPSHOT`（`canGetRowCount` 等签名仍在变）。被替换的 3 个 H2 类按该版本源码合入，升级时必须逐类重新比对 |
| 依赖 | 运行期只有 `com.h2database:h2`，测试用 `junit-jupiter`，构建期插件 `maven-shade-plugin` |
| 判定依据 | `src/test/java/com/hyd/h2omap/OmapTableContractTest.java`（78 个用例）、`src/test/java/com/hyd/h2omap/storage/ColumnarStoreTest.java`（24 个用例）、`src/test/java/com/hyd/h2omap/storage/SegmentFileFormatTest.java`（5 个用例）、`src/test/java/com/hyd/h2omap/persistence/ValueCodecTest.java`（3 个用例）、`src/test/java/com/hyd/h2omap/vector/ValueVectorTest.java`（3 个用例）与 `src/test/java/com/hyd/h2omap/bench/DataScaleTest.java`（3 个用例），以及 `src/test/java/com/hyd/h2omap/bench/DataScaleLargeTest.java`（1 个 100 万行用例，需 `-Domap.scale=large -DargLine=-Xmx3g` 才运行）。改动表、索引、存储或向量化实现后必须跑通；不得为了让改动通过而放宽、跳过或删除用例 |
| 查 H2 源码 | `.h2/` 目录存放本仓库依赖的 H2 `2.5.252` 源码包内容（`.h2/main/org/h2/...`、`.h2/test/org/h2/...`、`.h2/docsrc/html/...`），已在 `.gitignore` 中排除，属本地参考。重新获取：`https://github.com/h2database/h2database/releases/download/version-2.5.252/h2-2026-09-23.zip`，只取其中的 `h2/src/`。该目录可能不存在（已排除在版本控制之外）。Maven Central 的 `h2-2.5.252-sources.jar` 内容等价，但个别文件与该 zip 相差数行，核对行号时以 zip 为准 |

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
| 全局默认引擎 | `engine/DbSettings.java:261`，`DEFAULT_TABLE_ENGINE`（仍不可用于本引擎，见 README）；本项目的默认引擎由替换后的 `schema/Schema.java` 实现 |
| 没有 JPMS 限制 | H2 `2.5.252` jar 的 `MANIFEST.MF` 只有 `Automatic-Module-Name: com.h2database`，没有 `module-info.java` |
| H2 自己的样板 | `.h2/test/org/h2/test/db/TestTableEngines.java`，`TreeSetTable`(:777) 加 `TreeSetIndex`(:896) 加 `IteratorCursor`(:1025) |

`TreeSetTable` 那个样例约 300 行，就是一个可用的外部内存表，可以当骨架参考。

### 换不掉的三件事

| 限制 | 位置 |
| --- | --- |
| 执行是行式 | `expression/Expression.java:113`，`Value getValue(SessionLocal session)` 一次一行 |
| 事务管理器不可替换 | `engine/SessionLocal.java:77`，`public final class SessionLocal`，`commit`、`rollback` 直接调 `TransactionStore` |
| 元数据仍在 MVStore 里 | `engine/Database.java:328`，打开数据库时无条件 `new Store(...)` |

本项目接受这三条限制。第三条约等于“catalog 在内存 MVStore 里”，占用很小，不处理。

## 对 H2 的局部替换与打包

`TableEngine` 只能替换表的存储与索引，读路径下推与默认引擎都要改动 H2 自身。本分支因此替换了 6 个 H2 类，另加 3 个访问桥；替换类随本项目源码编译，`maven-shade-plugin` 打包时从 `com.h2database:h2` 中排除同名类（含内部类 `$*`），部署只需一个 Jar。

| 替换的类 | 改动 |
| --- | --- |
| `org.h2.command.query.Select` | `queryWithoutCache()` 增加 `vectorized` 标志与两个下推分支：`tryVectorizedGroup()` 排在 `queryGroup()` 之前，`tryVectorizedFlat()` 排在 `queryFlat()` 之前；命中时不再把 `offset` 归零 |
| `org.h2.schema.Schema` | `createTable()` 增加两段判断：显式写 `ENGINE "org.h2.mvstore.db.Store"` 时清空 `data.tableEngine` 直接走 MVStore；`tableEngine == null` 且未设 `defaultTableEngine`、非开库重放（`!database.isStarting()`）、`data.id != 0`、非临时表时，默认取 `com.hyd.h2omap.engine.OmapTableEngine` |
| `org.h2.expression.condition.Comparison` | 只新增一个 `public int getCompareType()`，供 `VectorizedFilter` 读取比较类型 |
| `org.h2.expression.condition.ConditionInQuery` | 按 `2.5.252` 源码逐字合入，末尾新增 `getLeft()`、`isNot()`、`isAll()`、`getCompareType()` 四个访问器，供 `SubqueryFold` 读取 `IN (SELECT)` 谓词的成员 |
| `org.h2.engine.DbSettings` | 按 `2.5.252` 源码逐字合入，加两处：构造器末尾把 `DEFAULT_TABLE_ENGINE` 的展示值写成 `com.hyd.h2omap.engine.OmapTableEngine`（`defaultTableEngine` 字段仍为 `null`，`Schema.createTable` 用它判断默认引擎路径，改的只是 `INFORMATION_SCHEMA.SETTINGS` 读的那张 map；`MV_STORE` 不动，H2 元数据确实在 MVStore 里）；新增 `public final boolean omapLazyLoad = get("OMAP_LAZY_LOAD", false)`，由服务端系统属性 `-Dh2.omapLazyLoad=true` 打开（连接串参数 `;OMAP_LAZY_LOAD=TRUE` 会被 H2 以 `Unsupported connection setting` 拒绝），库打开时读一次。开关打开后 `recover()` 只登记段的 id、generation 与骨架元数据，段内容由加载线程池逐段读入 |
| `org.h2.command.Parser` | 按 `2.5.252` 源码逐字合入，只改 `parseAlterTable()` 的 `SET` 分支：先认 `RESIDENT_BYTES` 关键字就交给 `parseAlterTableSetResidentBytes()`（构造 `com.hyd.h2omap.command.OmapAlterTableSetResidentBytes`），否则保持 `parseAlterTableSet()` 原语义；其余全部逐字保留。`ParserBase` 不替换 |

访问桥不替换原类，只是声明在同一个包里，用来读取包级与受保护成员：

| 访问桥 | 打开的成员 |
| --- | --- |
| `org.h2.expression.H2ExpressionAccess` | `Operation1.arg`、`Operation2.left`、`Operation2.right` |
| `org.h2.expression.aggregate.H2AggregateAccess` | `AbstractAggregate.args` |
| `org.h2.expression.condition.H2ConditionAccess` | `SimplePredicate.left`、`SimplePredicate.not`、`ConditionIn.left`、`ConditionIn.not`、`ConditionIn.valueList`、`ConditionAndOr.getAndOrType()`、`ConditionAndOrN.getAndOrType()`、`PredicateWithSubquery.query`（经 `getExistsQuery`/`getInQueryQuery` 读取 `ExistsPredicate` 与 `ConditionInQuery` 的子查询） |

升级 H2 时必须重做：

1. 按上表逐类重新比对替换点，确认上游没有改名、没有改变控制流、没有挪动 `vectorized` 标志要跳过的语句；`Parser` 只需比对 `parseAlterTable()` 的 SET 分支与新增方法周边，其余逐字合入。
2. `Comparison.getCompareType()` 若上游已公开，删除本项目的 `Comparison.java`，同步 `pom.xml` 的排除清单；`ConditionInQuery` 的四个访问器同理。
3. 复核 `Table`、`Index`、`Cursor` 的抽象方法集合，以及 `CreateTableData` 的构造器与所在包。
4. 复核 `pom.xml` 的 shade 排除清单与替换类一一对应；漏掉内部类会留下同一个类的两个版本。

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
├── segments : TreeMap<段号, SegmentRef>  按段号升序；全删段会被整体摘除，段号不连续
│     每列：primitive 数组、字典向量或数组列（前缀和下标 + 元素列）+ null 位图 + zone map
│     段级：删除位图（每行 1 bit）与已删行数计数（空段判定 O(1)）
└── delta    : DeltaStore               行式写缓冲
      ├── updated        已冻结段内被更新行的新像，存进 DeltaRowset：
      │                     按列向量（复用段向量的类型特化与打包编码），
      │                     位置不可变，每列带自己的 zone map
      ├── segmentDeletes 已冻结段内未回写段位图的删除标记，按段组织成位图（每行 1 bit）
      ├── dirtySegments  有改动的段号，供读路径跳过逐行查表
      └── updatedSegments 有未回写新像的段号，行式剪枝只跳过这些段

openSegment : SegmentBuilder            当前开放段的行缓冲，写满即冻结成段
rowId = 段号 << 32 | 段内偏移
```

rowId 在 `append` 时就确定，之后不变。段号在开放段创建时预先分配（等于 `nextSegmentId`），段内偏移就是写入顺序，所以冻结成段不改变任何已有 rowId；H2 把 rowId 存进 `Row.key` 并在后续语句里当行标识用，这一条是硬要求。段号单调递增、不回收（`truncate` 也不回收）。`segments` 按段号映射而不是按下标：全删段会从中间被整体摘除（见下），段号不再连续，下标定位不再成立。

写入路径全部是 O(1)，不重写列：

| H2 调用 | 本项目实现 |
| --- | --- |
| `addRow` | 追加进开放段，不动已冻结的列段 |
| `removeRow` | 由 `row.getKey()` 反解 rowId，已冻结段记进 `segmentDeletes` 位图，开放段记进 `openDeletes`；随后判定段内已删行数是否达到段行数，达到则整段摘除 |
| `updateRows` | 已冻结段记进 `updated`，开放段整体替换 builder 里的值数组 |
| `truncate` | 清空 segments 和 delta，段号不回收，返回旧行数 |

读取路径：`ColumnarStore.RowIterator.nextBatch()` 在单次读锁内取回一批可见行，`OmapCursor` 每批 512 行消费；`materialize()` 对 `dirtySegments` 之外的段直接按偏移取列值，跳过逐行哈希查找。`OmapScanIndex.find` 把 H2 的 `first`/`last` 边界翻译成每列的上下界，迭代器用 zone map 跳过不相交的段。返回的行对象是 `LazyOmapRow`，只在 H2 调 `getValue(col)` 时解码该列，`getValueList()` 时才补齐整个数组。

字典编码：`ColumnSegment` 建段时对 `VARCHAR` 与低基数数值列尝试建字典，容量上限 `min(4096, max(64, 行数 / 4))`，超出则退回普通向量。字典向量让等值过滤与分组按下标比较，减少 `Value` 装箱。

迭代器是创建时刻的快照：构造时固定最后一个段号与开放段的行数。开放段在迭代期间被写满冻结时，迭代器改从段列表读它，rowId 不变；冻结之后新开的段不在本次快照内。

合并（compaction）：`delta` 的改动数达到 `compactionMinRows` 时由写路径同步触发，分两阶段。第一阶段在写锁内把更新与删除按段归组，活像在此阶段物化成不可变快照（列向量只在锁内可读，锁外的无锁读者可能看到未完整发布的向量），并记下当时的段引用与活像位置；锁外对每个受影响的段调用 `applyDelta` 生成新段（Copy-On-Compact，旧段仍对先前取到的快照有效），新段的 zone map 随建段重算；第二阶段在写锁内替换段映射里的引用、按不可变位置双重比对移除 `delta` 里已写回的条目（防止误删锁外期间的新更新），再重算 `dirtySegments`，死位置超过半数时重排回收 rowset。回写后整段全删的段直接摘除。段号与段内偏移都不变，所以 rowId 稳定；除全删段被摘除外段不删除。

全删段丢弃（Merge-on-Read 批次 1）：段内已标记删除的行数（段位图加 delta 未回写部分）达到段行数时，写路径立即把整段从 `segments` 摘除，rowId 空洞保留不复用，行数在逐行删除时已扣过、不再调整；索引条目在每次删除时由 H2 逐行清理，摘除时无需处理。段文件在下次 checkpoint 随 manifest 摘除后由文件清理删除；恢复时 WAL 重放后由 `dropEmptySegments()` 一次性扫描补摘，惰性加载的段在加载完成时补摘。对按时间滚动的清理服务，这意味着最老的段被删空后零重写回收磁盘。

delta rowset（Merge-on-Read 批次 2）：更新新像不再按行存放（`Map<rowId, Value[]>` 每行一套 `Value` 对象），而是存进 `DeltaRowset`——复用段列向量的类型特化与打包编码，内存开销与段内数据同构；位置不可变（重复更新追加新位置、旧位置标死），合并的锁外构建阶段与双重比对都以位置为锚点；每列带自己的 zone map，向量化快照里新像与查询区间不相交时整组跳过（零物化零求值），相交时才在锁内物化进 `extraRows`。行式路径的剪枝口径不变（有未回写新像的段仍不剪枝）。tail 文件格式不变：checkpoint 仍按 rowId 集合写，恢复时逐条重放进 rowset。

段级部分聚合缓存（Merge-on-Read 批次 3）：`SegmentAggCache` 按「查询签名 × 段」缓存部分聚合结果，无分组时是 `AggState[]`、单列分组时是 `Map<Value, AggState[]>`。签名 = 过滤树语义签名（各谓词节点的列、比较、常量、集合内容）+ 分组键列 + 每个聚合的类型/列/算术口径/结果类型；`COUNT(DISTINCT)`、多列分组与含子查询预计算的谓词不参与。失效坐标是「段的 generation + 该段的 delta 屏蔽位版本」：合并会换 generation，未回写删除与更新新像的每次变化都递增 `DeltaStore` 的段版本；跨段写因此只使被改动的段失效，其余段在下一轮同签名聚合中直接合并缓存的部分结果（40 万行实测 26 ms → 1-2 ms）。合并部分结果时绝不与缓存对象按引用共享：`groups` 里的状态随后还会被 delta/开放段行累加，共享会污染缓存。分组部分结果基数超过 4096 不缓存；签名数量上限 64（访问序淘汰）。

段级逐出（Merge-on-Read 批次 4）：表级 WITH 参数 `residentBytes` 给出冻结段驻留堆的估算字节数预算（纯数字按字节，可跟后缀 k/m/g，不区分大小写，按 1024 换算，如 8g；非法后缀建表即报错），默认 0 不逐出；仅文件模式库生效（内存库无段文件可读回，不逐出）。预算可随时用 `ALTER TABLE [IF EXISTS] t SET RESIDENT_BYTES '40k'` 修改（`=` 可选，值解析与建表参数共用同一 `parseByteSize`；非 OMAP 表、非法后缀、NULL 值报 SQL 错误）：调小立即在写锁内逐出到新预算，调大不重启预载、由后续查询自然填驻留（预算变化不改变数据，H2 结果缓存对重复查询仍直接命中）。修改同步进 H2 目录的建表 SQL，重开库按新值生效：`CreateTableData.tableEngineParams` 是 public 字段、与 `TableBase` 私有字段同一个 `ArrayList` 实例，改写该条目后调 `database.updateMeta` 重写目录行（`MetaRecord` 经 `getCreateSQLForMeta` 重新渲染）；建表时没有 WITH 子句的表该列表不可变，由 `OmapTable` 覆写 `getCreateSQL`/`getCreateSQLForMeta` 在末尾追加 WITH 子句。逐出后段仍可见、只是慢：行数、已删行数、删除位图与每列 zone map 留在堆上的骨架里，行号级读路径（行迭代器、`collectVisibleRowIds`、可见性判定、全删段丢弃）全部走骨架；需要段内容的读路径先把段同步读回——点查在 `materialize` 前、合并第一阶段在归组前；批量行读与向量化快照不预读，改为推进到当前段/首次读列时按需读回（行式批量读复用驻留通道，向量化走临时切片读，见「逐出段的部分列读」）。逐出顺序按段级 LRU：从未被查询访问的段按段号老优先，访问过的按访问序，与数据写入顺序无关，时间滚动场景自然退化为逐出最老段。预算小于查询工作集时批量行读的读回可能被后续读回逐出，退回「读一份仅供本次使用」的临时读、不回写驻留状态；预算连单个段都容不下时（如 residentBytes=1）所有段保持逐出，点查与批量路径退回临时读，向量化路径的逐出段恒走临时切片读、不进驻留模型，行仍完整可见。逐出检查在段冻结的 checkpoint 之后、合并收尾与恢复收尾触发，读回后的段拥有最新访问序号，不会被紧接着的预算逐出先选中。重开库时恢复收尾把驻留裁回预算；骨架元数据随 manifest（v2）持久化，惰性加载与逐出同时开启时，重开后全部段立即可见可剪枝，预载只填预算内的内容（见「惰性加载」一节）。回归防线是「随机散布写压力用例」：随机 UPDATE/DELETE/INSERT 散布全部段，每轮与 MVStore 逐查询比对，并断言无写的第二遍命中段级部分聚合缓存，防止后续机制窄化到「只改最老几段」的滚动场景。

## 向量化执行

替换后的 `Select.queryWithoutCache()` 在两条路径上尝试下推，命中后整个查询由 `OmapVectorizedExecutor` 直接写入 `LocalResult`，未命中则原样走 H2 的 `queryFlat()`、`queryGroup()`。

| 入口 | 触发的形态 | 附加前提 |
| --- | --- | --- |
| `tryExecuteFlat` | 单表投影加 `WHERE`，可带 `LIMIT`、`OFFSET`；`ORDER BY` 单列且 `LIMIT` 为常量时走有序模式 | 投影项是单表列引用或常量；`tableFilter` 用的是扫描索引；有序模式要求排序键是被扫描表的列、`limit + offset` 不超过 10000 |
| `tryExecuteGroup` | 单表聚合，可带 `GROUP BY`、`HAVING` | 分组键是单表列引用；结果列是聚合或分组键；`qualifyIndex < 0` |

共同的拒绝条件：`result` 为 null、`filters.size() != 1`、`FOR UPDATE`、窗口查询、`DISTINCT` 或 `distinctExpressions != null`、`qualifyIndex >= 0`、`WITH TIES`。`VectorizedFilter.compile()` 返回 null 时同样放弃下推。

可下推的谓词：6 种 `Comparison` 比较、`BETWEEN`（`BetweenPredicate` 普通形态折叠成区间比较的 AND、`NOT BETWEEN` 折成两个取反比较的 OR，`SYMMETRIC` 与空边界交回 H2）、`AND`/`OR`（含 `ConditionAndOrN`）、`IN`、`IS NULL`/`IS NOT NULL`、`LIKE`/`NOT LIKE`、`CARDINALITY(...)`、`ARRAY_CONTAINS(数组列, 常量)`（含 `NOT`），以及比较左侧的一元与二元算术（`PLUS`、`MINUS`、`MULTIPLY`）。`ILIKE`、`REGEXP`、`= ANY(...)`/`= ALL(...)`（`ConditionInArray`）等形态交回 H2。

段级剪枝：`VectorizedFilter` 从过滤条件的顶层 `AND` 合取项收集每列上下界（`ColCmpNode` 按比较类型给出，其余节点不给出），`ColumnarStore.takeVectorizedSnapshot(lower, upper, columns)` 据此用 zone map 跳过不相交的段，任何谓词都不在被跳过的段上求值。同一列有多个合取项时只保留首次出现的界（区间取宽不取窄），因此不需要比较两个常量。

裸 MIN/MAX 定界：`tryExecuteGroup` 对无 `WHERE`、无 `GROUP BY`、单聚合且聚合参数为列引用的 `MIN`/`MAX` 走 `ColumnarStore.extremeColumn()`：候选段按目标方向的骨架 zone 边界排序后依次扫描，下一个段的边界已不优于当前最优值即停——时间递增数据通常扫一段即停，逐出段只读目标列，不进驻留模型。zone 边界含已删行，因此不能一次只扫全局边界最优的段（其最优行可能已删，迭代推进在任何删除形态下精确）；delta 新像与开放段的行无条件参与，扫描段内 delta 已删与已更新的旧像不参与。数组列与不可比较类型不定界，交回常规路径。

逐出段的部分列读：向量化快照把「投影列或聚合参数列 ∪ 分组键 ∪ 排序键 ∪ 谓词列」的列集传给 `takeVectorizedSnapshot`，拍快照不读盘——逐出段用堆上骨架构造懒加载段，行数、删除位图与 zone map 零 I/O，执行期首次读列才按段文件尾部列目录触发磁盘读取（`readSegmentColumns` → 临时 `ColumnSlice`），未在请求列集内的列以抛异常的 `AbsentColumnVector` 占位（意外访问显式暴露，不静默返回空值），不进驻留模型、不触发逐出，预算稳态不被冷查询搅动，被 `canSkipSegment` 跳过或聚合缓存命中的段零读盘。无 `ORDER BY` 的 `LIMIT` 先扫驻留段与开放段/新像，驻留可见行数不足才按需读回逐出段，返回行序不再隐含段号升序（无 `ORDER BY` 时 H2 本就不保证行序）；行式路径（如 DISTINCT）经驻留通道推进到当前段时逐段读回，读回段进 LRU 由预算管理，与切片读形成对照。

有序模式：`ORDER BY` 单列加常量 `LIMIT` 时，段按排序列在 zone map 上的边界排序后依次扫描，用容量为 `limit + offset` 的堆留下前 N 行；堆已满且某段内最好的一行也不优于第 N 名时整段跳过。物理聚簇与排序键同向时只需扫一两段；同向不成立时结果仍正确，只是跳过不了段。多列 `ORDER BY`、排序键为表达式、`LIMIT` 缺失或超过 10000 时交回 H2。

子查询下推（`SubqueryFold`，由 `VectorizedFilter.compile` 末尾与 `compileComparison` 调用）：

| 形态 | 执行方式 |
| --- | --- |
| 不相关标量子查询在比较一侧 | 编译期求值一次当常量用（M10 第一步，已先行提交） |
| 不相关 `IN (SELECT)` | 子查询一次求值收进集合；整数列用原始 `long` 开放寻址集合，其余用 `Value` 哈希集合；空集折成恒假/恒真，子查询含 NULL 且 `NOT IN` 折成恒假 |
| 不相关 `EXISTS`/`NOT EXISTS` | 编译期求值一次折成恒真/恒假 |
| 关联 `EXISTS`/`NOT EXISTS`（单列等值关联） | 对内层表预计算关联键集合，折成半连接节点；键按外层键列类型装箱后哈希查找 |
| 关联标量聚合子查询（单列等值关联、内层单聚合无分组） | 对内层表 `takeVectorizedSnapshot` 预计算一次分组聚合，外层按关联键查表比较；空组取 `COUNT` 为 0、其余聚合为 NULL 的空值语义 |
| 关联 `IN`/`NOT IN (SELECT)`（单列等值关联、内层投影为列引用） | 预计算「关联键 → 内层值集合」，外层按键做成员判定；整数族值列用原始 `long` 集合（按元素数扩容），字典键列按码预解析集合引用；`NOT IN` 按 SQL 三值语义，UNKNOWN 不命中 |

内层接受的形态：单表 OMAP 查询、无 `ORDER BY`/`LIMIT`/`OFFSET`/窗口、恰好一个等值关联、其余合取项只引用内层表且可被 `VectorizedFilter` 编译；聚合限 `COUNT(*)`/`COUNT`/`SUM`/`MIN`/`MAX`/`AVG` 且 `SUM`/`AVG` 参数为整数列。不识别的形态一律交回 H2 行式路径。

可下推的聚合：`COUNT(*)`、`COUNT(...)`、`COUNT(DISTINCT ...)`、`SUM`、`MIN`、`MAX`、`AVG`，以及形如 `SUM(col * 2)` 的「单列与常量做算术再聚合」。单列分组在字典向量上走 `groupByDict()`，多列分组走 `MultiGroupKey`，`HAVING` 编译成谓词在分组结果上求值。

执行基于 `ColumnarStore.takeVectorizedSnapshot()`：已冻结段按列整体扫描，删除位图折进每段的 `BitSet`，`delta` 的新像与开放段的行放进 `extraRows` 逐行求值；新像整组由 delta rowset 的 zone map 判定，与查询区间不相交时整组跳过、零物化零求值。因此向量化只作用于「已冻结段按列扫描」这一段，`extraRows` 仍是行式路径。

## 持久化

文件模式库默认开启，目录为 `<databasePath>.omap/<schema>_<table>/`。`backup=false` 关闭；`backup=true` 在内存库上必须同时给 `backupDir`；`syncIntervalMs` 默认 200 毫秒。

```
manifest.dat                  段清单：nextSegmentId、walSeq、每段的 id 与 generation，以及每段骨架元数据（行数、已删行数、删除位图、每列 zone map）
seg_<id>_<gen>.omap           列段快照：头部（魔数 OSG2/段号/代数/行数/已删行数/目录偏移）→ 删除位图 → 列块（沿用 VEC_* 编码，顺序存放）→ 尾部列目录（每列偏移、长度、zone map min/max）
tail_<walSeq>.omap            检查点时刻的开放段行、openDeletes 与 delta
wal_<seq>.log                 检查点之后的增量：insert/update/delete/truncate
```

manifest 与段文件都只有 v2 一种格式，旧 v1 回退分支已删除：旧格式文件报非法魔数，不迁移。全量读经尾部目录恢复 zone map 与已删行数，不再全列重算；整段一次大块读入后列块与目录都在内存缓冲上解码——原始数组用 `ByteBuffer` 视图一次批量拷贝（消掉 `DataInputStream` 逐元素的同步单字节拼接），打包文本按列块长度恒等式「剩余字节 - rows*4」一次精确分配零扩容，目录偏移与列块长度做位置校验；`OmapPersistence.readSegmentColumns` 尾部目录一次块读，请求列在文件上首尾相接时合并成单次大块连续读，返回 `ColumnSlice`（列向量 + zone map + 行数 + 删除位图），切片不注册段引用、不计入驻留预算、不触发 LRU 触碰，只服务向量化快照与临时读路径。

`append`、`remove`、`update`、`truncate` 在存储写锁外追加一条 WAL 记录，`syncExecutor` 按 `syncIntervalMs` 对通道执行 `force`。开放段写满冻结后立即 `checkpoint()`；`compact()` 的改动数达到 `max(1024, compactionMinRows)` 时也 `checkpoint()`。`checkpoint()` 在写锁内取段清单、开放段行、`openDeletes` 与 `delta` 的快照并 `rotateWal()`，锁外写段文件与 `tail`，最后写 `manifest.dat` 并清理过期文件。

恢复顺序：读 `manifest.dat` 与其中的段文件，读 `tail_<manifestWalSeq>.omap`，按序号重放 `wal_<manifestWalSeq>` 及其后的全部 WAL，最后以 `maxSeq + 1` 打开新 WAL。空目录走 `initEmptyWal()`，只开 `wal_1.log`。

`OmapTable.close()` 关掉 persistence，`removeChildrenAndResources()` 删除整个目录。索引不在 WAL 里：重启后 H2 重放 `ALTER TABLE ADD CONSTRAINT` 会重新调用 `addIndex`。

### 惰性加载

由服务端启动参数 `-Dh2.omapLazyLoad=true` 打开，默认关闭；开关关闭时 `recover()` 在开库时读完所有段（M17 起经加载线程池并行读回），行为与惰性加载引入前一致。

开关打开时，`recover()` 只读 `manifest.dat` 登记每段的 id、generation 与骨架元数据，段文件不读，随后 `ColumnarStore.startLazyLoading()` 在 `OmapPersistence.LOAD_CONCURRENCY`（固定 4，代码常量、无对外参数）的共享加载线程池上按段号**倒序**加载：4 个工人线程共享一个递减游标，段号与写入顺序同向，最近写入的数据最先驻留；非惰性恢复同样经该线程池并行读回全部段，任一段读失败按段序号最小者抛出。实测 800 个段 4 万行：开库 328 → 198 ms。

可见性口径：骨架随 manifest 恢复后，**未加载段对全部读路径整体可见且可剪枝**——行数（骨架行数减已删行数）、删除位图与每列 zone map 都在堆上，开库第一次 `COUNT(*)` 就是全量；需要段内容的读路径（点查 `materialize`、批量 `nextBatch`、向量化快照、合并归组）按需读回。无持久化的内存库没有 manifest，未加载段整体不参与，`publishLoaded` 发布时才计入行数与递增 modificationId。预载与驻留预算（`residentBytes`）同时开启时，预载在两个条件下停止：取段前驻留量已达预算即停；本次发布触发了任何预算逐出即停（并发下预算边界最多多读「并发数 - 1」个装不下的段，此后零无效读；终驻留集与顺序预载一致，逐出永远选已加载段中 id 最小者）。全表聚合会把驻留集滚动到最近触碰的段，目标查询只读回骨架 zone map 相交的段。

三处配套规则：

| 规则 | 原因 |
| --- | --- |
| 有骨架段的发布（`publishLoaded`）只挂内容，不递增 modificationId、不动行数 | 骨架已在恢复时计入行数且保证可见，内容加载不改变任何查询结果；无骨架段（无持久化的内存库）发布时仍递增 modificationId 并经 `visibilityListener` 调 `OmapTable.setModified()`，否则 H2 结果缓存返回加载前旧结果 |
| `isVisible` 对未加载段的行返回 `true`，`countedInRowCount` 对这些行返回 `false` | WAL 重放与客户端删除必须能记进 delta，但无骨架段的行在加载前尚未计入 `rowCount`，不能重复扣减；有骨架段两者都直接看骨架 |
| `checkpoint()` 只重写 generation 变了的段，未加载段由 `loadedSegment()` 返回 `null` 时跳过 | gen 只在合并时递增，而合并要求段已加载，所以未加载段无需写盘 |

合并（`compact()`）在加载期间只处理已加载段，指向未加载段的 delta 条目留在 delta 里，等该段加载后再合并；这些改动在有骨架段上始终参与可见性。

`addIndex` 回填：有骨架段对回填可见，回填会把全部段按需读回（索引唯一性要求覆盖每一行，属必要读而非无效读；预算开启时伴随逐出翻搅）；无骨架段（无持久化的内存库）回填只覆盖已加载段，唯一约束在加载完成前不完整。生产表没有主键与唯一约束（`INFORMATION_SCHEMA.INDEXES` 为 0 行），不触发。

## 与 H2 的契约

### 必须实现的方法

`Table` 有 15 个抽象方法：`close`、`addIndex`、`removeRow`、`truncate`、`addRow`、`checkSupportAlter`、`getTableType`、`getScanIndex`、`getIndexes`、`getMaxDataModificationId`、`isDeterministic`、`canGetRowCount`、`canDrop`、`getRowCount`、`getRowCountApproximation`。

`Index` 有 10 个：`close`、`add`、`remove(SessionLocal, Row)`（删一行）、`find`、`getCost`、`remove(SessionLocal)`（删整个索引）、`truncate`、`needRebuild`、`getRowCount`、`getRowCountApproximation`。

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
| 迭代器的单行 `next()` 与批量 `nextBatch()` 必须用同一套可见性判定（含 `delta` 的段内删除） | 两者不一致时 `collectVisibleRowIds` 会多出已删行，索引回填时数组越界，H2 把异常写进 `.trace.db` 后继续，主键索引静默丢失，重复主键不再被拒 | `ColumnarStore.SegmentRowIterator` |

`addIndex` 在 DDL 带 `PRIMARY KEY` 或 `UNIQUE` 时会被调用。本项目用它建 rowId 点查索引，支撑 UPDATE 和 DELETE。

`OmapRowIdIndex.find()` 的区间语义：`first`/`last` 是 `SearchRow`，只填了索引列，其余列为 null（null 元素表示该侧无界）；H2 在倒序时已经把两者交换（`IndexCursor.find()`），所以实现里先取正序区间，再按需 `descendingMap()`。等值查询是区间的退化形式，与区间共用同一条路径。索引条目用 `ConcurrentSkipListMap` 维护，因为 `find` 在存储锁之外迭代，而写路径并发 `put`/`remove`，`TreeMap` 会直接产生 `ConcurrentModificationException`（已由并发用例实证）。

索引不进元数据：重开数据库时 H2 只重放 `CREATE TABLE ... ENGINE` 与 `ALTER TABLE ADD CONSTRAINT`，后者会让 `addIndex` 在空表上重新执行，所以持久化场景下回填实际是空操作。

`Index.needRebuild()` 的返回值对外部引擎表没有行为影响：H2 只在 `MVTable.addIndex` 里读它，那段代码走不到。不要指望它触发重建。

### 无事务的三个坑

1. 失败的部分更新不会回滚。默认 `updateRows` 在异常时调 `session.rollbackTo(rollback)`，而 `transaction` 为 null 时它什么都不做。所以 `addRow` 与 `updateRows` 在索引报错时必须自己把存储与索引两项都退回去。
2. 并发写要自己保。`addRow` 和 `removeRow` 没有锁契约，多个连接会并发进入，delta 内部必须自己加锁。
3. `SELECT ... FOR UPDATE` 依赖 `isRowLockable()`，保持 `false` 就不会走到锁逻辑，语句能正常执行但不加锁。

## 已知天花板

行式求值无法绕过。标量路径的列向量在 `Cursor.get()` 处被拆成 `Row`，也就是 `Value[]`，每行每列一次装箱。

已经做的两点缓解：

- 列裁剪与 `LazyOmapRow`：只解码 H2 实际读到的列。
- 向量化下推：单表过滤与聚合按段整列执行，不再逐行拆 `Value[]`；`delta` 新像与开放段的行仍逐行求值。

`SET LAZY_QUERY_EXECUTION TRUE` 走流式结果可以避免 `LocalResult` 全量物化（`.h2/main/org/h2/result/LocalResult.java:67` 是 `ArrayList<Value[]>`）。本项目的代码路径不依赖它，测试里也没有启用。

## 模块结构

```
src/main/java/com/hyd/h2omap/
├── command/       ALTER TABLE SET RESIDENT_BYTES 命令
├── engine/        接入 H2：TableEngine 与 WITH 参数
├── table/         OmapTable，继承 TableBase
├── index/         扫描索引、rowId 索引、游标
├── storage/       列段、开放段缓冲、delta、rowId 编码、延迟行、部分列读的列切片与占位列向量
├── persistence/   WAL 增量日志、增量列段快照与重启恢复
├── vector/        列向量（含字典向量、数组列与 CLOB 打包）、ValueVector 回退、null 位图
├── vectorized/    单表过滤与聚合下推、谓词编译、子查询折叠（SubqueryFold）、段级部分聚合缓存（SegmentAggCache）
└── stats/         zone map

src/main/java/org/h2/                     被替换的 H2 类与访问桥
├── command/Parser.java                       替换：ALTER TABLE SET RESIDENT_BYTES 语法入口
├── command/query/Select.java                 替换：向量化下推入口
├── engine/DbSettings.java                     替换：默认引擎展示值与 omapLazyLoad 开关
├── schema/Schema.java                        替换：默认引擎与 MVStore 回退
├── expression/condition/Comparison.java      替换：公开 getCompareType()
├── expression/condition/ConditionInQuery.java 替换：公开四个访问器，供子查询折叠
└── expression/**/H2*Access.java              访问桥：读取包级与受保护成员
```

```
src/test/java/com/hyd/h2omap/
├── OmapTableContractTest.java    H2 接入契约
├── storage/ColumnarStoreTest.java 存储层
├── storage/SegmentFileFormatTest.java 段文件 v2 格式与部分列读
├── vector/ValueVectorTest.java   回退向量
├── persistence/ValueCodecTest.java 段向量编码往返
└── bench/                        测试数据生成与规模验证
```

`bench/DataGenerator.java` 是固定种子（20260923）的数据生成器，同时建 OMAP 表 `T` 与 MVStore 对比表 `T_MV`，两表数据完全一致。表结构 10 列：`ID` 主键、`LOW_CARD`（10 取值）、`HIGH_CARD`、`INT_COL`（部分连续部分随机）、`DOUBLE_COL`、`TS_COL`（随机）、`NULLABLE_COL`（约 5% null）、`ARRAY_COL`、`STR_ARRAY_COL`、`SORT_TS`（递增，与插入顺序同向，用于有序扫描与段级剪枝）。

命令行用法：`java -cp "target/h2omap-0.1.0-SNAPSHOT.jar:<h2 jar>:target/test-classes" com.hyd.h2omap.bench.DataGenerator [rows] [url] [omap|mv]`，默认 100 万行、内存库 `omapbench`、OMAP 引擎。第三参数选 `omap` 或 `mv`，两种引擎分开跑，每次只建一张表 `T`；命令行走 `benchSetup`（先插满预热，再 `TRUNCATE` 清空重插，最后跑查询）。`mv` 只用于核对结果与观察数据形状，不用于耗时测量（见「工作约定」的性能测试一条）。`DataScaleTest` 用 1 万行加 `segmentSize=1000`、`compactionMinRows=1000` 跑默认测试，同时建两表做签名对比；`DataScaleLargeTest` 用 100 万行，需 `-Domap.scale=large -DargLine=-Xmx3g` 才运行。

`bench/ConcurrentQueryBench.java` 用于多线程并发查询基准（`[threads] [rows] [rounds] [withWriter] [engine] [url] [writerRatePerSec]`），支持进程内直连与独立 TCP 服务端进程两种模式。它按行数构造 5 条固定查询：`q1`–`q4` 为 `COUNT`/`SUM` 聚合（含关联标量子查询），`q5` 为 `SORT_TS` 上 `ORDER BY ... DESC LIMIT 100` 的有序扫描；开写线程时 `compactionMinRows` 取 `1`，每轮 `UPDATE`/`DELETE` 各触发一次段合并：

- 一键复现「最终性能测试」（独立 TCP 服务端 + 500 万行 + 仅 OMAP + 16 读 + 1 写 + 5 条查询 × 50 轮共 4,000 次查询，自动起停服务端、定位 H2 Jar、结束清理）：Linux/macOS 用 `./scripts/final-benchmark.sh [writer_rps] [--persist] [--keep-data]`（`--persist` 改用文件数据库并启用 OMAP 持久化、结束时重启校验行数；端口与堆用 `OMAP_BENCH_PORT`/`OMAP_SERVER_XMX`/`OMAP_CLIENT_XMX`，数据目录用 `OMAP_BENCH_DATA_DIR`），Windows 用 `./scripts/final-benchmark.ps1`（`-WriterRps 50` 为写限速，`-Persist` 同上，`-Rows`/`-Readers`/`-Rounds`/`-DataDir`/`-KeepData`/`-ServerXmx`/`-ClientXmx`/`-SkipBuild` 可调）。两种脚本等价，均为一次调用跑一种写负载（实测结果记录于 `BENCHMARK_RESULT.md`）。
- 独立服务端启动（100 万行用 `-Xmx3g`，1000 万行用 `-Xmx8g`）：`java -Xmx8g -cp target/h2omap-0.1.0-SNAPSHOT.jar org.h2.tools.Server -tcp -tcpPort 19092 -ifNotExists`。
- 外部纯 H2 客户端压测（16 读 + 1 写 + 100 万行，客户端无需引入 `h2omap` Jar）：`java -Xmx1g -cp "$HOME/.m2/repository/com/h2database/h2/2.5.252/h2-2.5.252.jar:target/test-classes" com.hyd.h2omap.bench.ConcurrentQueryBench 16 1000000 5 true omap "jdbc:h2:tcp://localhost:19092/mem:omapconcurrent;DB_CLOSE_DELAY=-1;QUERY_CACHE_SIZE=0"`。
- 外部纯 H2 客户端压测（16 读 + 1 写 + 500 万行，仅 OMAP，5 条查询 × 50 轮共 4,000 次查询）：`java -Xmx2g -cp "$HOME/.m2/repository/com/h2database/h2/2.5.252/h2-2.5.252.jar:target/test-classes" com.hyd.h2omap.bench.ConcurrentQueryBench 16 5000000 50 true omap "jdbc:h2:tcp://localhost:19092/mem:omap5m;DB_CLOSE_DELAY=-1;QUERY_CACHE_SIZE=0"`。

### residentBytes 驻留预算测试环境（本机）

一键重建：`./scripts/resident-env.sh`。数据目录默认 `/Volumes/Yotta/Temp/h2omap`（本机 SSD APFS 卷），可用 `OMAP_RESIDENT_DATA_DIR`、`OMAP_RESIDENT_ROWS`（默认 900 万）、`OMAP_RESIDENT_BUDGET`（默认 `100m`）、`OMAP_RESIDENT_XMX`（默认 `8g`）覆盖。流程：生成 900 万行 10 列表 `T`（文件库 `benchdb`，磁盘约 1 GB、139 个段）→ `ALTER TABLE T SET RESIDENT_BYTES '100m'` 设 100 MiB 驻留预算 → 校验行数与 H2 目录里的 `WITH "residentBytes=104857600"`。重建耗时约 5 分钟（含 `benchSetup` 的双插与收尾查询）；脚本重置时会删除 `benchdb*` 全部文件（含 `.trace.db`）。

实测口径（Apple M4、900 万行）：开库 7–10 s，瓶颈不是磁盘——1 GB 段文件由 4 线程并行读只占零点几秒，大头是 `ID BIGINT PRIMARY KEY` 的 rowId 索引回填（900 万次 `ConcurrentSkipListMap.put` 串行，约 1 µs/行）；`COUNT(*)` 走骨架零段读（≤5 ms）；冷 `SUM(INT_COL)` 按列目录只读约 36 MB 列切片（13–19 ms）；二次 `SUM` 命中段级聚合缓存（0 ms）；开库后驻留 100,382,915 B ≤ 预算，逐出后 8 项聚合签名与逐出前一致。进程堆约 1.8 GB，其中段内容 100 MB，大头是主键索引（约 1 GB）与 JVM/H2 开销：`residentBytes` 只管冻结段内容，不管索引。

服务端按需启动：`java -Xmx8g -cp target/h2omap-0.1.0-SNAPSHOT.jar org.h2.tools.Server -tcp -tcpPort 19092 -ifNotExists`；客户端 URL `jdbc:h2:tcp://127.0.0.1:19092/Volumes/Yotta/Temp/h2omap/benchdb;QUERY_CACHE_SIZE=0`（客户端只需官方 H2 驱动）。

`src/test/java/com/hyd/h2omap/OmapTableContractTest.java` 是 H2 接入契约的集成测试；`src/test/java/com/hyd/h2omap/storage/ColumnarStoreTest.java` 直接驱动存储层，断言段数与 rowId 稳定性；`src/test/java/com/hyd/h2omap/vector/ValueVectorTest.java` 断言回退向量的内存估算随值内容变化。

## 当前状态与后续阶段

M0（接入契约验证）、M1（段真正产生）、M2（剪枝与流式扫描）、M3（合并与并发）、M4（索引区间查询）已完成；M5 的字典编码与延迟物化已交付，与 MVStore 的量化对比数据在 `problems/查询性能优化方向.md`，M6 判决点未开始。之后追加的 M7（向量化下推）、M8（持久化与单 Jar 打包）、M9（数组列列式存储）、M10（子查询下推）与 M11（段级剪枝与有序 LIMIT）均已完成，M12（Merge-on-Read 与冷热分层）四个批次全部交付；M13（CLOB 打包存储，方案 B2）已完成；M14（段文件尾部列目录重设计，P2）、M15（裸 MIN/MAX 定界与逐出段部分列读，A+P3）、M16（快照按段懒加载与大块 I/O）与 M17（并行段加载，P1）已完成。判定依据为 78 个契约用例、29 个存储层用例、3 个编码用例、3 个向量用例、3 个规模用例，另有 1 个 100 万行用例需显式开启。生产代码改动见 git 历史。后续每个阶段都以可运行的集成测试作为结束条件。

阶段编号按提出顺序，M7、M8 的实际交付早于 M6。

| 阶段 | 目标 | 状态 |
| --- | --- | --- |
| M0 接入契约验证 | 真实调用下 CREATE/INSERT/SELECT/UPDATE/DELETE/TRUNCATE/DROP 全通 | 完成 |
| M1 段真正产生 | rowId 插入时确定；`append` 累积到 `segmentSize` 冻结成 `ColumnSegment`；读路径先段后 delta | 完成 |
| M2 剪枝与流式扫描 | `OmapScanIndex.find` 用 zone map 剪枝；扫描按段惰性推进 | 完成 |
| M3 合并与并发 | `compact()` 分两阶段写回段；删除位图；读迭代器快照 | 完成（后台合并线程未做） |
| M4 索引 | `OmapRowIdIndex` 区间查询；唯一约束；`addIndex` 回填的规模验证 | 完成 |
| M5 编码与性能 | Varchar 字典编码、列裁剪物化、与 MVStore 表的量化对比 | 部分完成（字典编码与延迟物化已做，对比数据见 `problems/查询性能优化方向.md`） |
| M6 判决点 | 给出「行式物化开销占总耗时比例」的实测结论 | 未开始 |
| M7 向量化下推 | 替换 `Select` 与 `Comparison`，单表过滤与聚合按列执行 | 完成 |
| M8 持久化与单 Jar 打包 | WAL 与增量段快照、重启恢复；shade 合并为单 Jar；普通用户表默认走本引擎 | 完成 |
| M9 数组列列式存储 | 段内数组列拆成「前缀和下标数组 + 元素列」，`CARDINALITY` 退化为整数减法；段快照新增 `VEC_ARRAY` | 完成 |
| M10 子查询下推 | `WHERE` 里的子查询谓词折进向量化过滤：不相关标量子查询/EXISTS 常量化、`IN (SELECT)` 集合化、关联标量聚合预计算、关联 EXISTS 半连接 | 完成 |
| M11 段级剪枝与有序 LIMIT | 向量化快照按过滤条件给出的列值区间跳过不相交的段；`ORDER BY` 单列加 `LIMIT` 时按段边界排序扫描、用堆取前 N 行并整段早停 | 完成 |
| M12 Merge-on-Read 与冷热分层 | 参考 Doris/ClickHouse 的 MOR 机制支撑跨段写与冷热分层。批次 1：删除标记常驻为段级位图、全删段整体丢弃、纯删除段恢复剪枝。批次 2：更新新像移入带列向量与 zone map 的 delta rowset。批次 3：按「查询签名 × 段」的部分聚合缓存。批次 4：residentBytes 驻留预算 + 段级 LRU 逐出与同步读回 | 完成 |
| M13 CLOB 打包存储 | 方案 B2：段内 CLOB 列复用「字节池 + 偏移」机制（ClobVector），装箱时才切出 ValueClob，消掉逐值对象开销；内容仍驻堆、不读盘，谓词语义不变；段文件新增 VEC_CLOB，旧段 VEC_VALUE 读路径不变 | 完成 |
| M14 段文件尾部列目录 | 段文件重设计：头部记目录偏移，列块顺序存放，尾部目录存每列偏移/长度/zone map；全量读免重算、部分列读返回不进驻留模型的 ColumnSlice；manifest 与段文件单一 v2 格式，旧 v1 回退删除，旧数据靠重新灌入 | 完成 |
| M15 逐出段读优化 | A：裸 MIN/MAX 按骨架 zone map 迭代定界，候选段按边界排序、命中即停，逐出段只读目标列；P3：向量化快照命中逐出段时按列目录只读所需列集，临时 ColumnSlice 不进驻留模型 | 完成 |
| M16 快照懒加载与大块 I/O | 向量化快照对逐出段不预读：骨架 + 按需列加载器构造懒段，首次读列才触发磁盘读取；无 ORDER BY 的 LIMIT 先扫驻留段与开放段/新像；行式游标推进到当前段按段读回；段文件读改整段/请求列集一次大块连续读 + 内存批量解码 | 完成 |
| M17 并行段加载（P1） | 恢复与惰性预载的批量读段走固定并发 4 的共享加载线程池；预载停止信号改为「本次发布触发任何预算逐出」，预算边界多读的装不下的段至多并发数 - 1 个；并行恢复任一段失败按段序号最小者抛出 | 完成 |

已确认的设计决策：

- rowId 在插入时按「预分配的段号 + 段内偏移」编码，冻结与合并都不改变已有 rowId。
- 专用向量包含 `INT`、`BIGINT`、`DOUBLE`、`VARCHAR`、`TIMESTAMP`，其余类型用 `ValueVector` 原样存放，保证段机制对所有类型生效。
- `CHAR`/`VARCHAR` 列在段内按「UTF-8 字节池 + 每行前缀和偏移」存放（`VarcharVector`），不逐行持有 `String` 与 `byte[]`；段文件格式不变（每行仍是长度加字节），旧文件可直接恢复。逐行成本从「对象头 + 数组槽 + 字节数组」降到「长度 + 4 字节偏移」，15 字符高基数列实测 66 B/行降到 27 B/行。基数不超过段长四分之一且不超过 4096 的列仍走字典向量，不进入这条路径。
- `CLOB` 列在段内同样按「UTF-8 字节池 + 每行前缀和偏移」存放（`ClobVector` 复用 `VarcharVector` 机制，装箱时才切出 `ValueClob`），消掉逐值「ValueClob + LobDataInMemory + byte[]」的对象开销：233 字符形状实测 316 B/值降到 262 B/值，按生产 2814 万个非空 CLOB 折算约 1.4 GiB。内容仍驻堆，过滤与投影路径只多一次字节切片，不读盘；等值比较照旧走 `filterGeneric` 逐值比较，LIKE 编译入口只接受 VARCHAR/CHAR 列、交回 H2 行式路径，谓词语义不变。段文件新增 `VEC_CLOB`（与 `VEC_VARCHAR` 同布局）；旧段文件里 CLOB 列的 `VEC_VALUE` 读路径不变，未合并的旧段继续以 `ValueVector` 逐值驻留，合并后转为打包形态。`BLOB` 与 `NCLOB` 仍走 `ValueVector`。
- 数组列拆成「前缀和下标数组 `starts` + 按行拼接的元素列」两部分：`CARDINALITY` 退化为一次整数减法。元素值类型与列声明的元素类型不一致，或元素类型带扩展类型（嵌套数组）时退回 `ValueVector`；只比较元素的值类型，不比较精度，因为 H2 给 `ARRAY[]` 的元素类型是 `NULL`、给 `ARRAY['a','bb']` 的元素类型是 `CHARACTER VARYING(2)` 这样的窄类型。数组列不参与 zone map 剪枝。
- `ARRAY_CONTAINS` 在元素列上求值：按 `starts` 的区间找每行是否有元素相等，不在行上重建 `ValueArray`；`INT`/`BIGINT`/`DOUBLE`/`VARCHAR` 元素列走原始数组，字典元素列先在字典上算出匹配位图，其余类型逐元素 `compareTo`。语义按 H2：数组为 NULL 时结果是 NULL（`ARRAY_CONTAINS` 与 `NOT ...` 两种情况都不命中），非数组首参也返回 NULL。
- 字典向量在建段时按基数决定：`VARCHAR` 与低基数数值列尝试建字典，容量上限 `min(4096, max(64, 行数 / 4))`，超出则退回普通向量。
- 行式路径的区间剪枝只作用于没有未回写更新新像的段：段的 zone map 可能落后于 delta 的新像（重开后 WAL 重放把更新放回 delta 的窗口，或合并阈值未触发的窗口），被剪段内的 delta 新像会随段一起丢。删除只会收窄值域，不改变 zone map 的有效性，纯删除段仍参与剪枝（`updatedSegments` 判定）。向量化路径不受此影响（`extraRows` 无条件保留）。
- delta rowset 的列向量只在锁内可读：`upsert` 追加与容量翻倍都会替换或改动向量内部数组，无锁读者可能看到未完整发布的向量（实测在并发合并用例里解码出负长度）。因此向量化快照在读锁内物化活像，合并第一阶段在写锁内顺带物化；位置不可变只是让锁外的「按位置双重比对」不需要值引用比对，不意味着内容可以无锁读。
- 段级逐出按段级 LRU（未访问段按段号老优先、访问过的按访问序）而不是固定段号序：时间滚动场景下自然退化为逐出最老段，随机访问场景下被逐出的才是真冷段；逐出后段仍可见，行数、删除位图与每列 zone map 留在堆上的骨架里，因此逐出语义不复用惰性加载的「`loaded == null` 即不可见」标记，需要段内容的读路径同步读回。
- 骨架元数据（行数、已删行数、删除位图、每列 zone map）随 manifest 持久化：惰性加载与驻留预算同时开启时，未加载段必须既可见又可剪枝，预载停止后预算外的段不能静默丢数据；无持久化的内存库没有 manifest，未加载段回退旧惰性语义。
- 段文件重设计为「尾部列目录」单格式：头部记目录偏移，列块沿用 VEC_* 编码顺序存放，目录每项含列偏移、长度与 zone map；全量读免全列重算，`readSegmentColumns` 支持按列定位的部分读，返回不进驻留模型的 `ColumnSlice`。旧 v1 段文件与 v1 manifest 回退分支随格式重设计删除，旧数据靠重新灌入而非迁移。
- 预载的停止条件（取段前驻留量达预算即停、本次发布触发任何预算逐出即停）是内容寻址的：不依赖段号顺序，预算内的新段照常预载，预算外只在并发窗口内多读「并发数 - 1」个装不下的段，老段等查询命中时带访问标记读回；并发发布下「自己发布的段被逐出」不再可靠，停止信号取发布路径在写锁内观测到的任意逐出。
- 裸 MIN/MAX 的定界用按 zone 边界升序扫描候选段、命中即停的迭代法，而不是一次取全局最优边界，因为候选段内最优值所在行可能已被删除、一次性全局边界会算错，迭代法在任何删除形态下都精确。
- 向量化查询命中逐出段时按段文件列目录只读所需列集，返回不进驻留模型的临时 `ColumnSlice`，不再整段读回驻留，预算稳态不被冷查询搅动；行式路径不受此影响，仍读回并驻留。
- `residentBytes` 的运行时修改用 `ALTER TABLE ... SET RESIDENT_BYTES`（关键字形态，扩展 H2 既有 `ALTER TABLE SET` 语句族，需要替换 `Parser`）并持久化进 H2 目录：`CreateTableData.tableEngineParams` 是 public 字段且与 `TableBase` 私有字段共享同一个 `ArrayList`，改写条目后 `database.updateMeta` 经 `getCreateSQLForMeta` 重新渲染目录行，重开库即按新值生效；建表时没有 WITH 子句的表该列表不可变，由 `OmapTable` 覆写 `getCreateSQL`/`getCreateSQLForMeta` 在末尾追加 WITH 子句。预载只在启动时执行，预算调大不重启预载线程，由后续查询自然填驻留。
- 冻结同步在 `append` 的写锁内完成，不交给后台线程。
- 合并采用「就地写回」而不是「写新段」：段内保留删除位图，delta 的新值写回列向量，rowId 全程不变，因此不需要段集合快照与引用计数。
- 合并分两阶段：写锁内按段归组 delta 改动并记下段引用，锁外 `applyDelta` 生成新段（Copy-On-Compact，先前取到的迭代器仍读旧段），写锁内替换段引用并按「rowId 加值」双重比对移除 delta 条目。
- 合并由写路径同步触发（`append`/`remove`/`update` 后判断 delta 大小），不交给后台线程。
- 读迭代器采用创建时刻快照：固定最后一个段号与开放段行数，开放段冻结后从段列表继续读，冻结后新开的段不可见；`OmapCursor` 每批 512 行在单次读锁内取回。
- zone map 比较失败（JSON、几何等不可比较的类型）时该列退化为「恒相交」，不参与剪枝；剪枝只是优化，不能让它阻止建段。
- 主键与唯一索引用 `ConcurrentSkipListMap` 维护「索引键 → rowId」，`find` 按 `subMap`/`tailMap`/`headMap` 取区间后在存储锁之外迭代。`getCost` 沿用 `getCostRangeIndex` 且不额外加码——加码会让范围查询永远选扫描索引，区间分支不会被调用。
- 两个索引的 `getCreateSQL()` 都保持返回 `null`：区间分支虽然按索引列有序返回，但顺序保证由 H2 的 `Select.getIndexSorts` 判断，交给扫描加排序更稳妥。
- 向量化下推只在替换后的 `Select` 上触发，拒绝条件命中时退回 H2 原路径；下推只改变执行方式，不改变结果集内容。无 `ORDER BY` 时把 `LIMIT`、`OFFSET` 折成行数上限；`ORDER BY` 单列加常量 `LIMIT` 时由有序模式产出已排序结果，其余 `ORDER BY` 仍交回 H2。
- 子查询下推的折叠判定与预计算都发生在 `VectorizedFilter.compile` 编译期，不在每行求值期：不相关子查询一次求值后与常量同待遇；关联形态把内层表的一次 `takeVectorizedSnapshot` 预计算成分组聚合或键集合，外层扫描阶段只做哈希查找。预计算的聚合口径对齐 H2 的 `AggregateDataDefault`/`AggregateDataAvg`：`SUM` 溢出即回退，`AVG` 按结果类型走 double 或 `BigDecimal` 除法（HALF_DOWN），`MIN`/`MAX` 用 `session.compare`；关联键无匹配组时 `COUNT` 取 0、其余聚合取 NULL（与 SQL 空组语义一致）；内外层 NULL 键不匹配。整数列的 `IN` 与半连接用原始 `long` 开放寻址集合，避免百万级 `Value` 装箱哈希。关联聚合的外层扫描对字典键列（`DictValueVector`、`DictVarcharVector`）按字典下标预先解析出每个码的比较目标，行内只剩数组读，不再逐行装箱或哈希查找。
- 向量化快照对逐出段不预读：拍快照用堆上骨架构造懒加载段（行数、删除位图、zone map 零 I/O），执行期首次读列才触发按列目录的临时切片读；无 `ORDER BY` 的 `LIMIT` 先扫驻留段与开放段/新像，驻留可见行数不足才按需读回逐出段。这条路径的取舍是无 `ORDER BY` 时返回行序不再隐含段号升序（H2 本就不保证无排序查询的行序）。
- 行式行迭代器推进到当前相交段时按段读回（复用驻留通道，预算装不下单段时退整段临时读并复用到本段扫完），不再在入口对剩余扫描范围全量预读；行式读回段进 LRU 由预算管理，扫描集超预算时先扫的段被后扫的段挤出属预期。
- 段文件读全部走「一次大块连续读 + 内存批量解码」：全量读整段读入后用 `ByteBuffer` 视图批量拷贝原始数组、打包文本按列块长度精确分配；部分列读尾部目录一次块读、请求列在文件上首尾相接时合并成单次连续读。磁盘约束型的单线程读取下小 I/O 与逐元素解码是主要瓶颈；批量读段的 I/O 并发由 M17 的加载线程池提供。
- 默认引擎由替换后的 `Schema.createTable()` 决定，不依赖 `DEFAULT_TABLE_ENGINE`（它会把 H2 元数据表也交给本引擎）。
- 性能数字记录在 `BENCHMARK_RESULT.md` 与 `problems/查询性能优化方向.md`；回归只要求集成测试与内部计数器断言。

## 尚未实现

- 并发写的后台合并线程：合并现在由写路径同步触发。
- M12 已全部交付（批次 1 删除位图与空段丢弃、批次 2 delta rowset、批次 3 段级部分聚合缓存、批次 4 段级 LRU 逐出）；驻留预算按估算内存而非精确占用，重开库后访问热度清零、按段号老优先。
- 段级部分聚合缓存不覆盖 `COUNT(DISTINCT)`、多列分组、表达式分组键与投影/JOIN 里的子查询；这些形态照常逐段扫描。
- 数组列的剪枝与按元素过滤：数组列不参与 zone map 剪枝（算值域要重建每行 `ValueArray`，而数组值域几乎不能用于跳段），`UNNEST` 与按元素过滤也没有下推路径。
- `collectVisibleRowIds` 的数组按 `rowCount` 估计值起步并在超出时扩容，因此估计值与实际可见行数不一致时不会越界；但行数仍不可能超过 `Integer.MAX_VALUE`（`long[]` 的长度上限）。
- 向量化路径对 `delta` 新像与开放段的行逐行求值，这些行不参与按列扫描。
- `GROUP BY` 的键必须是单表列引用，表达式分组键不下推。
- 多表连接条件下的下推。子查询下推只覆盖单表 `WHERE`：投影里的子查询、JOIN 条件里的子查询、多表查询里的关联子查询仍走 H2 行式路径。
- 关联 `IN (SELECT ...)` 的投影为表达式时不折叠（内层投影限列引用）；关联子查询的关联键限单列等值，多关联条件、关联键为表达式、内层 `HAVING`/`GROUP BY`/`DISTINCT`/窗口、内层非 OMAP 表均回退。
- 关联聚合预计算只接受 `COUNT(*)`/`COUNT`/`SUM`/`MIN`/`MAX`/`AVG` 且 `SUM`/`AVG` 参数为整数列；浮点与十进制求和口径、`COUNT(DISTINCT)`、聚合参数为表达式时不折叠。
- `ILIKE`、`REGEXP` 等谓词形态未下推。
- 锚定前缀 `LIKE 'x%'` 在向量化路径里仍逐行 `getString` 装箱后交给 `CompareLike.test`（每次加锁），未在打包字节池上直接比较前缀；`LIKE` 只参与列裁剪，不提供列值区间，因此也不参与段级剪枝，前缀查询要读全部段的该列。

## 待核对

基线是 `2.5.252`，`Table`、`Index`、`Cursor` 的签名已逐条用 `javap` 核对。剩下几条：

- `CreateTableData` 位于 `org.h2.command.ddl`，却被 `org.h2.api.TableEngine` 的签名引用。这是事实上的公开接口，H2 升级时需要复核。
- `OmapTableEngine` 不解析 schema 前缀：目录名取 `data.schema.getName()`，表名里的 schema 由 H2 在调用引擎之前解析完。
- 文中 H2 行号已按 `.h2/` 逐条核对（该目录为 release zip 的 `h2/src/`，不入版本控制，获取方式见「工作约定」）；H2 版本固定为 `2.5.252`，换版本后需要重核。
- `Comparison` 的替换只为公开 `getCompareType()`。若上游补上同名方法，删除本地副本并同步 shade 排除清单。
- `ConditionInQuery` 的替换按 `2.5.252` 源码逐字合入后追加四个访问器；升级时除比对新增部分外，还要确认上游本体无改动（尤其 `getValue` 的 NULL 语义与 `createIndexConditions`）。

## 参考

- H2 架构文档：`.h2/docsrc/html/architecture.html`（第 7 到 8 层已过时，PageStore 已删除）
- MVStore 设计：`.h2/docsrc/html/mvstore.html`
- 外部表引擎样例：`.h2/test/org/h2/test/db/TestTableEngines.java`
- 量化对比与优化记录：`BENCHMARK_RESULT.md`、`problems/查询性能优化方向.md`
- 标量子查询的定位记录与当前限制：`problems/子查询暂不支持.md`（文件名沿用问题发现时的判断；正文记录两轮修复：结果集复用与子查询下推，以及仍未覆盖的形态）

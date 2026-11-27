# AGENTS.md — h2omap 工程说明

本文件面向在本仓库工作的人与代理，记录设计与实现层面的约定、依据和当前状态。使用方式见 `README.md`。

## 项目定位

OMAP 是 H2 的外部表引擎（`org.h2.api.TableEngine`）。H2 提供 SQL 前端、JDBC、优化器、元数据；本项目替换表的存储与索引，并局部替换 8 个 H2 类以支撑向量化下推与默认引擎，见「对 H2 的局部替换与打包」。

| 需求 | 结论 | 谁来做 |
| --- | --- | --- |
| 内存驻留与持久化 | 需要实现 | 本项目：列段驻留堆、WAL + 段快照持久化、按预算逐出与按需读回 |
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
| 性能目标 | 面向 OLAP：接受最快查询场景的适度放宽（如原 10 ms 级的查询增至几百 ms 可接受），优先把最慢场景的耗时与 IO 压到尽可能低（逐出段全表扫描、大规模排序与聚合、冷段读盘等）。优化提案与取舍判断都以这个目标为准：不为快场景微调而牺牲慢场景，慢场景的收益可以用快场景的开销换取 |
| JDK | 构建与运行用 JDK 21.0.11；字节码目标 `maven.compiler.release=17` |
| H2 版本 | 固定 `2.5.252`，不跟进 `2.5.259-SNAPSHOT`（`canGetRowCount` 等签名仍在变）。被替换的 8 个 H2 类按该版本源码合入，升级时必须逐类重新比对 |
| 依赖 | 运行期只有 `com.h2database:h2`，测试用 `junit-jupiter`，构建期插件 `maven-shade-plugin` |
| 测试口径 | 新增测试不建 `T_MV` MVStore 对照表，用例内预置期望值直接断言；既有含 MVStore 对照的用例保持不动，MVStore 仅保留结果正确性对照用途 |
| 判定依据 | `src/test/java/com/hyd/h2omap/OmapTableContractTest.java`（115 个用例）、`src/test/java/com/hyd/h2omap/storage/ColumnarStoreTest.java`（39 个用例）、`src/test/java/com/hyd/h2omap/storage/SegmentFileFormatTest.java`（10 个用例）、`src/test/java/com/hyd/h2omap/persistence/SegmentUpgradeTest.java`（6 个用例：v2→v3 升级、幂等重入、损坏文件拒绝开库）、`src/test/java/com/hyd/h2omap/persistence/ValueCodecTest.java`（5 个用例）、`src/test/java/com/hyd/h2omap/vector/BitPackedTest.java`（5 个用例：位宽边界、跨字、零宽拒绝、`bitsFor`）、`src/test/java/com/hyd/h2omap/vector/ValueVectorTest.java`（3 个用例）与 `src/test/java/com/hyd/h2omap/bench/DataScaleTest.java`（3 个用例），以及 `src/test/java/com/hyd/h2omap/bench/DataScaleLargeTest.java`（1 个 100 万行用例，需 `-Domap.scale=large -DargLine=-Xmx3g` 才运行）。改动表、索引、存储或向量化实现后必须跑通；不得为了让改动通过而放宽、跳过或删除用例 |
| 查 H2 源码 | `.h2/` 目录存放本仓库依赖的 H2 `2.5.252` 源码包内容（`.h2/main/org/h2/...`、`.h2/test/org/h2/...`、`.h2/docsrc/html/...`），已在 `.gitignore` 中排除，属本地参考。重新获取：`https://github.com/h2database/h2database/releases/download/version-2.5.252/h2-2026-09-23.zip`，只取其中的 `h2/src/`。该目录可能不存在（已排除在版本控制之外）。Maven Central 的 `h2-2.5.252-sources.jar` 内容等价，但个别文件与该 zip 相差数行，核对行号时以 zip 为准 |
| 临时目录 | 一次性脚本、探针程序与中间产物统一放 `.tmp/`（已在 `.gitignore` 排除，不进版本控制），任务收尾时清空；不再用 `target/dbg/` 或系统临时目录存放本仓库的调试产物 |
| 文件规模 | 新增或重构的生产类控制在单次文件读取窗口内（约 1200 行 / 45 KB，源自我代理工具的读取上限），超出时按职责拆分；拆分提交只做纯搬移（包声明、可见性、import、限定名），不得顺带改逻辑，验收是全部判定用例与用例计数不变。判定依据：超限文件只能分段读取，分段读会丢失锁契约等跨段不变量的上下文，是代理误判的主要来源 |

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

`TableEngine` 只能替换表的存储与索引，读路径下推与默认引擎都要改动 H2 自身。本分支因此替换了 8 个 H2 类，另加 3 个访问桥；替换类随本项目源码编译，`maven-shade-plugin` 打包时从 `com.h2database:h2` 中排除同名类（含内部类 `$*`），部署只需一个 Jar。

| 替换的类 | 改动 |
| --- | --- |
| `org.h2.command.query.Select` | `queryWithoutCache()` 增加 `vectorized` 标志与两个下推分支：`tryVectorizedGroup()` 排在 `queryGroup()` 之前，`tryVectorizedFlat()` 排在 `queryFlat()` 之前，命中时不再把 `offset` 归零；另在 `sort != null` 强制建 `LocalResult` 之前增加第三个分支 `tryVectorizedOrderedStreaming()`（M20），命中时直接返回 `LazyResultSelect` 流式游标（内部类 `LazyResultOrderedStreaming`），不建 `LocalResult`、不全量物化 |
| `org.h2.schema.Schema` | `createTable()` 增加两段判断：显式写 `ENGINE "org.h2.mvstore.db.Store"` 时清空 `data.tableEngine` 直接走 MVStore；`tableEngine == null` 且未设 `defaultTableEngine`、非开库重放（`!database.isStarting()`）、`data.id != 0`、非临时表时，默认取 `com.hyd.h2omap.engine.OmapTableEngine` |
| `org.h2.expression.condition.Comparison` | 只新增一个 `public int getCompareType()`，供 `VectorizedFilter` 读取比较类型 |
| `org.h2.expression.condition.ConditionInQuery` | 按 `2.5.252` 源码逐字合入，末尾新增 `getLeft()`、`isNot()`、`isAll()`、`getCompareType()` 四个访问器，供 `SubqueryFold` 读取 `IN (SELECT)` 谓词的成员 |
| `org.h2.engine.DbSettings` | 按 `2.5.252` 源码逐字合入，加两处：构造器末尾把 `DEFAULT_TABLE_ENGINE` 的展示值写成 `com.hyd.h2omap.engine.OmapTableEngine`（`defaultTableEngine` 字段仍为 `null`，`Schema.createTable` 用它判断默认引擎路径，改的只是 `INFORMATION_SCHEMA.SETTINGS` 读的那张 map；`MV_STORE` 不动，H2 元数据确实在 MVStore 里）；新增 `public final boolean omapLazyLoad = get("OMAP_LAZY_LOAD", false)`，由服务端系统属性 `-Dh2.omapLazyLoad=true` 打开（连接串参数 `;OMAP_LAZY_LOAD=TRUE` 会被 H2 以 `Unsupported connection setting` 拒绝），库打开时读一次。开关打开后 `recover()` 只登记段的 id、generation 与骨架元数据，段内容由加载线程池逐段读入 |
| `org.h2.command.Parser` | 按 `2.5.252` 源码逐字合入，只改一处：`parseSet()` 的名字链头部先认 `OMAP_RESIDENT_BYTES` 与 `OMAP_SLICE_CACHE_BYTES` 两个关键字，分别构造 `com.hyd.h2omap.command.OmapSetOmapResidentBytes` 与 `com.hyd.h2omap.command.OmapSetOmapSliceCacheBytes`，其余 SET 名保持原语义。`parseAlterTable()` 不改动、与上游一致。其余全部逐字保留。`ParserBase` 不替换 |
| `org.h2.command.Command` | 按 `2.5.252` 源码逐字合入，只在 `executeQuery()` 外包一对 `QueryIoStats.begin()/end()`，把一次查询（含其内部子命令）的段文件读取归到同一组。所有语句类型（`Statement` 与 `PreparedStatement`）都经过这里 |
| `org.h2.jdbc.JdbcStatement` | 按 `2.5.252` 源码逐字合入，改 `getMoreResults()`：取走本次查询的统计后返回一个六列结果集（`SEGMENT_ID`、`GENERATION`、`COLUMNS`、`DATA_BYTES`、`FOOTER_BYTES`、`IO_READS`），没有统计时维持原来的 `false`；另加 public `getLastQueryIoStats()` 供强转后直接拿结构化明细。`JdbcResultSet` 是 final，无法派生专用子类，所以统计以普通结果集的行承载 |

访问桥不替换原类，只是声明在同一个包里，用来读取包级与受保护成员：

| 访问桥 | 打开的成员 |
| --- | --- |
| `org.h2.expression.H2ExpressionAccess` | `Operation1.arg`、`Operation2.left`、`Operation2.right` |
| `org.h2.expression.aggregate.H2AggregateAccess` | `AbstractAggregate.args` |
| `org.h2.expression.condition.H2ConditionAccess` | `SimplePredicate.left`、`SimplePredicate.not`、`ConditionIn.left`、`ConditionIn.not`、`ConditionIn.valueList`、`ConditionAndOr.getAndOrType()`、`ConditionAndOrN.getAndOrType()`、`PredicateWithSubquery.query`（经 `getExistsQuery`/`getInQueryQuery` 读取 `ExistsPredicate` 与 `ConditionInQuery` 的子查询） |

升级 H2 时必须重做：

1. 按上表逐类重新比对替换点，确认上游没有改名、没有改变控制流、没有挪动 `vectorized` 标志要跳过的语句；`Parser` 只需比对 `parseSet()` 名字链头部两个 `OMAP_*` 分支周边，其余逐字合入。
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

合并（compaction）：`delta` 的改动数达到 `compactionMinRows` 时由写路径同步触发，分三步。第一步在写锁内把更新与删除按段归组，活像在此步物化成不可变快照（列向量只在锁内可读，锁外的无锁读者可能看到未完整发布的向量），并记下当时的段引用与活像位置；这一步只登记段引用，不读回逐出段。随后在表锁之外对要重写的逐出段逐个 `ensureResident`（段文件读取与解码不持表写锁，读回期间本表写入照常进行），再对每个受影响的段调用 `applyDelta` 生成新段（Copy-On-Compact，旧段仍对先前取到的快照有效），新段的 zone map 随建段重算；最后在写锁内按 generation 逐段安装新段（读回的基段在安装前被预算逐出不影响安装），按不可变位置双重比对移除 `delta` 里已写回的条目（防止误删锁外期间的新更新），再重算 `dirtySegments`，死位置超过半数时重排回收 rowset；锁外期间被摘除或代数已变的段只跳过该段，不整轮放弃。合并由 `compactLock` 串行化：已有合并在进行时，到达阈值的写线程直接返回、不排队（`compactionReadBacks`、`compactionSkippedTriggers`、`compactionSkippedSegments` 三个包可见计数器供验收）。回写后整段全删的段直接摘除。段号与段内偏移都不变，所以 rowId 稳定；除全删段被摘除外段不删除。

全删段丢弃（Merge-on-Read 批次 1）：段内已标记删除的行数（段位图加 delta 未回写部分）达到段行数时，写路径立即把整段从 `segments` 摘除，rowId 空洞保留不复用，行数在逐行删除时已扣过、不再调整；索引条目在每次删除时由 H2 逐行清理，摘除时无需处理。段文件在下次 checkpoint 随 manifest 摘除后由文件清理删除；恢复时 WAL 重放后由 `dropEmptySegments()` 一次性扫描补摘，惰性加载的段在加载完成时补摘。对按时间滚动的清理服务，这意味着最老的段被删空后零重写回收磁盘。

delta rowset（Merge-on-Read 批次 2）：更新新像不再按行存放（`Map<rowId, Value[]>` 每行一套 `Value` 对象），而是存进 `DeltaRowset`——复用段列向量的类型特化与打包编码，内存开销与段内数据同构；位置不可变（重复更新追加新位置、旧位置标死），合并的锁外构建阶段与双重比对都以位置为锚点；每列带自己的 zone map，向量化快照里新像与查询区间不相交时整组跳过（零物化零求值），相交时才在锁内物化进 `extraRows`。行式路径的剪枝口径不变（有未回写新像的段仍不剪枝）。tail 文件格式不变：checkpoint 仍按 rowId 集合写，恢复时逐条重放进 rowset。

段级部分聚合缓存（Merge-on-Read 批次 3）：`SegmentAggCache` 按「查询签名 × 段」缓存部分聚合结果，无分组时是 `AggState[]`、单列分组时是 `Map<Value, AggState[]>`。签名 = 过滤树语义签名（各谓词节点的列、比较、常量、集合内容）+ 分组键列 + 每个聚合的类型/列/算术口径/结果类型；`COUNT(DISTINCT)`、多列分组与含子查询预计算的谓词不参与。失效坐标是「段的 generation + 该段的 delta 屏蔽位版本」：合并会换 generation，未回写删除与更新新像的每次变化都递增 `DeltaStore` 的段版本；跨段写因此只使被改动的段失效，其余段在下一轮同签名聚合中直接合并缓存的部分结果（40 万行实测 26 ms → 1-2 ms）。合并部分结果时绝不与缓存对象按引用共享：`groups` 里的状态随后还会被 delta/开放段行累加，共享会污染缓存。分组部分结果基数超过 4096 不缓存；签名数量上限 64（访问序淘汰）。

段级逐出（Merge-on-Read 批次 4）：驻留预算是数据库级共享账本（`ResidentBudget`，按 `Database` 维护），给出冻结段驻留堆的估算字节数总预算，默认 0 不逐出；仅文件模式库生效（内存库无段文件可读回，不逐出）。配置命令是 `SET OMAP_RESIDENT_BYTES '8g'`（等号可选，纯数字按字节，可跟后缀 k/m/g 不区分大小写按 1024 换算，允许 0 = 关闭，无下限，需 admin）：值持久化进 H2 目录的 `Setting`，重开库重放后生效，重放之后建的表在构造时读同一值；调小立即在各表自己的写锁内再平衡逐出，调大不重启预载、由后续查询自然填驻留。逐出判定在各表自己的写锁内、只能逐本表的段，因此预算是软上限：一张长期无写入的冷表持量不会被热表逐出动作清掉（超出量以冷表持量为上界）。全局占用是无锁 `AtomicLong` 账本，各表写锁内经 `residentAdd` 统一入口增量维护（冻结、发布、读回、合并、逐出、摘除、truncate、注销）；账本求和不得逐个加其他表的锁，跨表锁序会死锁（两表并发发布已实证）。旧目录里的表级 `WITH "residentBytes=..."` 参数保留解析但被忽略（非法后缀仍建表报错），以免现网目录重放拒绝开库；`ALTER TABLE SET RESIDENT_BYTES` 命令已删除。逐出后段仍可见、只是慢：行数、已删行数、删除位图与每列 zone map 留在堆上的骨架里，行号级读路径（行迭代器、`collectVisibleRowIds`、可见性判定、全删段丢弃）全部走骨架；需要段内容的读路径先把段同步读回——点查在 `materialize` 前、合并第一阶段在归组前；批量行读与向量化快照不预读，改为推进到当前段/首次读列时按需读回（行式批量读复用驻留通道，向量化走临时切片读，见「逐出段的部分列读」）。逐出顺序按段级 LRU：从未被查询访问的段按段号老优先，访问过的按访问序，与数据写入顺序无关，时间滚动场景自然退化为逐出最老段。预算小于查询工作集时批量行读的读回可能被后续读回逐出，退回「读一份仅供本次使用」的临时读、不回写驻留状态；预算连单个段都容不下时（如 SET OMAP_RESIDENT_BYTES '1'）所有段保持逐出，点查与批量路径退回临时读，向量化路径的逐出段恒走临时切片读、不进驻留模型，行仍完整可见。逐出检查在段冻结的 checkpoint 之后、合并收尾与恢复收尾触发，读回后的段拥有最新访问序号，不会被紧接着的预算逐出先选中。重开库时恢复收尾把驻留裁回预算；骨架元数据随 manifest（v2）持久化，惰性加载与逐出同时开启时，重开后全部段立即可见可剪枝，预载只填预算内的内容（见「惰性加载」一节）。回归防线是「随机散布写压力用例」：随机 UPDATE/DELETE/INSERT 散布全部段，每轮与 MVStore 逐查询比对，并断言无写的第二遍命中段级部分聚合缓存，防止后续机制窄化到「只改最老几段」的滚动场景。

## 向量化执行

替换后的 `Select.queryWithoutCache()` 在四个入口尝试下推：`tryExecuteFlat`、`tryExecuteGroup` 命中后整个查询由 `OmapVectorizedExecutor` 直接写入 `LocalResult`；`tryExecuteOrderedStreaming`、`tryExecuteFlatStreaming` 命中时直接返回流式游标（`LazyResultSelect` 子类），不建 `LocalResult`、不全量物化。未命中则原样走 H2 的 `queryFlat()`、`queryGroup()`。

| 入口 | 触发的形态 | 附加前提 |
| --- | --- | --- |
| `tryExecuteFlat` | 单表投影加 `WHERE`，可带 `LIMIT`、`OFFSET`；`ORDER BY` 单列且 `LIMIT` 为常量时走有序模式 | 投影项是单表列引用或常量；`tableFilter` 用的是扫描索引；有序模式要求排序键是被扫描表的列、`limit + offset` 不超过 10000 |
| `tryExecuteGroup` | 单表聚合，可带 `GROUP BY`、`HAVING` | 分组键是单表列引用；结果列是聚合或分组键；`qualifyIndex < 0` |
| `tryExecuteOrderedStreaming`（M20/M22/M24） | 单表无 `LIMIT` 的 `ORDER BY`，可带 `WHERE`、`OFFSET` | 先试有序流式（M20/M22）：排序键全是单表列引用；排序首列不含 NULL 行——按快照内各段骨架的 hasNull 判定，任一参与段首列 hasNull 为真即试归并流式（M21 前要求首列声明 NOT NULL）；首列段间 zone 区间次邻严格分离（M22 起相邻段允许边界相交，相交带行归入下一发射单元一起归并；次邻区间相接/相交、边界缺失即试归并流式）；再试归并流式（M24）：全体参与段排序键流驻留堆归并，段间 zone 无任何要求、首列含 NULL 也接（无 hasNull 门控，NULL 组由比较器全局定序），键流量超过 `omap.mergeStreamMaxBytes`（默认 2 GiB）即整条交回 H2；投影是单表列引用、常量或确定表达式（含 `SELECT *`；顶层子查询、非确定表达式与 `_ROWID_` 等伪列交回 H2）；`tableFilter` 用扫描索引 |
| `tryExecuteFlatStreaming`（M23） | 单表无 `LIMIT` 无 `ORDER BY` 的平面查询，可带 `WHERE`、`OFFSET`（含 `SELECT *`） | 投影是单表列引用、常量或确定表达式（顶层子查询与非确定表达式交回 H2）；`tableFilter` 用扫描索引；两个流式入口都要求 `target == null`、`fetch == -1`，命中时发射顺序为「已驻留段（段号升序）→ delta 新像与开放段行 → 逐出段按需读回」，无 `ORDER BY` 的行序不再隐含段号升序 |

共同的拒绝条件：`result` 为 null、`filters.size() != 1`、`FOR UPDATE`、窗口查询、`DISTINCT` 或 `distinctExpressions != null`、`qualifyIndex >= 0`、`WITH TIES`。`VectorizedFilter.compile()` 返回 null 时同样放弃下推。

可下推的谓词：6 种 `Comparison` 比较、`BETWEEN`（`BetweenPredicate` 普通形态折叠成区间比较的 AND、`NOT BETWEEN` 折成两个取反比较的 OR，`SYMMETRIC` 与空边界交回 H2）、`AND`/`OR`（含 `ConditionAndOrN`）、`IN`、`IS NULL`/`IS NOT NULL`、`LIKE`/`NOT LIKE`、`CARDINALITY(...)`、`ARRAY_CONTAINS(数组列, 常量)`（含 `NOT`），以及比较左侧的一元与二元算术（`PLUS`、`MINUS`、`MULTIPLY`）。`ILIKE`、`REGEXP`、`= ANY(...)`/`= ALL(...)`（`ConditionInArray`）等形态交回 H2。

段级剪枝：`VectorizedFilter` 从过滤条件的顶层 `AND` 合取项收集每列上下界（`ColCmpNode` 按比较类型给出，其余节点不给出），`ColumnarStore.takeVectorizedSnapshot(lower, upper, columns)` 据此用 zone map 跳过不相交的段，任何谓词都不在被跳过的段上求值。同一列有多个合取项时只保留首次出现的界（区间取宽不取窄），因此不需要比较两个常量。

裸 MIN/MAX 定界：`tryExecuteGroup` 对无 `WHERE`、无 `GROUP BY`、单聚合且聚合参数为列引用的 `MIN`/`MAX` 走 `ColumnarStore.extremeColumn()`：候选段按目标方向的骨架 zone 边界排序后依次扫描，下一个段的边界已不优于当前最优值即停——时间递增数据通常扫一段即停，逐出段只读目标列，不进驻留模型。zone 边界含已删行，因此不能一次只扫全局边界最优的段（其最优行可能已删，迭代推进在任何删除形态下精确）；delta 新像与开放段的行无条件参与，扫描段内 delta 已删与已更新的旧像不参与。数组列与不可比较类型不定界，交回常规路径。

逐出段的部分列读：向量化快照把「投影列或聚合参数列 ∪ 分组键 ∪ 排序键 ∪ 谓词列」的列集传给 `takeVectorizedSnapshot`，拍快照不读盘——逐出段用堆上骨架构造懒加载段，行数、删除位图与 zone map 零 I/O，执行期首次读列才按段文件尾部列目录触发磁盘读取（`readSegmentColumns` → 临时 `ColumnSlice`），未在请求列集内的列以抛异常的 `AbsentColumnVector` 占位（意外访问显式暴露，不静默返回空值），不进驻留模型、不触发逐出，预算稳态不被冷查询搅动，被 `canSkipSegment` 跳过或聚合缓存命中的段零读盘。快照后段被并发读回或预载驻留时（`ensureResident` 会清 skeleton 挂 loaded），懒段读列直接复用驻留内容（驻留内容即当前代内容），不回填占位向量也不做无谓的磁盘切片读。无 `ORDER BY` 的 `LIMIT` 先扫驻留段与开放段/新像，驻留可见行数不足才按需读回逐出段，返回行序不再隐含段号升序（无 `ORDER BY` 时 H2 本就不保证行序）；行式路径（如 DISTINCT）经驻留通道推进到当前段时逐段读回，读回段进 LRU 由预算管理，与切片读形成对照。

有序模式：`ORDER BY` 单列加常量 `LIMIT` 时，段按排序列在 zone map 上的边界排序后依次扫描，用容量为 `limit + offset` 的堆留下前 N 行；堆已满且某段内最好的一行也不优于第 N 名时整段跳过。物理聚簇与排序键同向时只需扫一两段；同向不成立时结果仍正确，只是跳过不了段。多列 `ORDER BY`、排序键为表达式、`LIMIT` 缺失或超过 10000 时交回 H2。

有序流式执行（M20/M22）：无 `LIMIT` 的 `ORDER BY` 不再强制全表物化排序。排序首列的段间 zone 区间次邻严格分离（M22 起相邻段允许边界相交——亚秒级迟到写入形态；字典序由首列决定段间顺序，与后续列取值无关），段按发射方向的边界排序后逐发射单元流式输出：发射单元 U_k = 段 k 的主区间行（首键严格优于段 k+1 的近端界）∪ 段 k-1 的迟到带行（首键不严格优于段 k 的近端界），段内一次排序后按近端界二分切分主区间与迟到带，单元内按复合键比较器归并（键并列时 rowId 定序对齐 H2 稳定排序）、单元之间严格有序；delta 新像与开放段行按同一比较器预先排好后逐行归并进输出流（首键优于单元近端界的活像先于整单元发射）；客户端取一行才推进一次，停止取数时未被触碰的段零读取（TCP 会话按 fetch 批次拉取，客户端弃游标即停扫）。投影延迟物化：排序阶段每段只读「排序键 ∪ 谓词列」，行真正发出时才按行来源段读回投影列（`ColumnarStore.readDeferredProjection`，驻留段直读、逐出段走切片读、不进驻留模型；迟到带行与主区间行共享同一段的投影缓存），被谓词整段过滤空的段不产生任何投影读。投影含表达式时求值发生在发射期而非排序期：表达式引用列并入延迟读集，行发出时把该行引用列的值经 `TableFilter.set(Row)` 就位后调 `Expression.getValue(session)` 逐行求值，只对真正发射的行付费；非确定表达式（`ExpressionVisitor.DETERMINISTIC_VISITOR` 判定，如 `RAND()`）与顶层 `Subquery` 交回 H2，嵌套在其他表达式里的子查询照 H2 行式口径逐行求值，正确性相同、仅性能不设限。首列含 NULL 行的段存在时交回 H2（NULL 组跨段，`NULLS FIRST/LAST` 要求 NULL 行整体位于首/尾，流式段推进会把 NULL 组拆散；zone map 对 NULL 不可见，段间顺序无法覆盖 NULL 组）；次邻区间相接/相交（跨两段的极迟写入）同样交回；相邻段边界相交已由 M22 迟到带归并覆盖。NULL 判定依据是段骨架的 hasNull——M21 起从「首列声明 NOT NULL」放宽为内容判定，hasNull 随段持久化、claim 时刻零 I/O；合并把 NULL 新像写回段后 hasNull 单调置真，下一轮同查询自动回退。首行延迟从「全表扫描 + 装箱 + 全量排序」降到「最新一段解码 + 段内排序 + 首批投影」。H2 2.5.252 对 lazy 结果不复用（`Query.executeQuery` 的复用门把 lazy 结果排除，部分读、读尽、读尽未关三种形态实测都重新执行），流式游标的 `reset()` 属防御性实现、当前不可达。

平面流式执行（M23）：无 `LIMIT` 无 `ORDER BY` 的单表平面查询（如大表的 `SELECT *`）同样不建 `LocalResult`、不全量物化，直接返回流式游标（`LazyResultFlatStreaming`）。动机是行式全表扫描对逐出段会整表读回并全量装箱，服务端在发出首行前就付出全部代价；本入口把发射改成按需，客户端取一行才推进一次，停止取数时未触碰的段零读取。发射顺序沿用 M16 无序 `LIMIT` 的驻留优先口径：先发已驻留段（段号升序），再发 delta 新像与开放段行（`extraRows`，已在内存），最后按段号升序按需读回逐出段——逐出段首次取行才经向量化快照的临时切片读回列向量，不进驻留模型；谓词在段级向量化过滤，`OFFSET` 在发射流上跳过。投影分类与 M20-2 共用 `ProjectionPlan`（列引用/常量/确定表达式三类，确定表达式在行发出时经 `TableFilter.set(Row)` 就位后逐行求值，发射行视图 `EmitRow` 亦共用）。带 `LIMIT` 的平面查询仍走 `tryExecuteFlat` 的物化路径（含 M11 有序模式与 M16 驻留优先 LIMIT），带 `ORDER BY` 走 `tryExecuteOrderedStreaming`；两个流式游标的 `reset()` 同属防御性实现。

归并流式执行（M24）：段间 zone 无序的 `ORDER BY`（随机键形态，如 `ORDER BY KEY1`）不再整条交回 H2 全量物化排序。claim 链为「有序流式（次邻分离）→ 归并流式 → H2 行式」，facade `tryExecuteOrderedStreaming` 依次尝试：次邻分离时段序已知、任一时刻只需当前发射单元在场（免归并）；不满足时输出行的下一候选只可能是各段当前头，必须真归并——对快照内每个参与段只读排序键列（`takeVectorizedSnapshot` 的谓词列集 ∪ 排序键列），段内按复合键比较器排序成键流，全体段头进堆做 k 路归并（键并列时 rowId 定序对齐 H2 稳定排序）；delta 新像与开放段行作为一条独立键流参与归并。归并模式没有 zone 门控也没有 hasNull 门控：NULL 组天然由比较器全局定序，不会被流式段推进拆散。键流量按 `Value.getMemory()` 精确记账、全体参与段一次性驻留，粗估 `行数 × (行开销 + 每键 64 B × 键列数)` 先拒、段排序后再按装箱键精确复核；超过系统属性 `omap.mergeStreamMaxBytes`（默认 2 GiB）即返回 null 整条交回 H2——这是简单版口径（有界版「占位头 + 二分重放」留作表增长后的升级项）。投影延迟物化与 M20/M22 共用：行发出时才按行来源段 `readDeferredProjection`，不进驻留模型；`OFFSET` 在发射流上跳过。首行延迟从「全表读 + 全量装箱 + 全量排序」（现网 837 万行实测 81.5 s）降到「全部段键列读 + 段内排序 + 堆初始化」（键流量 165 MB / 1.1 GB 量级，均落预算内）。

子查询下推（入口 `SubqueryFold`，由 `VectorizedFilter.compile` 末尾与 `compileComparison` 调用；内层形态解析与共享原语在 `SubqueryInner`，S3/S4/S5 各形态在 `CorrelatedAggFold`/`CorrelatedExistsFold`/`CorrelatedInFold`）：

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
manifest.dat                  段清单：nextSegmentId、walSeq、每段的 id 与 generation，以及每段骨架元数据（行数、已删行数、删除位图、每列 zone map 与每列 hasNull）
seg_<id>_<gen>.omap           列段快照：头部（魔数 OSG3、formatVersion=3、段号/代数/行数/已删行数/目录偏移）→ 删除位图 → 列块（沿用 VEC_* 编码，顺序存放）→ 尾部列目录（每列偏移、长度、zone map min/max、hasNull）
tail_<walSeq>.omap            检查点时刻的开放段行、openDeletes 与 delta
wal_<seq>.log                 检查点之后的增量：insert/update/delete/truncate
```

manifest 与段文件当前为 v3 格式（manifest 魔数 OMAPR、段文件魔数 OSG3，头部紧随魔数写 formatVersion=3；骨架与段文件尾部目录都新增每列 hasNull，与 zone map 同构维护：建段/合并重建时由列向量算出、恢复时读回、逐出后留在骨架，claim 时刻零 I/O 可查）。WAL 与 tail 不含骨架元数据，不版本化。v1 文件仍报非法魔数；v2 目录在启动时自动升级：`recover()` 发现 manifest 是 OMAPQ 就对全部登记段并行检查（复用加载线程池），OSG2 段全量读入并按 v3 原子写回（tmp + rename，段号/代数/内容语义不变），OSG3 段只读头部与尾部目录回填骨架 hasNull（升级中途崩溃后重入的幂等分支），任一段失败按段号最小者抛错拒绝开库，完成后原子重写 manifest 为 OMAPR；manifest 已是 OMAPR 时整个升级跳过——新版本 manifest 意味着全部活跃段文件已同版本，稳态启动零额外 I/O。一次性代价是部署后首次重启把存量段文件全量读写一遍（4 线程并行）。全量读经尾部目录恢复 zone map 与已删行数，不再全列重算；整段一次大块读入后列块与目录都在内存缓冲上解码——原始数组用 `ByteBuffer` 视图一次批量拷贝（消掉 `DataInputStream` 逐元素的同步单字节拼接），打包文本按列块长度恒等式「剩余字节 - rows*4」一次精确分配零扩容，目录偏移与列块长度做位置校验；`OmapPersistence.readSegmentColumns` 尾部目录一次块读，请求列在文件上首尾相接时合并成单次大块连续读，返回 `ColumnSlice`（列向量 + zone map + 行数 + 删除位图），切片不注册段引用、不计入驻留预算、不触发 LRU 触碰，只服务向量化快照与临时读路径。

`append`、`remove`、`update`、`truncate` 在存储写锁外追加一条 WAL 记录，`syncExecutor` 按 `syncIntervalMs` 对通道执行 `force`。开放段写满冻结后立即 `checkpoint()`；`compact()` 的改动数达到 `max(1024, compactionMinRows)` 时也 `checkpoint()`。`checkpoint()` 在写锁内取段清单、开放段行、`openDeletes` 与 `delta` 的快照并 `rotateWal()`，锁外写段文件与 `tail`，最后写 `manifest.dat` 并清理过期文件。

恢复顺序：读 `manifest.dat` 与其中的段文件，读 `tail_<manifestWalSeq>.omap`，按序号重放 `wal_<manifestWalSeq>` 及其后的全部 WAL，最后以 `maxSeq + 1` 打开新 WAL。空目录走 `initEmptyWal()`，只开 `wal_1.log`。

`OmapTable.close()` 关掉 persistence，`removeChildrenAndResources()` 删除整个目录。索引不在 WAL 里：重启后 H2 重放 `ALTER TABLE ADD CONSTRAINT` 会重新调用 `addIndex`。

### 惰性加载

由服务端启动参数 `-Dh2.omapLazyLoad=true` 打开，默认关闭；开关关闭时 `recover()` 在开库时读完所有段（M17 起经加载线程池并行读回），行为与惰性加载引入前一致。

开关打开时，`recover()` 只读 `manifest.dat` 登记每段的 id、generation 与骨架元数据，段文件不读，随后 `ColumnarStore.startLazyLoading()` 在 `OmapPersistence.LOAD_CONCURRENCY`（固定 4，代码常量、无对外参数）的共享加载线程池上按段号**倒序**加载：4 个工人线程共享一个递减游标，段号与写入顺序同向，最近写入的数据最先驻留；非惰性恢复同样经该线程池并行读回全部段，任一段读失败按段序号最小者抛出。实测 800 个段 4 万行：开库 328 → 198 ms。

可见性口径：骨架随 manifest 恢复后，**未加载段对全部读路径整体可见且可剪枝**——行数（骨架行数减已删行数）、删除位图与每列 zone map 都在堆上，开库第一次 `COUNT(*)` 就是全量；需要段内容的读路径（点查 `materialize`、批量 `nextBatch`、向量化快照、合并归组）按需读回。无持久化的内存库没有 manifest，未加载段整体不参与，`publishLoaded` 发布时才计入行数与递增 modificationId。预载与驻留预算同时开启时，预载在两个条件下停止（预算判定读库级共享账本）：取段前全库驻留量已达预算即停；本次发布触发了任何预算逐出即停（并发下预算边界最多多读「并发数 - 1」个装不下的段，此后零无效读；终驻留集与顺序预载一致，逐出永远选已加载段中 id 最小者）。全表聚合会把驻留集滚动到最近触碰的段，目标查询只读回骨架 zone map 相交的段。

三处配套规则：

| 规则 | 原因 |
| --- | --- |
| 有骨架段的发布（`publishLoaded`）只挂内容，不递增 modificationId、不动行数 | 骨架已在恢复时计入行数且保证可见，内容加载不改变任何查询结果；无骨架段（无持久化的内存库）发布时仍递增 modificationId 并经 `visibilityListener` 调 `OmapTable.setModified()`，否则 H2 结果缓存返回加载前旧结果 |
| `isVisible` 对未加载段的行返回 `true`，`countedInRowCount` 对这些行返回 `false` | WAL 重放与客户端删除必须能记进 delta，但无骨架段的行在加载前尚未计入 `rowCount`，不能重复扣减；有骨架段两者都直接看骨架 |
| `checkpoint()` 只重写 generation 变了的段，未加载段由 `loadedSegment()` 返回 `null` 时跳过 | gen 只在合并时递增，而合并要求段已加载，所以未加载段无需写盘 |

合并（`compact()`）在加载期间只处理已加载段，指向未加载段的 delta 条目留在 delta 里，等该段加载后再合并；这些改动在有骨架段上始终参与可见性。

`addIndex` 回填：有骨架段对回填可见，回填会把全部段按需读回（索引唯一性要求覆盖每一行，属必要读而非无效读；预算开启时伴随逐出翻搅）；无骨架段（无持久化的内存库）回填只覆盖已加载段，唯一约束在加载完成前不完整。生产表没有主键与唯一约束（`INFORMATION_SCHEMA.INDEXES` 为 0 行），不触发。

### 查询期段文件 IO 统计

目的：记录每次查询读了哪些段文件的哪些列、搬运多少字节，作为后续 IO 调度（并发配额、请求合并、优先级）的依据。当前只做采集与回传，不做调度。

| 项 | 口径 |
| --- | --- |
| 开关 | 服务端系统属性 `-Domap.ioStats=true`（`QueryIoStats.setEnabled` 可在代码里改，供测试与基准用）；默认关闭，关闭时采集方法直接返回 |
| 归组边界 | 一次 `org.h2.command.Command.executeQuery()`，含该命令内的全部子命令（同线程嵌套共用一个收集器，最外层结束时发布） |
| 采集点 | `OmapPersistence.readSegment()`（整段读，恢复与惰性预载也走这里，但那时无收集器，不入统计）与 `readSegmentColumns()`（按列目录部分读，向量化快照的临时切片与行式按需读回都走这里） |
| 字节口径 | `DATA_BYTES` = 本次读取的列块长度之和（连续读的起止差）；`FOOTER_BYTES` = 尾部目录一次固定开销（列偏移与长度表加 zone map）；整段读记文件大小，`FOOTER_BYTES` 为 0 |
| 读请求次数 | `IO_READS` = 本次读取提交给文件通道的读请求数，用于判断读取是否碎片化：部分列读固定为「头部 1 次 + 尾部目录 1 次 + 每个连续列块 1 次」，整段读为填满整个文件的循环次数；`QueryIoStats` 另有 `getTotalIoReads()` 与 `getAverageIoReadBytes()` |
| 并发合并 | `OmapPersistence.loadSegment()` 与 `readSegmentColumns()` 对同键的**在途**请求合并：只让一个线程真正读盘，其余等待同一个结果。键为 `(段号, 代数, 列集位掩码)`，列 id ≥ 64 时不合并。合并只在请求在途期间生效，完成后立即从在途表移除，不缓存结果 |
| 容量上限 | 单次查询最多 8192 条明细，超出只置 truncated 标记，合计值不受影响 |
| 传递通道 | ThreadLocal 从命令执行线程传给替换后的 `JdbcStatement.getMoreResults()`：返回六列结果集 `SEGMENT_ID`、`GENERATION`、`COLUMNS`、`DATA_BYTES`、`FOOTER_BYTES`、`IO_READS`；强转 `JdbcStatement` 后可调 `getLastQueryIoStats()` 拿结构化明细与合计 |

`COLUMNS` 列整段读为 `*`，部分列读为逗号分隔的列 id（按请求顺序）。统计只覆盖文件模式库的按需读回：内存库没有段文件可读，恒为空；恢复与预载发生在命令之外，也不计入。TCP 模式下统计产生在服务端进程，客户端 JDBC 层拿不到（H2 传输协议没有附加字段），需要时由服务端另行导出。

并发合并后的统计口径：只有真正执行读盘的线程产生 IO 记录，等待者（合并命中）不产生记录，因此统计反映的是「本次查询实际引发的读盘量」而非「逻辑读取量」。判定用例 `SegmentFileFormatTest.concurrentSegmentReadsAreCoalesced` 用串行 16 次与并发 16 次对照，断言并发总字节不超过串行的一半。

### 列切片缓存

目的：消除重复非聚合查询对同一逐出段的重复读盘。向量化快照命中逐出段时走临时切片读，而临时切片**不进驻留模型**（M16 的刻意设计，避免冷查询搅动 `residentBytes` 预算），因此重复查询会反复读盘。实测 benchdb（900 万行、`residentBytes=100 MiB`）上 `SELECT ID, INT_COL FROM T WHERE NULLABLE_COL IS NOT NULL` 每轮读 133 MB / 620 次 IO，三轮零复用。

| 项 | 口径 |
| --- | --- |
| 开关与预算 | 数据库全局变量 `SET OMAP_SLICE_CACHE_BYTES '64m'`（等号可选），值是带单位的数字：纯数字按字节，可跟后缀 k/m/g（不区分大小写，按 1024 换算），带后缀时写成字符串字面量；默认与下限均为 32 MiB（32768k），更小值与 NULL 报 Invalid value，即缓存不可关闭、不可低于 32 MiB。需要 admin 权限。值以规范化字节数存进 H2 目录的 `Setting`（`INFORMATION_SCHEMA.SETTINGS` 可查），重开库重放 `SET` 后仍生效，重放之后建的表在构造时从目录读同一值 |
| 生效范围 | 库内全部 OMAP 表：`SET` 时对已存在的表立即下推新预算，每个表的切片缓存各自持有该预算（多表时总占用为预算 × 表数） |
| 预算调整 | 调小立即按 LRU 淘汰到新一档；调大不主动预载，由后续查询自然填入缓存（与全局驻留预算语义对称） |
| 缓存对象 | 段列切片（`ColumnSlice`：列向量 + zone map + 行数 + 删除位图），不是查询结果 |
| 缓存键 | `(段号, 代数, 列集位掩码)`；列 id ≥ 64 时 `maskOf` 返回 `Long.MIN_VALUE`，该请求不参与缓存 |
| 失效坐标 | 只有 generation。冻结段内容在合并前不可变，delta 的未回写删除与更新新像不在切片内（可见性判定另走 delta），因此 delta 变化不影响正确性。合并、全删段摘除、`truncate` 都会清掉该段号的全部条目 |
| 淘汰 | 字节预算 + LRU（`LinkedHashMap` 访问序）；单个切片就超预算时不放入，避免插入即淘汰自身 |
| 与驻留的关系 | 相互独立：不计入全局驻留预算、不参与逐出、不触发逐出；驻留段走内存，逐出段走本缓存 |
| 并发 | 全部方法同步在同一个锁上；取出的切片其向量与删除位图只读，可跨线程共享 |

收益取决于查询形态：全表扫描类查询的读只占总耗时一小部分（实测同一查询段驻留后 1098 → 1032 ms，约 6%），宽窗口有序扫描类查询读几乎就是全部（实测 48 → 4 ms，约 90%）。判定用例：`SegmentFileFormatTest.sliceCacheServesRepeatReadsOfEvictedSegment`（重复读命中、IO 归零、同一向量实例）、`sliceCacheInvalidatesWhenSegmentIsCompacted`（合并换代后旧条目清除）、`sliceCacheBudgetShrinkEvictsImmediately`（调小立即 LRU 淘汰），契约用例 `OmapTableContractTest.setOmapSliceCacheBytesIsGlobalWithLowerBoundAndPersists`（默认 32 MiB、下限拒绝、目录持久化与重开生效）。

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
├── command/       SET OMAP_RESIDENT_BYTES 与 SET OMAP_SLICE_CACHE_BYTES 命令
├── engine/        接入 H2：TableEngine 与 WITH 参数
├── table/         OmapTable，继承 TableBase
├── index/         扫描索引、rowId 索引、游标
├── storage/       列段、开放段缓冲、delta、rowId 编码、延迟行、部分列读的列切片与占位列向量；段内行迭代器独立成文件 SegmentRowIterator，向量化快照的顶层记录 VectorizedSnapshot、极值结果 ExtremeValue 与快照构建器 SnapshotQuery；后台段加载与发布 SegmentLoader；检查点与开库恢复 StorePersistence；跨表驻共享账本 ResidentBudget（按 Database 经 OmapResidentBudgets 维护）；逐出段列切片缓存 SliceCache（预算由全局 SET OMAP_SLICE_CACHE_BYTES 配置，默认 32 MiB）
├── persistence/   WAL 增量日志、增量列段快照与重启恢复
├── vector/        列向量（含字典向量、数组列与 CLOB 打包、数值列与字典码的位打包 BitPacked：可写原始数组 / 编码态双形态）、ValueVector 回退、null 位图
├── vectorized/    单表过滤与聚合下推：谓词编译入口 VectorizedFilter（含过滤树编译与区间/签名工具），各谓词节点独立成文件 AndNode/OrNode/ColCmpNode/ArithLongCmpNode/InNode/NullCheckNode/CardinalityCmpNode/ArrayContainsNode/LikeNode 与原始 long 集合 LongSet；子查询折叠（SubqueryFold 入口 + SubqueryInner + CorrelatedAggFold/CorrelatedInFold/CorrelatedExistsFold）；执行入口 OmapVectorizedExecutor（facade，含共用快照管线）+ FlatExecutor/OrderedExecutor/GroupExecutor；流式执行 FlatStreamingExecutor/OrderedStreamingExecutor/OrderedMergeExecutor（行源 FlatStreamingRowSource/OrderedStreamingRowSource，共用投影分类 ProjectionPlan 与发射行视图 EmitRow）；聚合口径 AggSpec、累加器 AggState、HAVING 编译 HavingPredicates；段级部分聚合缓存（SegmentAggCache）
└── stats/         zone map；查询期段文件 IO 统计 QueryIoStats（收集器 + 明细记录）

src/main/java/org/h2/                     被替换的 H2 类与访问桥
├── command/Command.java                      替换：查询期 IO 统计的归组边界
├── command/Parser.java                       替换：SET OMAP_RESIDENT_BYTES 与 SET OMAP_SLICE_CACHE_BYTES 语法入口
├── command/query/Select.java                 替换：向量化下推入口
├── engine/DbSettings.java                     替换：默认引擎展示值与 omapLazyLoad 开关
├── jdbc/JdbcStatement.java                   替换：getMoreResults() 返回 IO 统计结果集
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
├── vector/BitPackedTest.java     位打包
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

一键重建：`./scripts/resident-env.sh`（驻留预算已改库级 SET OMAP_RESIDENT_BYTES）。数据目录默认 `/Volumes/Yotta/Temp/h2omap`（本机 SSD APFS 卷），可用 `OMAP_RESIDENT_DATA_DIR`、`OMAP_RESIDENT_ROWS`（默认 900 万）、`OMAP_RESIDENT_BUDGET`（默认 `100m`）、`OMAP_RESIDENT_XMX`（默认 `8g`）覆盖。流程：生成 900 万行 10 列表 `T`（文件库 `benchdb`，磁盘约 1 GB、139 个段）→ `SET OMAP_RESIDENT_BYTES '100m'` 设 100 MiB 库级驻留预算 → 校验行数与 `INFORMATION_SCHEMA.SETTINGS` 里的 `OMAP_RESIDENT_BYTES` 值。重建耗时约 5 分钟（含 `benchSetup` 的双插与收尾查询）；脚本重置时会删除 `benchdb*` 全部文件（含 `.trace.db`）。

实测口径（Apple M4、900 万行）：开库 7–10 s，瓶颈不是磁盘——1 GB 段文件由 4 线程并行读只占零点几秒，大头是 `ID BIGINT PRIMARY KEY` 的 rowId 索引回填（900 万次 `ConcurrentSkipListMap.put` 串行，约 1 µs/行）；`COUNT(*)` 走骨架零段读（≤5 ms）；冷 `SUM(INT_COL)` 按列目录只读约 36 MB 列切片（13–19 ms）；二次 `SUM` 命中段级聚合缓存（0 ms）；开库后驻留 100,382,915 B ≤ 预算，逐出后 8 项聚合签名与逐出前一致。进程堆约 1.8 GB，其中段内容 100 MB，大头是主键索引（约 1 GB）与 JVM/H2 开销：`residentBytes` 只管冻结段内容，不管索引。

服务端按需启动：`java -Xmx8g -cp target/h2omap-0.1.0-SNAPSHOT.jar org.h2.tools.Server -tcp -tcpPort 19092 -ifNotExists`；客户端 URL `jdbc:h2:tcp://127.0.0.1:19092/Volumes/Yotta/Temp/h2omap/benchdb;QUERY_CACHE_SIZE=0`（客户端只需官方 H2 驱动）。

`src/test/java/com/hyd/h2omap/OmapTableContractTest.java` 是 H2 接入契约的集成测试；`src/test/java/com/hyd/h2omap/storage/ColumnarStoreTest.java` 直接驱动存储层，断言段数与 rowId 稳定性；`src/test/java/com/hyd/h2omap/vector/ValueVectorTest.java` 断言回退向量的内存估算随值内容变化。

## 当前状态与后续阶段

M0（接入契约验证）、M1（段真正产生）、M2（剪枝与流式扫描）、M3（合并与并发）、M4（索引区间查询）已完成；M5 的字典编码与延迟物化已交付，与 MVStore 的量化对比数据在 `problems/查询性能优化方向.md`，M6 判决点未开始。之后追加的 M7（向量化下推）、M8（持久化与单 Jar 打包）、M9（数组列列式存储）、M10（子查询下推）与 M11（段级剪枝与有序 LIMIT）均已完成，M12（Merge-on-Read 与冷热分层）四个批次全部交付；M13（CLOB 打包存储，方案 B2）已完成；M14（段文件尾部列目录重设计，P2）、M15（裸 MIN/MAX 定界与逐出段部分列读，A+P3）、M16（快照按段懒加载与大块 I/O）、M17（并行段加载，P1）、M18（查询期段文件 IO 统计与在途读合并）、M19（逐出段列切片缓存）、M20（无 LIMIT 的 ORDER BY 有序流式执行）、M21（磁盘格式 v3、启动自动升级与有序流式 claim 门控放宽）、M22（次邻分离门控与迟到带归并）、M23（平面查询流式执行）与 M24（段间无序 ORDER BY 的 k 路归并流式执行）已完成；列编码与压缩阶段 A（数值列与字典码位打包，计划见 `problems/列编码与压缩实施计划.md`）已完成，阶段 B（列块压缩与格式 v4）、C（段内分页）、D（浮点编码）未开始。判定依据为 115 个契约用例、49 个存储层用例（ColumnarStoreTest 39 + SegmentFileFormatTest 10）、6 个 SegmentUpgradeTest 升级用例、5 个编码用例、5 个位打包用例、3 个向量用例、3 个规模用例，另有 1 个 100 万行用例需显式开启。生产代码改动见 git 历史。后续每个阶段都以可运行的集成测试作为结束条件。

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
| M12 Merge-on-Read 与冷热分层 | 参考 Doris/ClickHouse 的 MOR 机制支撑跨段写与冷热分层。批次 1：删除标记常驻为段级位图、全删段整体丢弃、纯删除段恢复剪枝。批次 2：更新新像移入带列向量与 zone map 的 delta rowset。批次 3：按「查询签名 × 段」的部分聚合缓存。批次 4：驻留预算 + 段级 LRU 逐出与同步读回（预算已从表级 WITH 参数改为库级共享 SET OMAP_RESIDENT_BYTES） | 完成 |
| M13 CLOB 打包存储 | 方案 B2：段内 CLOB 列复用「字节池 + 偏移」机制（ClobVector），装箱时才切出 ValueClob，消掉逐值对象开销；内容仍驻堆、不读盘，谓词语义不变；段文件新增 VEC_CLOB，旧段 VEC_VALUE 读路径不变 | 完成 |
| M14 段文件尾部列目录 | 段文件重设计：头部记目录偏移，列块顺序存放，尾部目录存每列偏移/长度/zone map；全量读免重算、部分列读返回不进驻留模型的 ColumnSlice；manifest 与段文件单一 v2 格式，旧 v1 回退删除，旧数据靠重新灌入 | 完成 |
| M15 逐出段读优化 | A：裸 MIN/MAX 按骨架 zone map 迭代定界，候选段按边界排序、命中即停，逐出段只读目标列；P3：向量化快照命中逐出段时按列目录只读所需列集，临时 ColumnSlice 不进驻留模型 | 完成 |
| M16 快照懒加载与大块 I/O | 向量化快照对逐出段不预读：骨架 + 按需列加载器构造懒段，首次读列才触发磁盘读取；无 ORDER BY 的 LIMIT 先扫驻留段与开放段/新像；行式游标推进到当前段按段读回；段文件读改整段/请求列集一次大块连续读 + 内存批量解码 | 完成 |
| M17 并行段加载（P1） | 恢复与惰性预载的批量读段走固定并发 4 的共享加载线程池；预载停止信号改为「本次发布触发任何预算逐出」，预算边界多读的装不下的段至多并发数 - 1 个；并行恢复任一段失败按段序号最小者抛出 | 完成 |
| M18 查询期段文件 IO 统计 | 采集每次查询读了哪些段文件的哪些列、搬运多少字节与读请求次数，经替换后的 `Command` 归组、`JdbcStatement.getMoreResults()` 回传；另在两个读入口加在途请求合并，消除并发窗口内的重复读盘 | 完成 |
| M19 逐出段列切片缓存 | 临时切片读的结果按 `(段号, 代数, 列集)` 缓存，字节预算 + LRU；预算是数据库全局变量 `SET OMAP_SLICE_CACHE_BYTES`（默认与下限 32 MiB，持久化进目录）；失效坐标只有 generation | 完成 |
| M20 有序流式执行 | 无 `LIMIT` 的 `ORDER BY` 在首列段间 zone 区间两两严格不相交时按段流式输出：段按发射序边界排序、段内一次排序、delta 新像与开放段行归并；投影延迟物化，段内首行发出前才读投影列；客户端停止取数即停扫。首列实际含 NULL（骨架 hasNull 判定）、次邻区间相接或相交（M22 起相邻相交由迟到带归并覆盖）、排序键为表达式时交回 H2 | 完成 |
| M21 格式 v3 与启动自动升级 | 段文件/manifest 版本化（OSG3/OMAPR + formatVersion，每列 hasNull 进尾部目录与骨架）；v2 目录启动时并行自动升级、失败拒绝开库、崩溃重入幂等、manifest 新版本即整体跳过；有序流式 claim 门控从「首列声明 NOT NULL」放宽为「全部参与段首列 hasNull == false」 | 完成 |
| M22 次邻分离门控与迟到带归并 | 有序流式 claim 门控从「首列段间 zone 区间两两严格不相交」放宽为「次邻段严格分离」：相邻段边界相交（迟到写入）时，段内排序后按下一发射单元的近端界二分切分主区间与迟到带，迟到带行归入下一单元与后段主区间一起归并；次邻相接/相交、首列实际含 NULL 仍整条交回 H2。发射单元与行来源段化（`Unit`），延迟投影按行来源段读回、共享段级投影缓存。修复懒加载段读路径竞态：`readSliceTransient`/`readSegmentTransient` 先查 `ref.loaded`，段被并发读回驻留时直接返回驻留内容，不再误判为不可读 | 完成 |
| M23 平面查询流式执行 | 无 `LIMIT` 无 `ORDER BY` 的单表平面查询流式下推：不建 `LocalResult`、不全量物化，客户端取一行才推进一次，停止取数即停扫，未触碰的段零读取。发射序驻留段优先（与 M16 无序 LIMIT 同口径），逐出段首次取行才临时切片读回、不进驻留模型；投影分类（`ProjectionPlan`）与发射行视图（`EmitRow`）从 M20 抽出共用。带 `LIMIT` 仍走 `tryExecuteFlat` 物化路径，带 `ORDER BY` 走有序流式 | 完成 |
| M24 归并流式执行 | 段间 zone 无序的 `ORDER BY` 走 k 路归并流式：每个参与段只读排序键列、段内排序成键流，全体段头进堆归并，delta 新像与开放段行作为独立键流参与；无 zone 门控、无 hasNull 门控（NULL 组由比较器全局定序）；键流量按 `Value.getMemory()` 精确记账，超 `omap.mergeStreamMaxBytes`（默认 2 GiB）整条交回 H2。claim 链「有序流式 → 归并流式 → H2」，随机键形态（现网 `ORDER BY KEY1` 81.5 s 全量排序）首行延迟降到键列读 + 段内排序 + 堆初始化 | 完成 |
| 列编码与压缩 | A 位打包：`INT`/`BIGINT`/`TIMESTAMP`（dateValue）与字典码按位宽打包（`BitPacked`，`long[]` 字包），向量双形态（开放段与合并回写用可写原始数组、冻结段用编码态），查询路径经单点读 `longAt`/`codeAt`/`dateAt`，不新增向量类型；编码后字节数不比原始省 15% 以上、含负值或位宽 > 32 时退回原数组；磁盘格式 v3 不变。B 列块压缩 + 格式 v4 + 启动升级、C 段内分页与页级剪枝、D 浮点编码未开始，见 `problems/列编码与压缩实施计划.md` | A 完成；B/C/D 未开始 |

已确认的设计决策：

- 逐出段的列切片缓存（M19）只缓存「从段文件读出的列切片」，不缓存查询结果，也不与 `residentBytes` 共享预算：临时切片读不进驻留模型是 M16 为避免冷查询搅动预算的刻意设计，本缓存不计入驻留预算、不参与逐出、不触发逐出。失效坐标只有 generation，因为冻结段内容在合并前不可变，delta 的未回写删除与更新新像不在切片内（可见性判定另走 delta）。预算是数据库全局变量 `SET OMAP_SLICE_CACHE_BYTES`（默认与下限 32 MiB，调小立即 LRU 淘汰、调大不预载，值持久化进 H2 目录）；`Parser` 的 `parseSet` 里认这个关键字，命令类是 `OmapSetOmapSliceCacheBytes`，对库内全部 OMAP 表下推。

- rowId 在插入时按「预分配的段号 + 段内偏移」编码，冻结与合并都不改变已有 rowId。
- 专用向量包含 `INT`、`BIGINT`、`DOUBLE`、`VARCHAR`、`TIMESTAMP`，其余类型用 `ValueVector` 原样存放，保证段机制对所有类型生效。
- `CHAR`/`VARCHAR` 列在段内按「UTF-8 字节池 + 每行前缀和偏移」存放（`VarcharVector`），不逐行持有 `String` 与 `byte[]`；段文件格式不变（每行仍是长度加字节），旧文件可直接恢复。逐行成本从「对象头 + 数组槽 + 字节数组」降到「长度 + 4 字节偏移」，15 字符高基数列实测 66 B/行降到 27 B/行。基数不超过段长四分之一且不超过 4096 的列仍走字典向量，不进入这条路径。
- `CLOB` 列在段内同样按「UTF-8 字节池 + 每行前缀和偏移」存放（`ClobVector` 复用 `VarcharVector` 机制，装箱时才切出 `ValueClob`），消掉逐值「ValueClob + LobDataInMemory + byte[]」的对象开销：233 字符形状实测 316 B/值降到 262 B/值，按生产 2814 万个非空 CLOB 折算约 1.4 GiB。内容仍驻堆，过滤与投影路径只多一次字节切片，不读盘；等值比较照旧走 `filterGeneric` 逐值比较，LIKE 编译入口只接受 VARCHAR/CHAR 列、交回 H2 行式路径，谓词语义不变。段文件新增 `VEC_CLOB`（与 `VEC_VARCHAR` 同布局）；旧段文件里 CLOB 列的 `VEC_VALUE` 读路径不变，未合并的旧段继续以 `ValueVector` 逐值驻留，合并后转为打包形态。`BLOB` 与 `NCLOB` 仍走 `ValueVector`。
- 数组列拆成「前缀和下标数组 `starts` + 按行拼接的元素列」两部分：`CARDINALITY` 退化为一次整数减法。元素值类型与列声明的元素类型不一致，或元素类型带扩展类型（嵌套数组）时退回 `ValueVector`；只比较元素的值类型，不比较精度，因为 H2 给 `ARRAY[]` 的元素类型是 `NULL`、给 `ARRAY['a','bb']` 的元素类型是 `CHARACTER VARYING(2)` 这样的窄类型。数组列不参与 zone map 剪枝。
- `ARRAY_CONTAINS` 在元素列上求值：按 `starts` 的区间找每行是否有元素相等，不在行上重建 `ValueArray`；`INT`/`BIGINT`/`DOUBLE`/`VARCHAR` 元素列走原始数组，字典元素列先在字典上算出匹配位图，其余类型逐元素 `compareTo`。语义按 H2：数组为 NULL 时结果是 NULL（`ARRAY_CONTAINS` 与 `NOT ...` 两种情况都不命中），非数组首参也返回 NULL。
- 字典向量在建段时按基数决定：`VARCHAR` 与低基数数值列尝试建字典，容量上限 `min(4096, max(64, 行数 / 4))`，超出则退回普通向量。
- 列编码不新增向量类型、只在 `ColumnVector` 内部换表示：向量化路径对数值列有 61 处 `instanceof XxxVector` 分发并直接读原始数组，新增类型要补全部分支，且位打包数组上逐行随机解包比原始数组更慢。因此 `IntVector`/`LongVector`/`TimestampVector`（只打包 dateValue；timeNanos 约 37 位超上限，保留原数组）与两个字典向量的码是双形态——可写态原始数组（开放段、合并回写、`ValueCodec` 读回）与编码态 `BitPacked`（冻结段）；逐行访问走单点读 `longAt`/`codeAt`/`dateAt`（一次移位一次与），整列一次性消费（序列化、建集合、哈希预计算）走 `raw*()` 整列解包且不缓存。编码在 `ColumnSegment.build()` 字典判定之后与 `applyDelta()` 末尾按列选择：按最大值位宽打包（全同值列基数 1 必先走单值字典，没有常量特例），编码后字节数不比原始省 15% 以上、含负值（按非负最大值定位宽，负值使位宽为 0）或位宽 > 32 即退回原数组；合并产出的可写态在收尾重新选编码。不做差分编码：差分态的随机单点读要 O(index) 累加，而向量化路径按选择向量随机访问占多数。磁盘格式 v3 不变，`ValueCodec` 写盘前整列解包。
- 行式路径的区间剪枝只作用于没有未回写更新新像的段：段的 zone map 可能落后于 delta 的新像（重开后 WAL 重放把更新放回 delta 的窗口，或合并阈值未触发的窗口），被剪段内的 delta 新像会随段一起丢。删除只会收窄值域，不改变 zone map 的有效性，纯删除段仍参与剪枝（`updatedSegments` 判定）。向量化路径不受此影响（`extraRows` 无条件保留）。
- delta rowset 的列向量只在锁内可读：`upsert` 追加与容量翻倍都会替换或改动向量内部数组，无锁读者可能看到未完整发布的向量（实测在并发合并用例里解码出负长度）。因此向量化快照在读锁内物化活像，合并第一阶段在写锁内顺带物化；位置不可变只是让锁外的「按位置双重比对」不需要值引用比对，不意味着内容可以无锁读。
- 段级逐出按段级 LRU（未访问段按段号老优先、访问过的按访问序）而不是固定段号序：时间滚动场景下自然退化为逐出最老段，随机访问场景下被逐出的才是真冷段；逐出后段仍可见，行数、删除位图与每列 zone map 留在堆上的骨架里，因此逐出语义不复用惰性加载的「`loaded == null` 即不可见」标记，需要段内容的读路径同步读回。
- 骨架元数据（行数、已删行数、删除位图、每列 zone map）随 manifest 持久化：惰性加载与驻留预算同时开启时，未加载段必须既可见又可剪枝，预载停止后预算外的段不能静默丢数据；无持久化的内存库没有 manifest，未加载段回退旧惰性语义。
- 段文件重设计为「尾部列目录」单格式：头部记目录偏移，列块沿用 VEC_* 编码顺序存放，目录每项含列偏移、长度与 zone map；全量读免全列重算，`readSegmentColumns` 支持按列定位的部分读，返回不进驻留模型的 `ColumnSlice`。旧 v1 段文件与 v1 manifest 回退分支随格式重设计删除，旧数据靠重新灌入而非迁移。
- 预载的停止条件（取段前驻留量达预算即停、本次发布触发任何预算逐出即停）是内容寻址的：不依赖段号顺序，预算内的新段照常预载，预算外只在并发窗口内多读「并发数 - 1」个装不下的段，老段等查询命中时带访问标记读回；并发发布下「自己发布的段被逐出」不再可靠，停止信号取发布路径在写锁内观测到的任意逐出。
- 裸 MIN/MAX 的定界用按 zone 边界升序扫描候选段、命中即停的迭代法，而不是一次取全局最优边界，因为候选段内最优值所在行可能已被删除、一次性全局边界会算错，迭代法在任何删除形态下都精确。
- 向量化查询命中逐出段时按段文件列目录只读所需列集，返回不进驻留模型的临时 `ColumnSlice`，不再整段读回驻留，预算稳态不被冷查询搅动；行式路径不受此影响，仍读回并驻留。
- 驻留预算是库级共享账本而非表级 WITH 参数：`SET OMAP_RESIDENT_BYTES` 的值持久化进 H2 目录 `Setting`，建表时经 `OmapResidentBudgets` 读同一值，重放之后建的表共享同一预算；逐出判定在各表自己的写锁内只能逐本表的段，预算因此是软上限（超出量以长期无写入的冷表持量为上界，硬上限需要跨表锁或后台线程，违反现有约定）。全局占用是无锁 `AtomicLong` 账本、各表写锁内经 `residentAdd` 统一入口增量维护：账本求和不得逐个加其他表的锁，跨表锁序在两表并发发布下死锁（已实证并被「共享预算多表预载」用例覆盖）。旧目录里的表级 `WITH "residentBytes=..."` 保留解析但忽略，`ALTER TABLE SET RESIDENT_BYTES` 命令与 `Parser` 对应分支删除（缩小 H2 升级比对面）。预载只在启动时执行，预算调大不重启预载线程，由后续查询自然填驻留。
- 冻结同步在 `append` 的写锁内完成，不交给后台线程。
- 合并采用「就地写回」而不是「写新段」：段内保留删除位图，delta 的新值写回列向量，rowId 全程不变，因此不需要段集合快照与引用计数。
- 合并分三步：写锁内按段归组 delta 改动并登记段引用（不读回逐出段），锁外读回要重写的逐出段并 `applyDelta` 生成新段（Copy-On-Compact，先前取到的迭代器仍读旧段），写锁内按 generation 逐段替换段引用并按「rowId 加值」双重比对移除 delta 条目；被摘除或代数已变的段只跳过、不整轮放弃。逐出段在读回时不持表写锁：现网驻留预算 4 GiB 小于数据量 10.6 GiB 时，合并曾在写锁内同步读回被逐出的段，DELETE 清理任务触发的合并把本表写入冻结数十秒，9 个 INSERT 会话排队在合并上并占满连接池（2026-11-27 定位报告）。
- 合并由写路径同步触发（`append`/`remove`/`update` 后判断 delta 大小），不交给后台线程；已有合并在进行时触发方直接返回，delta 留给下一次写触发，写线程不在合并上排队。
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
- 磁盘格式演进走「版本化 + 启动时自动升级写回」，不以删除 `.omap` 目录重灌数据作为迁移手段；manifest 版本是全局升级不变量（新版本 manifest ⇒ 全部活跃段文件已同版本，升级整体跳过），升级失败按段号最小者拒绝开库，崩溃重入靠「段文件已最新 ⇒ 只读尾部目录回填」幂等。
- 每列 hasNull 与 zone map 同构地在段文件尾部目录与 manifest 骨架中维护：有序流式的 claim 时刻必须零 I/O 证明未读段里没有 NULL 行，而 zone map 对 NULL 不可见、不携带 NULL 存在性；判定依据因此是段内容（hasNull）而非列声明（NOT NULL）。
- 无 `LIMIT` 的 `ORDER BY` 优化走分段有序归并流式（M20/M22），不给排序列建二级索引：数千万行的排序列索引要占数 GB 堆，且 `append` 从 O(1) 退化为 O(log N)，会摧毁写路径 O(1) 与内存设计的前提。触发前提是排序首列段间 zone 区间次邻严格分离（M22 起；相邻段允许边界相交，相交带行归入下一发射单元一起归并）且快照内全部参与段首列 `hasNull == false`（M21 起按骨架内容判定）；次邻区间相接/相交或任一段首列实际含 NULL 时整条查询交回 H2 行式路径。现网实测（868 对相邻段全部亚秒级相交、次邻 0 对相交、最大重叠带 12 秒 / 段跨 90 秒）证明相邻相交是写入常态，严格两两不相交的门控在真实数据上永不命中——这是 M22 的立项依据。排序阶段每段只读「排序键 ∪ 谓词列」，输出流只携带复合键与 rowId，投影列在该行真正发出时才读回（延迟物化），被谓词整段过滤空的段不产生投影列 I/O。投影含表达式时求值同样发生在发射期：排序阶段不受投影影响，表达式引用列并入延迟读集，行发出时经 `TableFilter.set(Row)` 就位后逐行求值；非确定表达式与顶层子查询交回 H2。段间顺序只由首列决定，多列排序在段内用复合键比较器、键并列时 rowId 定序。
- 段间无序的 `ORDER BY` 走独立的 k 路归并流式（M24），与 M20 免归并路径并列：zone 次邻分离时段序已知、免归并；段序未知时输出下一行取决于全体参与段的当前头，必须真归并。采用简单版口径：全体参与段键流一次性驻留，键流量按 `Value.getMemory()` 精确记账，超过 `omap.mergeStreamMaxBytes`（默认 2 GiB）即拒绝并交回 H2，不实现惰性物化与二分重放——现网键流量（LOGID ~66 MB、KEY1 ~165 MB / 1.1 GB）都落预算内；有界版（骨架 zone-min 作零 I/O 下界种子 + 占位头成堆顶才物化真头 + 按 rowId 二分重放）留作表增长后的升级项。归并模式没有 hasNull 门控：NULL 组在同一个堆里由比较器全局定序，不存在流式单元推进拆散 NULL 组的问题。
- 无 `LIMIT` 无 `ORDER BY` 的平面查询走 M23 平面流式执行，不改默认 `LAZY_QUERY_EXECUTION`（会话级霰弹枪：把该会话全部查询的结果缓存复用下线，且默认值硬编码在 `SessionLocal` 字段初值，替换面要扩张到事务核心类）；且本项目替换后的 `queryWithoutCache()` 里 `tryExecuteFlatStreaming` 分支排在 H2 的 `queryFlat()`（lazy 路径）之前，单表无谓词的 `SELECT *` 命中本项目的平面执行器，永远轮不到 H2 的 `LazyResult`。流式发射顺序沿用 M16 无序 LIMIT 的「先驻留后逐出」口径：依赖插入序翻页的查询应显式 `ORDER BY` 走 M20/M22。行式全表扫描对逐出段「每执行一次读一遍整表」的形态（`SELECT * FROM POLICY_OW`，829 万行 5.5 GB、预算 4 GiB 下 93 秒）由本入口消除：首行延迟等于首个被触碰段的读回时间，客户端取 100 行即停只读一个段。
- 向量化快照的 `extraRows` 是按数据列索引的行，不含 0 号 rowId；活像与开放段行的 rowId 由快照携带的 `extraRowIds` 对齐数组提供，取代旧实现从 `srcRow[0].getLong()` 反解的口径（那是首列数据的值，列 0 为数值列时会产生错误并列定序，该缺陷随 M20 一并修复）。
- 性能数字记录在 `BENCHMARK_RESULT.md` 与 `problems/查询性能优化方向.md`；回归只要求集成测试与内部计数器断言。

## 尚未实现

- 并发写的后台合并线程：合并现在由写路径同步触发。
- M12 已全部交付（批次 1 删除位图与空段丢弃、批次 2 delta rowset、批次 3 段级部分聚合缓存、批次 4 段级 LRU 逐出）；驻留预算按估算内存而非精确占用，重开库后访问热度清零、按段号老优先。
- 段级部分聚合缓存不覆盖 `COUNT(DISTINCT)`、多列分组、表达式分组键与投影/JOIN 里的子查询；这些形态照常逐段扫描。
- 数组列的剪枝与按元素过滤：数组列不参与 zone map 剪枝（算值域要重建每行 `ValueArray`，而数组值域几乎不能用于跳段），`UNNEST` 与按元素过滤也没有下推路径。
- `collectVisibleRowIds` 的数组按 `rowCount` 估计值起步并在超出时扩容，因此估计值与实际可见行数不一致时不会越界；但行数仍不可能超过 `Integer.MAX_VALUE`（`long[]` 的长度上限）。
- 向量化路径对 `delta` 新像与开放段的行逐行求值，这些行不参与按列扫描。
- 位打包只覆盖非负、位宽 ≤ 32 的 `INT`/`BIGINT`/`TIMESTAMP` dateValue 列与字典码；含负值的数值列、位宽 > 32 的列、`DOUBLE`、`timeNanos`、`ValueVector` 保持原始数组。列块压缩、段内分页与浮点编码是列编码与压缩计划的后续阶段（B/C/D），未开始。
- `GROUP BY` 的键必须是单表列引用，表达式分组键不下推。
- 多表连接条件下的下推。子查询下推只覆盖单表 `WHERE`：投影里的子查询、JOIN 条件里的子查询、多表查询里的关联子查询仍走 H2 行式路径。
- 关联 `IN (SELECT ...)` 的投影为表达式时不折叠（内层投影限列引用）；关联子查询的关联键限单列等值，多关联条件、关联键为表达式、内层 `HAVING`/`GROUP BY`/`DISTINCT`/窗口、内层非 OMAP 表均回退。
- 关联聚合预计算只接受 `COUNT(*)`/`COUNT`/`SUM`/`MIN`/`MAX`/`AVG` 且 `SUM`/`AVG` 参数为整数列；浮点与十进制求和口径、`COUNT(DISTINCT)`、聚合参数为表达式时不折叠。
- `ILIKE`、`REGEXP` 等谓词形态未下推。
- 锚定前缀 `LIKE 'x%'` 在向量化路径里仍逐行 `getString` 装箱后交给 `CompareLike.test`（每次加锁），未在打包字节池上直接比较前缀；`LIKE` 只参与列裁剪，不提供列值区间，因此也不参与段级剪枝，前缀查询要读全部段的该列。
- 查询期 IO 只做采集与回传（见「查询期段文件 IO 统计」）；并发查询的 IO 资源分配（配额、请求合并、优先级）未做，当前每个查询线程各自同步直读。
- 平面流式执行（M23）不接受的形态：带 `LIMIT`（走 `tryExecuteFlat` 的 M11/M16 路径）、带 `ORDER BY`（走 M20/M22，区间相交或含 NULL 时交回 H2 全量物化排序）、`DISTINCT`、聚合、`FOR UPDATE`、多表连接；投影含顶层子查询或非确定表达式交回 H2；投影列全被主键索引覆盖时 H2 选 rowId 索引而非扫描索引，同样交回。
- 有序流式执行（M20/M22）不接受的形态：排序键为表达式、投影含顶层子查询或非确定表达式（普通表达式在发射期逐行求值）、投影引用 `_ROWID_` 等伪列、首列实际含 NULL、次邻区间相接或相交——这些先落归并流式（M24）；归并自身不接受：带 `LIMIT` 或 `WITH TIES`、多表连接与窗口查询、键流量超过 `omap.mergeStreamMaxBytes`、排序键为表达式与投影含顶层子查询/非确定表达式/伪列（与有序流式同一套投影门控）；两道流式都不命中才交回 H2 行式路径，无 `LIMIT` 时仍是全表物化排序。流式游标的 `reset()` 经一次性验证程序实测不可达（H2 对 lazy 结果不复用），属防御性实现。

## 待核对

基线是 `2.5.252`，`Table`、`Index`、`Cursor` 的签名已逐条用 `javap` 核对。剩下几条：

- `CreateTableData` 位于 `org.h2.command.ddl`，却被 `org.h2.api.TableEngine` 的签名引用。这是事实上的公开接口，H2 升级时需要复核。
- `OmapTableEngine` 不解析 schema 前缀：目录名取 `data.schema.getName()`，表名里的 schema 由 H2 在调用引擎之前解析完。
- 文中 H2 行号已按 `.h2/` 逐条核对（该目录为 release zip 的 `h2/src/`，不入版本控制，获取方式见「工作约定」）；H2 版本固定为 `2.5.252`，换版本后需要重核。
- `Comparison` 的替换只为公开 `getCompareType()`。若上游补上同名方法，删除本地副本并同步 shade 排除清单。
- `ConditionInQuery` 的替换按 `2.5.252` 源码逐字合入后追加四个访问器；升级时除比对新增部分外，还要确认上游本体无改动（尤其 `getValue` 的 NULL 语义与 `createIndexConditions`）。
- `Command` 只需比对 `executeQuery()` 的包国位置（开头 `begin()`、最外层 finally `end()`）；`JdbcStatement` 只需比对 `getMoreResults()` 与静态列定义。两者其余部分逐字合入，升级时先确认上游本体未变。

## 参考

- H2 架构文档：`.h2/docsrc/html/architecture.html`（第 7 到 8 层已过时，PageStore 已删除）
- MVStore 设计：`.h2/docsrc/html/mvstore.html`
- 外部表引擎样例：`.h2/test/org/h2/test/db/TestTableEngines.java`
- 量化对比与优化记录：`BENCHMARK_RESULT.md`、`problems/查询性能优化方向.md`
- 标量子查询的定位记录与当前限制：`problems/子查询暂不支持.md`（文件名沿用问题发现时的判断；正文记录两轮修复：结果集复用与子查询下推，以及仍未覆盖的形态）
- 列编码与压缩的分阶段实施计划（阶段 A 已交付，阶段 A 的实际取舍以本文「已确认的设计决策」为准）：`problems/列编码与压缩实施计划.md`

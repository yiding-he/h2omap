# OMAP（On Memory Analyze Processing）

基于 H2 的纯内存列式存储。走 JDBC，不做事务，面向 OLAP，同时要接住高频细粒度变更。

本目录是独立 Maven 模块，通过 `org.h2.api.TableEngine` 接入 H2，**不修改 H2 源码**。

| 需求 | 结论 | 谁来做 |
| --- | --- | --- |
| 纯内存 | H2 已支持 | 本项目只管理自己的内存结构 |
| JDBC 兼容 | 完全复用 H2 | `org.h2.jdbc` 直接可用，不用重写 |
| 列式存储 | 需要实现 | 本项目 |
| OLAP 扫描 | 需要实现 | 列段、zone map、迟物化 |
| 高频细粒度变更 | 需要实现 | 行式 delta 缓冲加定期合并 |
| 事务 | 不支持 | 见 `AGENTS.md` 的「无事务的三个坑」 |

一句话：H2 提供 SQL 前端、JDBC 和元数据，本项目只替换表的存储与索引。

## 构建

```
mvn -q package
```

依赖 `com.h2database:h2`，版本固定在 Maven Central 的 `2.5.252`。测试用 JUnit 5，`mvn test` 运行。

## 用法

```
-- 单表指定引擎
CREATE TABLE EVENTS(TS TIMESTAMP, HOST VARCHAR, COST DOUBLE)
  ENGINE "com.hyd.h2omap.engine.OmapTableEngine"
  WITH "segmentSize=65536"
```

`ENGINE` 和 `WITH` 的参数都必须是双引号标识符。单引号是语法错误（`Parser.readIdentifier()` 只接受标识符），不加引号会被当成标识符并转成大写，导致 `ClassNotFoundException`。

使用 `ENGINE` 需要管理员权限（`command/ddl/CreateTable.java:76-78`）。

表定义会随元数据持久化，数据不会。纯内存场景下这是预期行为，重启后需要重新导入。

## 已知限制

- **`DEFAULT_TABLE_ENGINE` 不能用**：它会让 `Schema.createTable` 把 H2 自己的元数据表也交给本引擎（`schema/Schema.java:795`），而元数据必须落在 MVStore 里，实测开库直接抛 `Unexpected code path`。它也只能通过 JDBC URL 参数设置，`2.5.252` 没有 `SET DEFAULT_TABLE_ENGINE` 命令。
- **列式结构已生效，但优化未做**：写入达到 `segmentSize` 后冻结成不可变列段，读路径先段后 delta；zone map 剪枝、合并与字典编码都还没实现。
- **没有事务**：失败的部分更新不会回滚，并发写由 delta 内部加锁保证。

## 设计与实现

架构、与 H2 的契约、必须遵守的硬性约束、模块职责、当前状态与后续阶段，见 [`AGENTS.md`](AGENTS.md)。

# OMAP（On Memory Analyze Processing）

基于 H2 的纯内存列式存储。走 JDBC，不做事务，面向 OLAP，同时要接住高频细粒度变更。

本分支在 H2 `2.5.252` 基础上局部替换少数类以支持向量化下推与默认 OMAP 引擎，并通过 `maven-shade-plugin` 将修改后的类与 `h2-2.5.252` 合并为单个 Jar，消除运行时类加载顺序冲突。

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

### 单独运行

本项目一开始的设计目标就是单独运行而非嵌入其他 java 应用。

在构建阶段， `mvn -q package` 通过 `maven-shade-plugin` 产出合并后的单 Jar `target/h2omap-0.1.0-SNAPSHOT.jar`：已内置 H2 `2.5.252` 并剔除被本项目替换的原版同名类，**部署时只需这一个 Jar，不要再额外引入 `h2-2.5.252.jar`**（避免原版 Jar 因文件名排序靠前而被优先加载）。

```bash
CP="target/h2omap-0.1.0-SNAPSHOT.jar"
```

下面三种方式的 `CP` 都是这一串。

命令行 Shell，交互或一次执行：

```bash
java -cp "$CP" org.h2.tools.Shell -url "jdbc:h2:~/omap-demo" -user sa
java -cp "$CP" org.h2.tools.Shell -url "jdbc:h2:mem:omap-demo" -user sa \
  -sql "CREATE TABLE EVENTS(TS TIMESTAMP, HOST VARCHAR, COST DOUBLE) ENGINE \"com.hyd.h2omap.engine.OmapTableEngine\" WITH \"segmentSize=65536\";"
```

Web Console 与 TCP 服务器，浏览器开 `http://localhost:18082`，或用 TCP 连接：

```bash
java -cp "$CP" org.h2.tools.Server -tcp -tcpPort 19092 -web -webPort 18082 -ifNotExists
java -cp "$CP" org.h2.tools.Shell -url "jdbc:h2:tcp://localhost:19092/~/omap-demo" -user sa
```

`-ifNotExists` 必须加：H2 服务器默认禁止远程建库，不加时客户端首次访问会报 `Database "..." not found`。

客户端通过 `jdbc:h2:tcp://...` 连接时，**只需在客户端引入标准 H2 驱动（`com.h2database:h2`）即可，无需引入本项目 Jar**（存储引擎与向量化执行均运行在服务端进程内）。

自己写的 Java 程序：

```bash
javac -cp "$CP" -d out Demo.java
java -cp "$CP:out" Demo
```

注意：

- `jdbc:h2:mem:名字` 只在当前进程内有效，进程退出即消失（除非建表时显式指定 `WITH "backupDir=/path"`）；`jdbc:h2:~/名字` 为文件模式库，会在磁盘保存表定义（`.mv.db`）和 OMAP 表数据（`<databasePath>.omap/`，即 WAL 与列段快照），重启后自动将表数据恢复到内存。如果不想在重启后自动恢复某张表的数据，可以在建表语句中使用参数 `WITH "backup=false"`。

### 建表语句

普通用户表（非 `SYS`、非临时表）默认使用 OMAP 引擎；如需显式调整 `WITH` 参数或指定走 MVStore：

```sql
-- 默认即 OMAP 引擎
CREATE TABLE EVENTS(TS TIMESTAMP, HOST VARCHAR, COST DOUBLE);

-- 显式指定参数
CREATE TABLE EVENTS_CUSTOM(TS TIMESTAMP, HOST VARCHAR, COST DOUBLE)
  ENGINE "com.hyd.h2omap.engine.OmapTableEngine"
  WITH "segmentSize=65536";

-- 显式回退原版 MVStore 引擎
CREATE TABLE EVENTS_MV(TS TIMESTAMP, HOST VARCHAR, COST DOUBLE)
  ENGINE "org.h2.mvstore.db.Store";
```

`ENGINE` 和 `WITH` 的参数都必须是双引号标识符。单引号是语法错误（`Parser.readIdentifier()` 只接受标识符），不加引号会被当成标识符并转成大写，导致 `ClassNotFoundException`。

使用 `ENGINE` 需要管理员权限（`command/ddl/CreateTable.java:76-78`）。

表定义随 H2 元数据（`.mv.db`）持久化；在文件模式库（如 `jdbc:h2:/path/to/db`）下，OMAP 表数据默认同步写入 `<databasePath>.omap/<schema>_<table>/`（WAL + 增量列段快照），重启后自动加载回内存。纯内存模式（`jdbc:h2:mem:...`）默认不写盘；可通过 `WITH "backup=false"` 关闭持久化，或通过 `WITH "backupDir=/path"`、`WITH "syncIntervalMs=200"` 调整存储路径与 WAL 刷盘间隔。

## 已知限制

- **`DEFAULT_TABLE_ENGINE` 不能用**：它会让 `Schema.createTable` 把 H2 自己的元数据表也交给本引擎（`schema/Schema.java:795`），而元数据必须落在 MVStore 里，实测开库直接抛 `Unexpected code path`。它也只能通过 JDBC URL 参数设置，`2.5.252` 没有 `SET DEFAULT_TABLE_ENGINE` 命令。
- **列式结构已生效，部分优化未做**：写入达到 `segmentSize` 后冻结成列段，读路径先段后 delta；zone map 剪枝与合并已实现，字典编码、列裁剪与后台合并线程未做。
- **没有事务**：失败的部分更新不会回滚，并发写由 delta 内部加锁保证。

## 设计与实现

架构、与 H2 的契约、必须遵守的硬性约束、模块职责、当前状态与后续阶段，见 [`AGENTS.md`](AGENTS.md)。

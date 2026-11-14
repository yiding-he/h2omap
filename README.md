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

### 单独运行

`mvn -q package` 产出的是普通 jar `target/h2omap-0.1.0-SNAPSHOT.jar`：不含 H2 依赖，也没有 `Main-Class`，因此 `java -jar` 跑不起来。要把本项目的 jar 与 H2 的 jar 一起放进 classpath，再指定 H2 自带的启动类。

```bash
# Linux / macOS；Windows 把 classpath 的分隔符 ":" 换成 ";"
H2_JAR="$HOME/.m2/repository/com/h2database/h2/2.5.252/h2-2.5.252.jar"
CP="target/h2omap-0.1.0-SNAPSHOT.jar:$H2_JAR"
```

H2 的 jar 在执行 `mvn -q package` 时已落到本地 Maven 仓库；也可以直接从 Maven Central 取 `https://repo1.maven.org/maven2/com/h2database/h2/2.5.252/h2-2.5.252.jar`。下面三种方式的 `CP` 都是这一串。

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

自己写的 Java 程序：

```bash
javac -cp "$CP" -d out Demo.java
java -cp "$CP:out" Demo
```

两个容易踩的点：

- 引擎类由 H2 反射加载，本项目的 jar 不在 classpath 时报 `Class "com.hyd.h2omap.engine.OmapTableEngine" not found`（SQL 错误码 90086），不是引擎实现本身出错。
- `jdbc:h2:mem:名字` 只在当前进程内有效，进程退出即消失；`jdbc:h2:~/名字` 会落盘，但 OMAP 表的数据不随元数据持久化，重启后仍需重新导入。

## 已知限制

- **`DEFAULT_TABLE_ENGINE` 不能用**：它会让 `Schema.createTable` 把 H2 自己的元数据表也交给本引擎（`schema/Schema.java:795`），而元数据必须落在 MVStore 里，实测开库直接抛 `Unexpected code path`。它也只能通过 JDBC URL 参数设置，`2.5.252` 没有 `SET DEFAULT_TABLE_ENGINE` 命令。
- **列式结构已生效，部分优化未做**：写入达到 `segmentSize` 后冻结成列段，读路径先段后 delta；zone map 剪枝与合并已实现，字典编码、列裁剪与后台合并线程未做。
- **没有事务**：失败的部分更新不会回滚，并发写由 delta 内部加锁保证。

## 设计与实现

架构、与 H2 的契约、必须遵守的硬性约束、模块职责、当前状态与后续阶段，见 [`AGENTS.md`](AGENTS.md)。

/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * M0：H2 接入契约的集成测试。
 *
 * <p>覆盖 {@code OmapTable} 与两个索引在真实调用下的语义，
 * 这些结论无法靠阅读 H2 源码或编译通过来确认。
 */
class OmapTableContractTest {

    private static final String ENGINE = "com.hyd.h2omap.engine.OmapTableEngine";

    private static final AtomicInteger SEQ = new AtomicInteger();

    private Connection conn;

    private String dbName;

    @BeforeEach
    void setUp() throws SQLException {
        dbName = "mem:omap" + SEQ.incrementAndGet() + ";DB_CLOSE_DELAY=-1";
        conn = DriverManager.getConnection("jdbc:h2:" + dbName, "sa", "");
    }

    @AfterEach
    void tearDown() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("SHUTDOWN");
        } catch (SQLException ignore) {
            // SHUTDOWN 后连接已关闭，属预期。
        }
        conn.close();
    }

    // ------------------------------------------------------------------
    // 基本路径
    // ------------------------------------------------------------------

    @Test
    void createInsertSelect() throws SQLException {
        execute(createTable("T", "ID INT, NAME VARCHAR, COST DOUBLE", null));
        execute("INSERT INTO T VALUES (1, 'a', 1.5), (2, 'b', 2.5)");
        assertEquals(List.of("1/a/1.5", "2/b/2.5"), rows("SELECT ID, NAME, COST FROM T ORDER BY ID"));
        assertEquals("2", scalar("SELECT COUNT(*) FROM T"));
    }

    @Test
    void selectWithFilter() throws SQLException {
        execute(createTable("T", "ID INT, NAME VARCHAR", null));
        execute("INSERT INTO T VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        assertEquals(List.of("2/b"), rows("SELECT ID, NAME FROM T WHERE ID = 2"));
        assertEquals(List.of("2/b", "3/c"), rows("SELECT ID, NAME FROM T WHERE ID > 1 ORDER BY ID"));
        assertEquals("1", scalar("SELECT COUNT(*) FROM T WHERE NAME = 'c'"));
    }

    // ------------------------------------------------------------------
    // 变更路径
    // ------------------------------------------------------------------

    @Test
    void updateSingleRow() throws SQLException {
        execute(createTable("T", "ID INT, NAME VARCHAR", null));
        execute("INSERT INTO T VALUES (1, 'a'), (2, 'b')");
        execute("UPDATE T SET NAME = 'x' WHERE ID = 1");
        assertEquals(List.of("1/x", "2/b"), rows("SELECT ID, NAME FROM T ORDER BY ID"));
    }

    @Test
    void updateAllRows() throws SQLException {
        execute(createTable("T", "ID INT, NAME VARCHAR", null));
        execute("INSERT INTO T VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        execute("UPDATE T SET NAME = 'z'");
        assertEquals(List.of("1/z", "2/z", "3/z"), rows("SELECT ID, NAME FROM T ORDER BY ID"));
    }

    @Test
    void updateKeyColumn() throws SQLException {
        execute(createTable("T", "ID INT, NAME VARCHAR", null));
        execute("INSERT INTO T VALUES (1, 'a'), (2, 'b')");
        execute("UPDATE T SET ID = 9 WHERE ID = 1");
        assertEquals(List.of("2/b", "9/a"), rows("SELECT ID, NAME FROM T ORDER BY ID"));
    }

    @Test
    void deleteRows() throws SQLException {
        execute(createTable("T", "ID INT, NAME VARCHAR", null));
        execute("INSERT INTO T VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        execute("DELETE FROM T WHERE ID = 2");
        assertEquals(List.of("1/a", "3/c"), rows("SELECT ID, NAME FROM T ORDER BY ID"));
        execute("DELETE FROM T");
        assertEquals("0", scalar("SELECT COUNT(*) FROM T"));
    }

    @Test
    void truncateTable() throws SQLException {
        execute(createTable("T", "ID INT", null));
        execute("INSERT INTO T VALUES (1), (2), (3)");
        execute("TRUNCATE TABLE T");
        assertEquals("0", scalar("SELECT COUNT(*) FROM T"));
        execute("INSERT INTO T VALUES (7)");
        assertEquals(List.of("7"), rows("SELECT ID FROM T"));
    }

    // ------------------------------------------------------------------
    // 索引
    // ------------------------------------------------------------------

    @Test
    void primaryKeyLookup() throws SQLException {
        execute(createTable("T", "ID INT PRIMARY KEY, NAME VARCHAR", null));
        execute("INSERT INTO T VALUES (1, 'a'), (2, 'b')");
        assertEquals(List.of("2/b"), rows("SELECT ID, NAME FROM T WHERE ID = 2"));
        execute("UPDATE T SET NAME = 'y' WHERE ID = 2");
        assertEquals(List.of("2/y"), rows("SELECT ID, NAME FROM T WHERE ID = 2"));
        execute("DELETE FROM T WHERE ID = 1");
        assertEquals("1", scalar("SELECT COUNT(*) FROM T"));
    }

    @Test
    void primaryKeyDuplicateRejected() throws SQLException {
        execute(createTable("T", "ID INT PRIMARY KEY, NAME VARCHAR", null));
        execute("INSERT INTO T VALUES (1, 'a')");
        assertThrows(SQLException.class, () -> execute("INSERT INTO T VALUES (1, 'b')"));
        assertEquals("1", scalar("SELECT COUNT(*) FROM T"));
    }

    @Test
    void rowIdIndexServesRangeQueries() throws SQLException {
        execute(createTable("T", "ID INT PRIMARY KEY, V INT", null));
        for (int i = 1; i <= 10; i++) {
            execute("INSERT INTO T VALUES (" + i + ", " + i * 10 + ")");
        }
        assertEquals(List.of("4", "5", "6", "7", "8", "9", "10"),
                rows("SELECT ID FROM T WHERE ID > 3"));
        assertEquals(List.of("2", "3", "4"), rows("SELECT ID FROM T WHERE ID >= 2 AND ID <= 4"));
        assertEquals(List.of("1", "2"), rows("SELECT ID FROM T WHERE ID < 3"));
        assertEquals(List.of("10", "9", "8", "7", "6", "5", "4"),
                rows("SELECT ID FROM T WHERE ID > 3 ORDER BY ID DESC"));
        assertTrue(rows("SELECT ID FROM T WHERE ID > 100").isEmpty());
        // 结果断言走全表扫描也会通过，所以同时确认计划用的是 rowId 索引而不是扫描索引。
        String plan = rows("EXPLAIN SELECT ID FROM T WHERE ID > 3").get(0);
        assertTrue(!plan.contains("_SCAN"), plan);
    }

    @Test
    void uniqueConstraintRejectsDuplicates() throws SQLException {
        execute(createTable("T", "ID INT, V INT, CONSTRAINT U UNIQUE(V)", null));
        execute("INSERT INTO T VALUES (1, 10)");
        assertThrows(SQLException.class, () -> execute("INSERT INTO T VALUES (2, 10)"));
        assertEquals("1", scalar("SELECT COUNT(*) FROM T"));
    }

    @Test
    void uniqueConstraintAddedAfterData() throws SQLException {
        execute(createTable("T", "ID INT, V INT", null));
        execute("INSERT INTO T VALUES (1, 10), (2, 10)");
        // 已有重复数据时不能建立唯一约束。
        assertThrows(SQLException.class,
                () -> execute("ALTER TABLE T ADD CONSTRAINT U UNIQUE(V)"));
        execute("DELETE FROM T WHERE ID = 2");
        execute("ALTER TABLE T ADD CONSTRAINT U UNIQUE(V)");
        assertThrows(SQLException.class, () -> execute("INSERT INTO T VALUES (3, 10)"));
        assertEquals(List.of("1/10"), rows("SELECT ID, V FROM T"));
    }

    @Test
    void addIndexBackfillsAllRows() throws SQLException {
        execute(createTable("T", "ID INT, NAME VARCHAR", null));
        StringBuilder sql = new StringBuilder("INSERT INTO T VALUES ");
        for (int i = 1; i <= 1000; i++) {
            if (i > 1) {
                sql.append(", ");
            }
            sql.append('(').append(i).append(", 'n").append(i).append("')");
        }
        execute(sql.toString());
        execute("ALTER TABLE T ADD PRIMARY KEY(ID)");
        assertEquals("1000", scalar("SELECT COUNT(*) FROM T"));
        assertEquals("500/n500", scalar("SELECT ID || '/' || NAME FROM T WHERE ID = 500"));
        assertEquals("500", scalar("SELECT COUNT(*) FROM T WHERE ID > 500"));
        assertEquals("500", scalar("SELECT COUNT(*) FROM T WHERE ID <= 500"));
    }

    // ------------------------------------------------------------------
    // 生命周期
    // ------------------------------------------------------------------

    @Test
    void primaryKeyAddedAfterData() throws SQLException {
        execute(createTable("T", "ID INT, NAME VARCHAR", null));
        execute("INSERT INTO T VALUES (2, 'b'), (1, 'a')");
        execute("ALTER TABLE T ADD PRIMARY KEY(ID)");
        assertEquals(List.of("1/a"), rows("SELECT ID, NAME FROM T WHERE ID = 1"));
        assertThrows(SQLException.class, () -> execute("INSERT INTO T VALUES (1, 'x')"));
        assertEquals(List.of("1/a", "2/b"), rows("SELECT ID, NAME FROM T ORDER BY ID"));
    }

    @Test
    void dropTable() throws SQLException {
        execute(createTable("T", "ID INT", null));
        execute("INSERT INTO T VALUES (1)");
        execute("DROP TABLE T");
        assertThrows(SQLException.class, () -> scalar("SELECT COUNT(*) FROM T"));
    }

    @Test
    void settingsReportOmapAsDefaultTableEngine() throws SQLException {
        // 库刚打开、还没有任何表时就应该表达真实的默认引擎。
        assertEquals(ENGINE, scalar(
                "SELECT SETTING_VALUE FROM INFORMATION_SCHEMA.SETTINGS WHERE SETTING_NAME = 'DEFAULT_TABLE_ENGINE'"));
        // MV_STORE 不动：H2 的元数据确实在 MVStore 里。
        assertEquals("true", scalar(
                "SELECT SETTING_VALUE FROM INFORMATION_SCHEMA.SETTINGS WHERE SETTING_NAME = 'MV_STORE'"));
        // 只改展示值，建表路径仍然由 Schema.createTable 决定：不写 ENGINE 的普通表依旧走本引擎。
        execute("CREATE TABLE PLAIN(ID INT)");
        execute("INSERT INTO PLAIN VALUES (1)");
        assertEquals("1", scalar("SELECT COUNT(*) FROM PLAIN"));
        assertEquals(ENGINE, scalar(
                "SELECT SETTING_VALUE FROM INFORMATION_SCHEMA.SETTINGS WHERE SETTING_NAME = 'DEFAULT_TABLE_ENGINE'"));
    }

    @Test
    void settingsExposeOmapLazyLoadSwitch() throws SQLException {
        // 默认关闭，行为与未加开关时一致。
        assertEquals("false", scalar(
                "SELECT SETTING_VALUE FROM INFORMATION_SCHEMA.SETTINGS WHERE SETTING_NAME = 'OMAP_LAZY_LOAD'"));
        // 开关唯一的来源是服务端的系统属性，库打开时读一次。
        String key = "h2.omapLazyLoad";
        String previous = System.getProperty(key);
        System.setProperty(key, "true");
        try {
            String name = "mem:omapLazy" + SEQ.incrementAndGet() + ";DB_CLOSE_DELAY=-1";
            try (Connection c = DriverManager.getConnection("jdbc:h2:" + name, "sa", "")) {
                try (ResultSet rs = c.createStatement().executeQuery(
                        "SELECT SETTING_VALUE FROM INFORMATION_SCHEMA.SETTINGS"
                                + " WHERE SETTING_NAME = 'OMAP_LAZY_LOAD'")) {
                    assertTrue(rs.next());
                    assertEquals("true", rs.getString(1));
                }
                c.createStatement().execute("SHUTDOWN");
            }
        } finally {
            if (previous == null) {
                System.clearProperty(key);
            } else {
                System.setProperty(key, previous);
            }
        }
    }

    /**
     * 驻留预算开启时惰性预载不产生无效读：预载新→老装满预算即停，
     * 装不下的段不再预载（旧实现会每个段都读入又立即逐出）。
     * 骨架元数据随 manifest 持久化：重开后全部段立即可见且可剪枝，
     * 开库第一次 COUNT 就是全量；老段等查询命中时按需读回且驻留稳定。
     * 3 列表形每段约 15 KB，预算 40k 容最新两段、容不下三段。
     * 不带主键：避免 addIndex 回填在重开时逐段读回（那是索引正确性的必要读，
     * 与预载停止是两回事）。
     */
    /**
     * 查询期段文件 IO 统计随第二个结果集返回客户端。
     * segmentSize=4 产生 4 个冻结段；residentBytes=1 容不下任何段，全部逐出。
     * SUM(V) 命中逐出段时按需读回列切片，统计里应出现每次读取的字节数。
     */
    @Test
    void queryIoStatsReportsEvictedSegmentReads(@TempDir Path dir) throws Exception {
        boolean previous = com.hyd.h2omap.stats.QueryIoStats.isEnabled();
        com.hyd.h2omap.stats.QueryIoStats.setEnabled(true);
        try {
            String url = "jdbc:h2:" + dir.resolve("ioStats");
            try (Connection c = DriverManager.getConnection(url, "sa", "");
                    Statement s = c.createStatement()) {
                s.execute(createTable("T", "ID INT, V INT",
                        "\"segmentSize=4\", \"residentBytes=1\""));
                try (PreparedStatement ps = c.prepareStatement("INSERT INTO T VALUES (?, ?)")) {
                    for (int i = 0; i < 16; i++) {
                        ps.setInt(1, i);
                        ps.setInt(2, i);
                        ps.executeUpdate();
                    }
                }
                try (ResultSet rs = s.executeQuery("SELECT SUM(V) FROM T")) {
                    assertTrue(rs.next());
                    assertEquals(120L, rs.getLong(1));
                }
                assertTrue(s.getMoreResults(), "逐出段读回应产生 IO 统计");
                int rows = 0;
                long bytes = 0;
                long ioReads = 0;
                try (ResultSet stats = s.getResultSet()) {
                    while (stats.next()) {
                        rows++;
                        assertTrue(stats.getLong(4) > 0);
                        // 每段三次读：头部、尾部目录、列块，不存在大量碎片读。
                        int io = stats.getInt(6);
                        assertTrue(io >= 3, "每段读回应至少 3 次，实际 " + io);
                        bytes += stats.getLong(4);
                        ioReads += io;
                    }
                }
                assertTrue(rows >= 1);
                assertTrue(bytes > 0);
                assertTrue(ioReads >= 3L * rows);
                assertFalse(s.getMoreResults());
            }
        } finally {
            com.hyd.h2omap.stats.QueryIoStats.setEnabled(previous);
        }
    }

    @Test
    void lazyLoadStopsAtResidentBudget(@TempDir Path dir) throws Exception {
        String key = "h2.omapLazyLoad";
        String previous = System.getProperty(key);
        System.setProperty(key, "true");
        try {
            String url = "jdbc:h2:" + dir.resolve("lazyBudget");
            try (Connection first = DriverManager.getConnection(url, "sa", "")) {
                try (Statement s = first.createStatement()) {
                    s.execute(createTable("T", "ID INT, V VARCHAR(64), IV INT",
                            "\"segmentSize=50\", \"residentBytes=40k\""));
                    String pad = "v".repeat(60);
                    try (java.sql.PreparedStatement ps = first.prepareStatement(
                            "INSERT INTO T VALUES (?, ?, ?)")) {
                        for (int i = 0; i < 400; i++) {
                            ps.setInt(1, i);
                            ps.setString(2, pad + i);
                            ps.setInt(3, i);
                            ps.addBatch();
                        }
                        ps.executeBatch();
                    }
                }
            }
            // 重开：骨架随 manifest 恢复，行数立即是全量。
            try (Connection second = DriverManager.getConnection(url, "sa", "");
                    Statement s = second.createStatement()) {
                org.h2.engine.SessionLocal session = (org.h2.engine.SessionLocal)
                        ((org.h2.jdbc.JdbcConnection) second).getSession();
                com.hyd.h2omap.table.OmapTable table = (com.hyd.h2omap.table.OmapTable)
                        session.getDatabase().getMainSchema().findTableOrView(session, "T");
                com.hyd.h2omap.storage.ColumnarStore store = table.getStore();
                // 预载收尾：并发 4 路按段号从新到老读入，预算边界处的探针逐出 1..4 次；
                // 若预载不停止，会读完全部 8 段，探针逐出计数到 6。
                long deadline = System.currentTimeMillis() + 10_000;
                long evictedSeen = -1;
                long stableSince = -1;
                while (System.currentTimeMillis() < deadline) {
                    long evicted = store.getEvictedSegmentCount();
                    if (evicted != evictedSeen) {
                        evictedSeen = evicted;
                        stableSince = System.currentTimeMillis();
                    } else if (store.isSegmentResident(6) && evicted >= 1
                            && System.currentTimeMillis() - stableSince >= 500) {
                        break;
                    }
                    Thread.sleep(20);
                }
                long evicted = store.getEvictedSegmentCount();
                assertTrue(evicted >= 1
                                && evicted <= com.hyd.h2omap.persistence.OmapPersistence.LOAD_CONCURRENCY,
                        "预算边界的探针逐出应为 1..4 次，实测 " + evicted);
                assertTrue(store.isSegmentResident(7), "最新段应预载驻留");
                assertTrue(store.isSegmentResident(6), "次新段应预载驻留");
                assertFalse(store.isSegmentResident(5), "探针段被预算逐出，不应驻留");
                assertFalse(store.isSegmentResident(0), "停止条件之后的段不应被预载读入");
                // 预载确实经线程池并行执行：8 段至少起 4 个加载任务。
                assertTrue(table.getPersistence().getPoolTaskRuns()
                        >= com.hyd.h2omap.persistence.OmapPersistence.LOAD_CONCURRENCY,
                        "预载应经加载线程池执行");

                // 骨架可见：预载未完成时开库第一次 COUNT 就是全量。
                assertEquals("400", toString(s, "SELECT COUNT(*) FROM T"));
                assertEquals("79800", toString(s, "SELECT SUM(ID) FROM T"));

                // 全表聚合与目标查询都走部分列读：命中段不进驻留，驻留集保持预载后的两段。
                assertEquals("50", toString(s, "SELECT COUNT(*) FROM T WHERE ID < 50"));
                assertFalse(store.isSegmentResident(0), "部分列读不进驻留模型");
                assertEquals("v".repeat(60) + "59", toString(s, "SELECT V FROM T WHERE ID = 59"));
                assertFalse(store.isSegmentResident(0), "部分列读不进驻留模型");
                assertTrue(store.isSegmentResident(7), "预载驻留的最新段不受影响");
            }
        } finally {
            if (previous == null) {
                System.clearProperty(key);
            } else {
                System.setProperty(key, previous);
            }
        }
    }

    @Test
    void lazyLoadReopensFileDatabaseAndKeepsResults(@TempDir Path dir) throws Exception {
        String key = "h2.omapLazyLoad";
        String previous = System.getProperty(key);
        System.setProperty(key, "true");
        try {
            String url = "jdbc:h2:" + dir.resolve("lazy");
            try (Connection first = DriverManager.getConnection(url, "sa", "")) {
                try (Statement s = first.createStatement()) {
                    s.execute(createTable("T", "ID INT, V VARCHAR", "\"segmentSize=50\""));
                    try (PreparedStatement ps = first.prepareStatement(
                            "INSERT INTO T VALUES (?, ?)")) {
                        for (int i = 0; i < 400; i++) {
                            ps.setInt(1, i);
                            ps.setString(2, "v" + i);
                            ps.addBatch();
                        }
                        ps.executeBatch();
                    }
                    s.execute("UPDATE T SET V = 'x' WHERE ID < 100");
                    s.execute("DELETE FROM T WHERE ID >= 300");
                }
            }
            // 连接断开后库已关闭，这次打开走惰性加载：先只登记 manifest 与骨架，段在后台逐段读入。
            // 骨架随 manifest 恢复：未加载段也计入行数且可剪枝，开库第一次 COUNT 就是全量。
            try (Connection second = DriverManager.getConnection(url, "sa", "")) {
                try (Statement s = second.createStatement()) {
                    assertEquals("true", toString(s,
                            "SELECT SETTING_VALUE FROM INFORMATION_SCHEMA.SETTINGS"
                                    + " WHERE SETTING_NAME = 'OMAP_LAZY_LOAD'"));
                    // 加载未完成时客户端就能写入：新行进开放段，冻结时触发 checkpoint，
                    // 未加载的历史段不会被重写。
                    assertEquals("300", toString(s, "SELECT COUNT(*) FROM T"));
                    s.execute("INSERT INTO T VALUES (1000, 'new')");
                    for (int i = 1001; i <= 1059; i++) {
                        s.execute("INSERT INTO T VALUES (" + i + ", 'new')");
                    }
                    // 新写入的行立即计入；全量行数不再依赖后台加载进度。
                    long expected = 360;
                    long count = -1;
                    long deadline = System.currentTimeMillis() + 10_000;
                    while (System.currentTimeMillis() < deadline) {
                        count = Long.parseLong(toString(s, "SELECT COUNT(*) FROM T"));
                        if (count == expected) {
                            break;
                        }
                        Thread.sleep(20);
                    }
                    assertEquals(expected, count);
                    assertEquals("106620", toString(s, "SELECT SUM(ID) FROM T"));
                    assertEquals("100", toString(s, "SELECT COUNT(*) FROM T WHERE V = 'x'"));
                    assertEquals("v299", toString(s, "SELECT V FROM T WHERE ID = 299"));
                }
            }
        } finally {
            if (previous == null) {
                System.clearProperty(key);
            } else {
                System.setProperty(key, previous);
            }
        }
    }

    @Test
    void vectorizedBoundsOnUpdatedRowsMatchMvStore() throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:h2:mem:vdb_upd_bounds", "sa", "");
                Statement s = c.createStatement()) {
            s.execute("CREATE TABLE T(ID INT PRIMARY KEY, V INT)"
                    + " WITH \"segmentSize=50\", \"compactionMinRows=100000\"");
            s.execute("CREATE TABLE T_MV(ID INT PRIMARY KEY, V INT)"
                    + " ENGINE \"org.h2.mvstore.db.Store\"");
            for (int i = 1; i <= 200; i++) {
                s.execute("INSERT INTO T VALUES (" + i + ", " + i + ")");
                s.execute("INSERT INTO T_MV VALUES (" + i + ", " + i + ")");
            }
            // 66 行的 V 更新到 1000+，留在 delta 不触发合并；
            // 更新前段值域 1..200，新像值域 1003..1198。
            s.execute("UPDATE T SET V = 1000 + ID WHERE MOD(ID, 3) = 0");
            s.execute("UPDATE T_MV SET V = 1000 + ID WHERE MOD(ID, 3) = 0");

            // 下界 500：段被剪，新像与区间相交 → 66 行；旧位置被掩，不多计。
            assertEquals(countRows(s, "SELECT ID FROM T_MV WHERE V >= 500"),
                    countRows(s, "SELECT ID FROM T WHERE V >= 500"));
            assertEquals(66, countRows(s, "SELECT ID FROM T WHERE V >= 500"));

            // 上界 500：段参与但旧位置被掩，新像不过滤 → 134 行。
            assertEquals(countRows(s, "SELECT ID FROM T_MV WHERE V < 500"),
                    countRows(s, "SELECT ID FROM T WHERE V < 500"));
            assertEquals(134, countRows(s, "SELECT ID FROM T WHERE V < 500"));

            // 下界 2000：新像 zone map 也不相交 → 0 行。
            assertEquals(0, countRows(s, "SELECT ID FROM T WHERE V >= 2000"));
            assertEquals(toString(s, "SELECT SUM(V) FROM T_MV WHERE V >= 500"),
                    toString(s, "SELECT SUM(V) FROM T WHERE V >= 500"));
        }
    }

    @Test
    void fullyDeletedSegmentDroppedAndSurvivesRestart(@TempDir Path dir) throws Exception {
        String url = "jdbc:h2:" + dir.resolve("mordrop");
        try (Connection first = DriverManager.getConnection(url, "sa", "")) {
            try (Statement s = first.createStatement()) {
                s.execute(createTable("T", "ID INT, V VARCHAR",
                        "\"segmentSize=50\", \"compactionMinRows=100000\""));
                try (PreparedStatement ps = first.prepareStatement(
                        "INSERT INTO T VALUES (?, ?)")) {
                    for (int i = 0; i < 300; i++) {
                        ps.setInt(1, i);
                        ps.setString(2, "v" + i);
                        ps.addBatch();
                    }
                    ps.executeBatch();
                }
                assertEquals("300", toString(s, "SELECT COUNT(*) FROM T"));
                // 删掉中间一整段（ID 50..99 恰为段 1），再删掉段 0 的一半（不丢段）。
                s.execute("DELETE FROM T WHERE ID BETWEEN 50 AND 99");
                s.execute("DELETE FROM T WHERE ID < 25");
                assertEquals("225", toString(s, "SELECT COUNT(*) FROM T"));
            }
        }

        // 重开：全删段在 WAL 重放后整段摘除，段号空洞保留，幸存行不受影响。
        try (Connection second = DriverManager.getConnection(url, "sa", "")) {
            try (Statement s = second.createStatement()) {
                assertEquals("225", toString(s, "SELECT COUNT(*) FROM T"));
                assertEquals("40825", toString(s, "SELECT SUM(ID) FROM T"));
                assertEquals("v30", toString(s, "SELECT V FROM T WHERE ID = 30"));
                assertEquals("v100", toString(s, "SELECT V FROM T WHERE ID = 100"));
                assertEquals("0", toString(s, "SELECT COUNT(*) FROM T WHERE ID = 60"));
                assertEquals("0", toString(s, "SELECT COUNT(*) FROM T WHERE ID = 10"));
            }
        }

        // 再写入触发 checkpoint：被丢弃段不再进 manifest，其段文件被清理。
        try (Connection third = DriverManager.getConnection(url, "sa", "")) {
            try (Statement s = third.createStatement()) {
                s.execute("INSERT INTO T SELECT ID + 300, 'w' || ID FROM T");
                assertEquals("450", toString(s, "SELECT COUNT(*) FROM T"));
            }
        }
        Path tableDir = dir.resolve("mordrop.omap/PUBLIC_T");
        try (var files = java.nio.file.Files.list(tableDir)) {
            assertTrue(files.noneMatch(p -> p.getFileName().toString().startsWith("seg_1_")),
                    "被丢弃段的段文件应随 checkpoint 清理");
        }
    }

    @Test
    void tableDefinitionSurvivesRestart(@TempDir Path dir) throws SQLException {
        String url = "jdbc:h2:" + dir.resolve("restart");
        try (Connection first = DriverManager.getConnection(url, "sa", "")) {
            try (Statement s = first.createStatement()) {
                s.execute(createTable("T", "ID INT", null));
                s.execute("INSERT INTO T VALUES (1), (2)");
            }
        }
        try (Connection second = DriverManager.getConnection(url, "sa", "")) {
            try (Statement s = second.createStatement()) {
                assertEquals("2", toString(s, "SELECT COUNT(*) FROM T"));
                s.execute("INSERT INTO T VALUES (5)");
                assertEquals("3", toString(s, "SELECT COUNT(*) FROM T"));
            }
        }
    }

    @Test
    void orderByDoesNotFollowInsertionOrder() throws SQLException {
        execute(createTable("T", "ID INT, NAME VARCHAR", null));
        execute("INSERT INTO T VALUES (5, 'e'), (1, 'a'), (3, 'c')");
        assertEquals(List.of("1/a", "3/c", "5/e"), rows("SELECT ID, NAME FROM T ORDER BY ID"));
        assertEquals(List.of("5/e", "3/c", "1/a"), rows("SELECT ID, NAME FROM T ORDER BY ID DESC"));
    }

    @Test
    void orderByPrimaryKeyColumn() throws SQLException {
        execute(createTable("T", "ID INT PRIMARY KEY, NAME VARCHAR", null));
        execute("INSERT INTO T VALUES (5, 'e'), (1, 'a'), (3, 'c')");
        assertEquals(List.of("1/a", "3/c", "5/e"), rows("SELECT ID, NAME FROM T ORDER BY ID"));
    }

    @Test
    void failedInsertLeavesNoResidue() throws SQLException {
        execute(createTable("T", "ID INT PRIMARY KEY, NAME VARCHAR", null));
        execute("INSERT INTO T VALUES (1, 'a')");
        assertThrows(SQLException.class, () -> execute("INSERT INTO T VALUES (1, 'b')"));
        assertEquals("1", scalar("SELECT COUNT(*) FROM T"));
        assertEquals(List.of("1/a"), rows("SELECT ID, NAME FROM T"));
    }

    @Test
    void failedUpdateKeepsOldValue() throws SQLException {
        execute(createTable("T", "ID INT PRIMARY KEY, NAME VARCHAR", null));
        execute("INSERT INTO T VALUES (1, 'a'), (2, 'b')");
        assertThrows(SQLException.class, () -> execute("UPDATE T SET ID = 2 WHERE ID = 1"));
        assertEquals(List.of("1/a", "2/b"), rows("SELECT ID, NAME FROM T ORDER BY ID"));
    }

    // ------------------------------------------------------------------
    // 类型与可见性
    // ------------------------------------------------------------------

    @Test
    void typeRoundTrip() throws SQLException {
        execute(createTable("T", "I INT, L BIGINT, D DOUBLE, S VARCHAR, B BOOLEAN, N INT", null));
        execute("INSERT INTO T VALUES (1, 9223372036854775807, 3.5, 'x', TRUE, NULL)");
        assertEquals(List.of("1/9223372036854775807/3.5/x/TRUE/null"),
                rows("SELECT I, L, D, S, B, N FROM T"));
    }

    /**
     * H2 会缓存查询计划与结果结构，表必须通过
     * {@code getMaxDataModificationId} 与 {@code isDeterministic} 让缓存失效。
     */
    @Test
    void writesVisibleToRepeatedQueries() throws SQLException {
        execute(createTable("T", "ID INT", null));
        execute("INSERT INTO T VALUES (1)");
        assertEquals("1", scalar("SELECT COUNT(*) FROM T"));
        execute("INSERT INTO T VALUES (2)");
        assertEquals("2", scalar("SELECT COUNT(*) FROM T"));
        execute("DELETE FROM T WHERE ID = 1");
        assertEquals("2", scalar("SELECT ID FROM T"));
    }

    @Test
    void selectForUpdateDoesNotLockRows() throws SQLException {
        execute(createTable("T", "ID INT", null));
        execute("INSERT INTO T VALUES (1)");
        assertEquals(List.of("1"), rows("SELECT ID FROM T FOR UPDATE"));
    }

    @Test
    void concurrentInsertFromTwoSessions() throws SQLException {
        execute(createTable("T", "ID INT", null));
        try (Connection other = DriverManager.getConnection("jdbc:h2:" + dbName, "sa", "")) {
            execute("INSERT INTO T VALUES (1)");
            try (Statement s = other.createStatement()) {
                s.execute("INSERT INTO T VALUES (2)");
            }
        }
        assertEquals("2", scalar("SELECT COUNT(*) FROM T"));
    }

    /**
     * 主键区间查询在存储锁之外迭代索引，写入并发进行时不允许出错。
     *
     * <p>索引的迭代器是弱一致的：并发插入的行可能看不到，并发删除的行
     * 即使被迭代出来，{@code OmapCursor} 的物化也会返回 null 并跳过。
     * 因此这里只断言无异常、无重复 rowId，不断言与并发写入的一致快照。
     */
    @Test
    void rangeQueryToleratesConcurrentWrites() throws Exception {
        execute(createTable("T", "ID INT PRIMARY KEY, V INT", null));
        for (int i = 1; i <= 20; i++) {
            execute("INSERT INTO T VALUES (" + i + ", " + i + ")");
        }
        CountDownLatch stop = new CountDownLatch(1);
        List<String> failures = new ArrayList<>();
        Thread writer = new Thread(() -> {
            try (Connection c = DriverManager.getConnection("jdbc:h2:" + dbName, "sa", "");
                    Statement s = c.createStatement()) {
                for (int i = 0; i < 100 && stop.getCount() > 0; i++) {
                    s.execute("INSERT INTO T VALUES (" + (100 + i) + ", 0)");
                    s.execute("DELETE FROM T WHERE ID = " + (100 + i));
                }
            } catch (SQLException e) {
                failures.add(e.getMessage());
            }
        });
        writer.start();
        try {
            for (int round = 0; round < 10; round++) {
                List<String> ids = rows("SELECT ID FROM T WHERE ID > 0");
                assertEquals(ids.size(), new HashSet<>(ids).size(), "duplicate rowId: " + ids);
            }
        } finally {
            stop.countDown();
            writer.join();
        }
        assertTrue(failures.isEmpty(), String.join("; ", failures));
    }

    /**
     * 多线程并发扫描查询与高频写入/合并（Copy-On-Compact）并行：
     * 验证各读线程互不干扰、无异常，且同一行内不同列不会发生跨版本撕裂。
     */
    @Test
    void concurrentScanQueriesWithCompaction() throws Exception {
        execute(createTable("T", "ID INT PRIMARY KEY, CAT VARCHAR, VAL INT",
                "\"segmentSize=50\", \"compactionMinRows=20\""));
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO T VALUES (?, ?, ?)")) {
            for (int i = 1; i <= 2000; i++) {
                ps.setInt(1, i);
                ps.setString(2, "C0");
                ps.setInt(3, i);
                ps.addBatch();
            }
            ps.executeBatch();
        }

        int readerCount = 4;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch stopWriter = new CountDownLatch(1);
        List<String> failures = Collections.synchronizedList(new ArrayList<>());

        Thread writer = new Thread(() -> {
            try (Connection c = DriverManager.getConnection("jdbc:h2:" + dbName, "sa", "");
                    PreparedStatement upd = c.prepareStatement(
                            "UPDATE T SET CAT = ?, VAL = ? WHERE ID = ?");
                    Statement s = c.createStatement()) {
                start.await();
                for (int round = 1; round <= 40 && stopWriter.getCount() > 0; round++) {
                    String cat = "C" + (round % 5);
                    for (int id = 1; id <= 30; id++) {
                        upd.setString(1, cat);
                        upd.setInt(2, round * 100000 + id);
                        upd.setInt(3, id);
                        upd.addBatch();
                    }
                    upd.executeBatch();
                    s.execute("INSERT INTO T VALUES (" + (2000 + round) + ", '" + cat + "', "
                            + (round * 100000 + 2000 + round) + ")");
                    s.execute("DELETE FROM T WHERE ID = " + (2000 + round));
                }
            } catch (Exception e) {
                failures.add("writer: " + e.getMessage());
            }
        });

        List<Thread> readers = new ArrayList<>();
        for (int r = 0; r < readerCount; r++) {
            final int readerId = r;
            Thread reader = new Thread(() -> {
                try (Connection c = DriverManager.getConnection("jdbc:h2:" + dbName, "sa", "");
                        Statement s = c.createStatement()) {
                    start.await();
                    for (int iter = 0; iter < 30; iter++) {
                        if (readerId == 0) {
                            try (ResultSet rs = s.executeQuery(
                                    "SELECT ID, CAT, VAL FROM T WHERE VAL >= 0 ORDER BY ID DESC")) {
                                while (rs.next()) {
                                    int id = rs.getInt(1);
                                    String cat = rs.getString(2);
                                    int val = rs.getInt(3);
                                    int version = val / 100000;
                                    int encodedId = val % 100000;
                                    if (encodedId != id || !("C" + (version % 5)).equals(cat)) {
                                        failures.add("row tear id=" + id + " cat=" + cat + " val=" + val);
                                        break;
                                    }
                                }
                            }
                        } else if (readerId == 1) {
                            try (ResultSet rs = s.executeQuery(
                                    "SELECT COUNT(*), SUM(VAL) FROM T WHERE CAT = 'C0' AND VAL > 0")) {
                                assertTrue(rs.next());
                                assertTrue(rs.getLong(1) >= 0);
                            }
                        } else if (readerId == 2) {
                            try (ResultSet rs = s.executeQuery(
                                    "SELECT CAT, COUNT(*) FROM T GROUP BY CAT ORDER BY CAT")) {
                                long total = 0;
                                while (rs.next()) {
                                    total += rs.getLong(2);
                                }
                                assertTrue(total >= 2000);
                            }
                        } else {
                            try (ResultSet rs = s.executeQuery(
                                    "SELECT COUNT(*) FROM T WHERE CAT IN ('C0', 'C1', 'C2', 'C3', 'C4')")) {
                                assertTrue(rs.next());
                                assertTrue(rs.getLong(1) >= 2000);
                            }
                        }
                    }
                } catch (Exception e) {
                    failures.add("reader-" + readerId + ": " + e.getMessage());
                }
            });
            readers.add(reader);
            reader.start();
        }

        writer.start();
        start.countDown();
        for (Thread reader : readers) {
            reader.join();
        }
        stopWriter.countDown();
        writer.join();

        assertTrue(failures.isEmpty(), String.join("; ", failures));
    }

    // ------------------------------------------------------------------
    // M1：段真正产生
    // ------------------------------------------------------------------

    /**
     * 写满 segmentSize 就把开放段冻结，行数与扫描结果不能受影响。
     */
    @Test
    void insertAcrossSegmentBoundary() throws SQLException {
        execute(createTable("T", "ID INT, NAME VARCHAR", "\"segmentSize=2\""));
        execute("INSERT INTO T VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')");
        assertEquals("5", scalar("SELECT COUNT(*) FROM T"));
        assertEquals(List.of("1/a", "2/b", "3/c", "4/d", "5/e"),
                rows("SELECT ID, NAME FROM T ORDER BY ID"));
        assertEquals(List.of("5/e", "4/d", "3/c", "2/b", "1/a"),
                rows("SELECT ID, NAME FROM T ORDER BY ID DESC"));
    }

    /**
     * 冻结后 rowId 不变，点查、更新、删除都要还能定位到冻结前的行。
     */
    @Test
    void pointOperationsOnFrozenRows() throws SQLException {
        execute(createTable("T", "ID INT PRIMARY KEY, NAME VARCHAR", "\"segmentSize=2\""));
        execute("INSERT INTO T VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')");
        assertEquals(List.of("2/b"), rows("SELECT ID, NAME FROM T WHERE ID = 2"));
        execute("UPDATE T SET NAME = 'y' WHERE ID = 2");
        assertEquals(List.of("2/y"), rows("SELECT ID, NAME FROM T WHERE ID = 2"));
        execute("DELETE FROM T WHERE ID = 1");
        assertEquals(List.of("2/y", "3/c", "4/d"), rows("SELECT ID, NAME FROM T ORDER BY ID"));
        assertEquals("3", scalar("SELECT COUNT(*) FROM T"));
        assertThrows(SQLException.class, () -> execute("INSERT INTO T VALUES (3, 'x')"));
        assertEquals("3", scalar("SELECT COUNT(*) FROM T"));
    }

    /**
     * 全表扫描同时遍历已冻结的段与未冻结的开放段。
     */
    @Test
    void scanCoversFrozenAndOpenSegments() throws SQLException {
        execute(createTable("T", "ID INT, NAME VARCHAR", "\"segmentSize=2\""));
        execute("INSERT INTO T VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')");
        execute("DELETE FROM T WHERE ID = 2");
        execute("DELETE FROM T WHERE ID = 4");
        assertEquals(List.of("1/a", "3/c", "5/e"),
                rows("SELECT ID, NAME FROM T ORDER BY ID"));
        assertEquals("3", scalar("SELECT COUNT(*) FROM T"));
    }

    /**
     * 未特化的类型走 {@code ValueVector} 回退，同样能冻结成段。
     */
    @Test
    void unsupportedTypesStillFreezeIntoSegments() throws SQLException {
        execute(createTable("T", "TS TIMESTAMP, DEC DECIMAL(10,2), S VARCHAR", "\"segmentSize=2\""));
        execute("INSERT INTO T VALUES "
                + "(TIMESTAMP '2026-01-01 10:00:00', 12.34, 'a'),"
                + "(TIMESTAMP '2026-01-02 11:00:00', 56.78, 'b'),"
                + "(TIMESTAMP '2026-01-03 12:00:00', 90.12, 'c')");
        assertEquals("3", scalar("SELECT COUNT(*) FROM T"));
        assertEquals(List.of(
                        "2026-01-01 10:00:00/12.34/a",
                        "2026-01-02 11:00:00/56.78/b",
                        "2026-01-03 12:00:00/90.12/c"),
                rows("SELECT TS, DEC, S FROM T ORDER BY TS"));
        execute("UPDATE T SET S = 'x' WHERE TS = TIMESTAMP '2026-01-01 10:00:00'");
        execute("DELETE FROM T WHERE TS = TIMESTAMP '2026-01-02 11:00:00'");
        assertEquals(List.of("2026-01-01 10:00:00/12.34/x", "2026-01-03 12:00:00/90.12/c"),
                rows("SELECT TS, DEC, S FROM T ORDER BY TS"));
    }

    /**
     * zone map 剪枝：多段表按范围条件查询，结果只含命中的行。
     */
    @Test
    void zoneMapPruningReturnsOnlyMatchingRows() throws SQLException {
        execute(createTable("T", "ID INT, V INT", "\"segmentSize=2\""));
        execute("INSERT INTO T VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50), (6, 60)");
        assertEquals(List.of("3/30", "4/40"), rows("SELECT ID, V FROM T WHERE V >= 30 AND V < 50"));
        assertEquals(List.of("6/60"), rows("SELECT ID, V FROM T WHERE V > 50"));
        assertEquals(List.of("1/10"), rows("SELECT ID, V FROM T WHERE V = 10"));
        assertEquals(List.of("6/60", "5/50", "4/40"),
                rows("SELECT ID, V FROM T WHERE V >= 40 ORDER BY V DESC"));
    }

    /**
     * delta 达到 compactionMinRows 就合并回段，之后行数、值、点查与剪枝都正确。
     */
    @Test
    void compactionKeepsDataReadable() throws SQLException {
        execute(createTable("T", "ID INT PRIMARY KEY, V INT",
                "\"segmentSize=2\", \"compactionMinRows=2\""));
        execute("INSERT INTO T VALUES (1, 10), (2, 20), (3, 30), (4, 40)");
        execute("UPDATE T SET V = 11 WHERE ID = 1");
        execute("DELETE FROM T WHERE ID = 2");
        assertEquals("3", scalar("SELECT COUNT(*) FROM T"));
        assertEquals(List.of("1/11", "3/30", "4/40"), rows("SELECT ID, V FROM T ORDER BY ID"));
        assertEquals(List.of("1/11"), rows("SELECT ID, V FROM T WHERE ID = 1"));
        assertEquals(List.of("4/40"), rows("SELECT ID, V FROM T WHERE V > 30"));
    }

    // ------------------------------------------------------------------
    // 兼容性限制
    // ------------------------------------------------------------------

    /**
     * 默认引擎会连 H2 自己的元数据表一起接管：
     * {@code Schema.createTable} 在 {@code data.tableEngine == null} 时
     * 回退到 {@code defaultTableEngine}（{@code org/h2/schema/Schema.java:795}），
     * 而元数据必须落在 MVStore 里。实测结果是开库直接失败。
     */
    @Test
    void defaultTableEngineIsIncompatible() {
        assertThrows(SQLException.class, () -> DriverManager.getConnection(
                "jdbc:h2:mem:omapdefault;DEFAULT_TABLE_ENGINE=" + ENGINE, "sa", ""));
    }

    @Test
    void primaryKeySurvivesRestart(@TempDir Path dir) throws SQLException {
        String url = "jdbc:h2:" + dir.resolve("restartpk");
        try (Connection first = DriverManager.getConnection(url, "sa", "")) {
            try (Statement s = first.createStatement()) {
                s.execute(createTable("T", "ID INT PRIMARY KEY, NAME VARCHAR", null));
                s.execute("INSERT INTO T VALUES (1, 'a'), (2, 'b')");
            }
        }
        try (Connection second = DriverManager.getConnection(url, "sa", "")) {
            try (Statement s = second.createStatement()) {
                assertEquals("2", toString(s, "SELECT COUNT(*) FROM T"));
                assertEquals("1/a", toString(s, "SELECT ID || '/' || NAME FROM T WHERE ID = 1"));
                s.execute("INSERT INTO T VALUES (7, 'g'), (8, 'h')");
                assertEquals("8/h", toString(s, "SELECT ID || '/' || NAME FROM T WHERE ID = 8"));
                try (ResultSet rs = s.executeQuery("SELECT ID FROM T ORDER BY ID")) {
                    assertTrue(rs.next());
                    assertEquals("1", rs.getString(1));
                }
            }
        }
    }

    /**
     * 段冻结后被删除的行只存在 delta 里，重开数据库时索引回填必须跳过它们：
     * 回填越界会让主键索引静默丢失，之后重复主键就不再被拒绝。
     */
    @Test
    void primaryKeySurvivesRestartAfterDeletingFrozenRows(@TempDir Path dir) throws SQLException {
        String url = "jdbc:h2:" + dir.resolve("restartpkdel");
        try (Connection first = DriverManager.getConnection(url, "sa", "")) {
            try (Statement s = first.createStatement()) {
                s.execute(createTable("T", "ID INT PRIMARY KEY, NAME VARCHAR", "\"segmentSize=2\""));
                s.execute("INSERT INTO T VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')");
                s.execute("DELETE FROM T WHERE ID IN (1, 3)");
            }
        }
        try (Connection second = DriverManager.getConnection(url, "sa", "")) {
            try (Statement s = second.createStatement()) {
                assertEquals("3", toString(s, "SELECT COUNT(*) FROM T"));
                assertEquals("2/b", toString(s, "SELECT ID || '/' || NAME FROM T WHERE ID = 2"));
                assertThrows(SQLException.class, () -> s.execute("INSERT INTO T VALUES (2, 'dup')"));
            }
        }
    }

    @Test
    void scalarSubqueryReusesResultAndInvalidatesOnWrite() throws SQLException {
        execute(createTable("T", "ID INT PRIMARY KEY, V INT", null));
        execute("INSERT INTO T VALUES (1, 10), (2, 20), (3, 30)");

        try (PreparedStatement psWhere = conn.prepareStatement(
                "SELECT ID FROM T WHERE V > (SELECT AVG(V) FROM T) ORDER BY ID");
                PreparedStatement psProj = conn.prepareStatement(
                        "SELECT ID, (SELECT MAX(V) FROM T) FROM T ORDER BY ID");
                PreparedStatement psExists = conn.prepareStatement(
                        "SELECT COUNT(*) FROM T WHERE EXISTS (SELECT 1 FROM T WHERE V >= 100)");
                PreparedStatement psIn = conn.prepareStatement(
                        "SELECT ID FROM T WHERE V IN (SELECT V FROM T WHERE V >= 30) ORDER BY ID")) {

            assertEquals(List.of("3"), queryStrings(psWhere));
            assertEquals(List.of("1/30", "2/30", "3/30"), queryStrings(psProj));
            assertEquals(List.of("0"), queryStrings(psExists));
            assertEquals(List.of("3"), queryStrings(psIn));

            // INSERT 后缓存必须立即失效。
            execute("INSERT INTO T VALUES (4, 100)");
            assertEquals(List.of("4"), queryStrings(psWhere));
            assertEquals(List.of("1/100", "2/100", "3/100", "4/100"), queryStrings(psProj));
            assertEquals(List.of("4"), queryStrings(psExists));
            assertEquals(List.of("3", "4"), queryStrings(psIn));

            // UPDATE 后缓存必须立即失效。
            execute("UPDATE T SET V = 10 WHERE ID = 4");
            assertEquals(List.of("2", "3"), queryStrings(psWhere));
            assertEquals(List.of("1/30", "2/30", "3/30", "4/30"), queryStrings(psProj));
            assertEquals(List.of("0"), queryStrings(psExists));
            assertEquals(List.of("3"), queryStrings(psIn));

            // DELETE 后缓存必须立即失效。
            execute("DELETE FROM T WHERE ID = 3");
            assertEquals(List.of("2"), queryStrings(psWhere));
            assertEquals(List.of("1/20", "2/20", "4/20"), queryStrings(psProj));
            assertEquals(List.of(), queryStrings(psIn));

            // TRUNCATE 后缓存必须立即失效。
            execute("TRUNCATE TABLE T");
            assertEquals(List.of(), queryStrings(psWhere));
            assertEquals(List.of(), queryStrings(psProj));
            assertEquals(List.of("0"), queryStrings(psExists));
        }
    }

    // ------------------------------------------------------------------
    // 子查询与常量边界
    // ------------------------------------------------------------------

    /**
     * 不相关子查询折成常量后，结果必须与 MVStore 等价查询一致。
     *
     * <p>覆盖下推入口把 {@code WHERE} 里的子查询当常量编译的三条路径：
     * 子查询带聚合、子查询自带 {@code WHERE}、{@code IN} 里的子查询。
     */
    @Test
    void uncorrelatedSubqueryFoldsIntoConstantPredicate() throws SQLException {
        execute(createTable("T", "ID INT PRIMARY KEY, V INT, TAG VARCHAR", "\"segmentSize=4\""));
        execute("CREATE TABLE M(ID INT PRIMARY KEY, V INT, TAG VARCHAR)"
                + " ENGINE \"org.h2.mvstore.db.Store\"");
        String insert = "INSERT INTO %s SELECT X, X * 10, 'k' || MOD(X, 3) FROM SYSTEM_RANGE(1, 40)";
        execute(String.format(insert, "T"));
        execute(String.format(insert, "M"));

        String[] queries = {
                "SELECT COUNT(*) FROM %1$s WHERE V > (SELECT AVG(V) FROM %1$s)",
                "SELECT COUNT(*) FROM %1$s WHERE V < (SELECT AVG(V) FROM %1$s WHERE TAG = 'k1')",
                "SELECT COUNT(*) FROM %1$s WHERE TAG IN (SELECT TAG FROM %1$s WHERE V >= 300)",
                "SELECT COUNT(*) FROM %1$s WHERE V >= (SELECT MAX(V) FROM %1$s WHERE TAG = 'k2')",
        };
        for (String query : queries) {
            assertEquals(rows(String.format(query, "M")), rows(String.format(query, "T")),
                    query);
        }
    }

    /**
     * 引用外层列的关联子查询不得折成常量，结果必须与 MVStore 一致。
     *
     * <p>外层列出现在子查询的 {@code WHERE} 与嵌套子查询里各测一次。
     */
    @Test
    void correlatedSubqueryIsNotFoldedIntoConstant() throws SQLException {
        execute(createTable("T", "ID INT PRIMARY KEY, V INT, TAG VARCHAR", "\"segmentSize=4\""));
        execute("CREATE TABLE M(ID INT PRIMARY KEY, V INT, TAG VARCHAR)"
                + " ENGINE \"org.h2.mvstore.db.Store\"");
        String insert = "INSERT INTO %s SELECT X, X * 10, 'k' || MOD(X, 3) FROM SYSTEM_RANGE(1, 40)";
        execute(String.format(insert, "T"));
        execute(String.format(insert, "M"));

        String[] queries = {
                "SELECT COUNT(*) FROM %1$s WHERE V > (SELECT AVG(Q.V) FROM %1$s Q WHERE Q.TAG = %1$s.TAG)",
                "SELECT COUNT(*) FROM %1$s WHERE V > (SELECT MAX(Q.V) FROM %1$s Q"
                        + " WHERE Q.TAG = %1$s.TAG AND Q.V < %1$s.V + 50)",
                "SELECT COUNT(*) FROM %1$s WHERE EXISTS (SELECT 1 FROM %1$s Q"
                        + " WHERE Q.TAG = %1$s.TAG AND Q.V > %1$s.V)",
        };
        for (String query : queries) {
            assertEquals(rows(String.format(query, "M")), rows(String.format(query, "T")),
                    query);
        }
    }

    /**
     * 整型列与带小数的常量比较时，常量必须收敛到整数边界而不是四舍五入。
     */
    @Test
    void fractionalConstantKeepsComparisonBoundary() throws SQLException {
        execute(createTable("T", "ID INT PRIMARY KEY, V INT", "\"segmentSize=4\""));
        execute("CREATE TABLE M(ID INT PRIMARY KEY, V INT) ENGINE \"org.h2.mvstore.db.Store\"");
        execute("INSERT INTO T VALUES (1, 1), (2, 2), (3, 3)");
        execute("INSERT INTO M VALUES (1, 1), (2, 2), (3, 3)");

        String[] queries = {
                "SELECT COUNT(*) FROM %s WHERE V > 1.5",
                "SELECT COUNT(*) FROM %s WHERE V < 1.5",
                "SELECT COUNT(*) FROM %s WHERE V >= 1.5",
                "SELECT COUNT(*) FROM %s WHERE V <= 1.5",
                "SELECT COUNT(*) FROM %s WHERE V = 2.5",
                "SELECT COUNT(*) FROM %s WHERE V <> 2.5",
                "SELECT COUNT(*) FROM %s WHERE V > (SELECT AVG(V) FROM %s)",
        };
        for (String query : queries) {
            assertEquals(rows(String.format(query, "M", "M")), rows(String.format(query, "T", "T")),
                    query);
        }
    }

    /**
     * {@code IN (SELECT)} 折成集合节点后的语义核对：非空集、空集、{@code NOT IN} 的 NULL 传播、
     * 不相关与关联两种形态。
     */
    @Test
    void inSubqueryFoldsIntoSetPredicate() throws SQLException {
        execute(createTable("T", "ID INT PRIMARY KEY, V INT, TAG VARCHAR", "\"segmentSize=4\""));
        execute("CREATE TABLE M(ID INT PRIMARY KEY, V INT, TAG VARCHAR)"
                + " ENGINE \"org.h2.mvstore.db.Store\"");
        execute("INSERT INTO T VALUES (1, 10, 'a'), (2, 20, 'b'), (3, NULL, 'a'), (4, 40, NULL)");
        execute("INSERT INTO M VALUES (1, 10, 'a'), (2, 20, 'b'), (3, NULL, 'a'), (4, 40, NULL)");

        String[] queries = {
                // 非空集且含 NULL 与字符串
                "SELECT COUNT(*) FROM %1$s WHERE TAG IN (SELECT TAG FROM %1$s WHERE V >= 10)",
                // NOT IN 且子查询含 NULL：任何行都不命中
                "SELECT COUNT(*) FROM %1$s WHERE TAG NOT IN (SELECT TAG FROM %1$s WHERE V >= 10)",
                // 子查询空集：IN 恒假、NOT IN 恒真（含左值为 NULL 的行）
                "SELECT COUNT(*) FROM %1$s WHERE TAG IN (SELECT TAG FROM %1$s WHERE V > 100)",
                "SELECT COUNT(*) FROM %1$s WHERE TAG NOT IN (SELECT TAG FROM %1$s WHERE V > 100)",
                // 左值带 NULL
                "SELECT COUNT(*) FROM %1$s WHERE V IN (SELECT V FROM %1$s WHERE TAG = 'a')",
                "SELECT COUNT(*) FROM %1$s WHERE V NOT IN (SELECT V FROM %1$s WHERE TAG = 'a')",
                // 数值键
                "SELECT ID FROM %1$s WHERE V IN (SELECT V FROM %1$s WHERE TAG = 'b') ORDER BY ID",
                // 关联 IN 交回 H2 路径，结果必须一致
                "SELECT ID FROM %1$s WHERE V IN (SELECT V FROM %1$s T2 WHERE T2.TAG = %1$s.TAG) ORDER BY ID",
        };
        for (String query : queries) {
            assertEquals(rows(String.format(query, "M")), rows(String.format(query, "T")), query);
        }
    }

    /**
     * 单列等值关联的标量聚合子查询折成预计算分组后的语义核对：
     * 空组取 COUNT 0 / 其余 NULL、内外层 NULL 键不匹配、删除与更新后的可见性。
     */
    @Test
    void correlatedAggregateFoldsIntoPrecomputedGroups() throws SQLException {
        execute(createTable("T", "ID INT PRIMARY KEY, K INT, V INT", "\"segmentSize=4\""));
        execute("CREATE TABLE M(ID INT PRIMARY KEY, K INT, V INT)"
                + " ENGINE \"org.h2.mvstore.db.Store\"");
        String insert = "INSERT INTO %s VALUES (1, 1, 10), (2, 1, 20), (3, 2, 5), (4, 2, NULL),"
                + " (5, 3, 7), (6, NULL, 99), (7, 4, 50), (8, 5, 1), (9, 5, 2), (10, NULL, 1)";
        execute(String.format(insert, "T"));
        execute(String.format(insert, "M"));
        // 让段内出现删除位图与 delta 改动，预计算必须跳过已删行、看到新像
        execute("DELETE FROM T WHERE ID = 2");
        execute("DELETE FROM M WHERE ID = 2");
        execute("UPDATE T SET V = 25 WHERE ID = 1");
        execute("UPDATE M SET V = 25 WHERE ID = 1");

        String[] queries = {
                "SELECT ID FROM %1$s WHERE V > (SELECT AVG(V) FROM %1$s T2 WHERE T2.K = %1$s.K) ORDER BY ID",
                "SELECT ID FROM %1$s WHERE V > (SELECT COUNT(V) FROM %1$s T2 WHERE T2.K = %1$s.K) ORDER BY ID",
                "SELECT ID FROM %1$s WHERE V > (SELECT COUNT(*) FROM %1$s T2 WHERE T2.K = %1$s.K) ORDER BY ID",
                "SELECT ID FROM %1$s WHERE V > (SELECT SUM(V) FROM %1$s T2 WHERE T2.K = %1$s.K) ORDER BY ID",
                "SELECT ID FROM %1$s WHERE V < (SELECT MIN(V) FROM %1$s T2 WHERE T2.K = %1$s.K) ORDER BY ID",
                "SELECT ID FROM %1$s WHERE V <= (SELECT MAX(V) FROM %1$s T2 WHERE T2.K = %1$s.K) ORDER BY ID",
                // 内层常量谓词
                "SELECT ID FROM %1$s WHERE V > (SELECT AVG(V) FROM %1$s T2 WHERE T2.K = %1$s.K AND T2.V > 6) ORDER BY ID",
                // 子查询在比较左侧
                "SELECT ID FROM %1$s WHERE (SELECT MIN(V) FROM %1$s T2 WHERE T2.K = %1$s.K) < V ORDER BY ID",
        };
        for (String query : queries) {
            assertEquals(rows(String.format(query, "M")), rows(String.format(query, "T")), query);
        }
    }

    /**
     * 关联键为字符串字典列（{@code DictVarcharVector}）时的语义核对：
     * 外层按字典下标取比较目标，结果必须与 MVStore 一致。
     *
     * <p>比较列取 200 个互不相同的值，使每段的不同值数量超过字典容量上限，
     * 从而建成普通 {@code IntVector}，外层才能走整数比较列的快路径。
     */
    @Test
    void correlatedAggregateWithVarcharKeyFoldsIntoPrecomputedGroups() throws SQLException {
        execute(createTable("T", "ID INT PRIMARY KEY, K VARCHAR, V INT", "\"segmentSize=100\""));
        execute("CREATE TABLE M(ID INT PRIMARY KEY, K VARCHAR, V INT)"
                + " ENGINE \"org.h2.mvstore.db.Store\"");
        String insert = "INSERT INTO %s SELECT X, CASE MOD(X, 7) WHEN 0 THEN NULL"
                + " ELSE 'k' || MOD(X, 5) END, X * 10 FROM SYSTEM_RANGE(1, 200)";
        execute(String.format(insert, "T"));
        execute(String.format(insert, "M"));
        // 让段内出现删除位图与 delta 改动，预计算必须跳过已删行、看到新像
        execute("DELETE FROM T WHERE ID IN (3, 50, 150)");
        execute("DELETE FROM M WHERE ID IN (3, 50, 150)");
        execute("UPDATE T SET V = 25 WHERE ID = 4");
        execute("UPDATE M SET V = 25 WHERE ID = 4");

        String[] queries = {
                "SELECT COUNT(*) FROM %1$s WHERE V > (SELECT AVG(V) FROM %1$s T2"
                        + " WHERE T2.K = %1$s.K)",
                "SELECT COUNT(*) FROM %1$s WHERE V < (SELECT MIN(V) FROM %1$s T2"
                        + " WHERE T2.K = %1$s.K)",
                "SELECT COUNT(*) FROM %1$s WHERE V <= (SELECT MAX(V) FROM %1$s T2"
                        + " WHERE T2.K = %1$s.K)",
                "SELECT COUNT(*) FROM %1$s WHERE V > (SELECT SUM(V) FROM %1$s T2"
                        + " WHERE T2.K = %1$s.K AND T2.V > 60)",
        };
        for (String query : queries) {
            assertEquals(rows(String.format(query, "M")), rows(String.format(query, "T")), query);
        }
    }

    /**
     * 关联 {@code EXISTS} / {@code NOT EXISTS} 折成半连接后的语义核对，
     * 含内层常量谓词与不相关 {@code EXISTS} 常量化。
     */
    @Test
    void correlatedExistsFoldsIntoSemiJoin() throws SQLException {
        execute(createTable("T", "ID INT PRIMARY KEY, K INT, V INT", "\"segmentSize=4\""));
        execute("CREATE TABLE M(ID INT PRIMARY KEY, K INT, V INT)"
                + " ENGINE \"org.h2.mvstore.db.Store\"");
        String insert = "INSERT INTO %s VALUES (1, 1, 10), (2, 1, 20), (3, 2, 5), (4, 3, 7),"
                + " (5, NULL, 99), (6, 4, 50), (7, NULL, 1)";
        execute(String.format(insert, "T"));
        execute(String.format(insert, "M"));

        String[] queries = {
                "SELECT ID FROM %1$s WHERE EXISTS (SELECT 1 FROM %1$s T2 WHERE T2.K = %1$s.K) ORDER BY ID",
                "SELECT ID FROM %1$s WHERE NOT EXISTS (SELECT 1 FROM %1$s T2 WHERE T2.K = %1$s.K) ORDER BY ID",
                // 内层常量谓词
                "SELECT ID FROM %1$s WHERE EXISTS (SELECT 1 FROM %1$s T2 WHERE T2.K = %1$s.K AND T2.V > 15) ORDER BY ID",
                "SELECT ID FROM %1$s WHERE NOT EXISTS (SELECT 1 FROM %1$s T2 WHERE T2.K = %1$s.K AND T2.V > 15) ORDER BY ID",
                // 不相关 EXISTS 常量化
                "SELECT ID FROM %1$s WHERE EXISTS (SELECT 1 FROM %1$s WHERE V > 100) ORDER BY ID",
                "SELECT ID FROM %1$s WHERE NOT EXISTS (SELECT 1 FROM %1$s WHERE V > 100) ORDER BY ID",
        };
        for (String query : queries) {
            assertEquals(rows(String.format(query, "M")), rows(String.format(query, "T")), query);
        }
    }

    /**
     * 不识别的关联形态（多关联条件、内层 HAVING、内层非 OMAP 表）交回 H2 路径，
     * 结果必须与 MVStore 一致。
     */
    @Test
    void unsupportedCorrelatedShapesFallBackToRowPath() throws SQLException {
        execute(createTable("T", "ID INT PRIMARY KEY, K INT, V INT", "\"segmentSize=4\""));
        execute("CREATE TABLE M(ID INT PRIMARY KEY, K INT, V INT)"
                + " ENGINE \"org.h2.mvstore.db.Store\"");
        String insert = "INSERT INTO %s VALUES (1, 1, 10), (2, 1, 20), (3, 2, 5), (4, 3, 7)";
        execute(String.format(insert, "T"));
        execute(String.format(insert, "M"));

        String[] queries = {
                // 多关联条件
                "SELECT ID FROM %1$s WHERE V > (SELECT AVG(V) FROM %1$s T2"
                        + " WHERE T2.K = %1$s.K AND T2.V < %1$s.V) ORDER BY ID",
                // 内层 HAVING
                "SELECT ID FROM %1$s WHERE V > (SELECT MAX(V) FROM %1$s T2 WHERE T2.K = K"
                        + " HAVING MAX(V) > 4) ORDER BY ID",
                // 内层非 OMAP 表
                "SELECT ID FROM %1$s WHERE V > (SELECT AVG(V) FROM M T2 WHERE T2.K = %1$s.K) ORDER BY ID",
                // 关联 IN：多关联条件不折叠，交回 H2
                "SELECT ID FROM %1$s WHERE V IN (SELECT T2.V FROM %1$s T2"
                        + " WHERE T2.K = %1$s.K AND T2.ID < %1$s.ID) ORDER BY ID",
        };
        for (String query : queries) {
            assertEquals(rows(String.format(query, "M")), rows(String.format(query, "T")), query);
        }
    }

    /**
     * 关联 IN / NOT IN (SELECT ...)：折成「关联键 → 内层值集合」后与 MVStore 逐行一致，
     * 覆盖整数与 varchar 关联键、未命中键、NULL 键、内层投影含 NULL（NOT IN 三值语义）、
     * 内层常量谓词，以及表达式投影不折叠交回 H2 的回退。
     */
    @Test
    void correlatedInFoldsIntoKeyedSets() throws SQLException {
        execute(createTable("T", "ID INT PRIMARY KEY, K INT, S VARCHAR(8), V INT", "\"segmentSize=4\""));
        execute("CREATE TABLE M(ID INT PRIMARY KEY, K INT, S VARCHAR(8), V INT)"
                + " ENGINE \"org.h2.mvstore.db.Store\"");
        String insert = "INSERT INTO %s VALUES "
                + "(1, 1, 'a', 10), (2, 1, 'b', NULL), (3, 2, 'a', 30), (4, NULL, 'b', 40),"
                + " (5, 3, 'a', 50), (6, 3, 'b', 60), (7, 9, 'c', 70), (8, 2, NULL, 80)";
        execute(String.format(insert, "T"));
        execute(String.format(insert, "M"));

        String[] queries = {
                "SELECT ID FROM %1$s WHERE V IN (SELECT T2.V FROM %1$s T2 WHERE T2.K = %1$s.K) ORDER BY ID",
                "SELECT ID FROM %1$s WHERE V NOT IN (SELECT T2.V FROM %1$s T2 WHERE T2.K = %1$s.K) ORDER BY ID",
                // varchar 关联键
                "SELECT ID FROM %1$s WHERE V IN (SELECT T2.V FROM %1$s T2 WHERE T2.S = %1$s.S) ORDER BY ID",
                "SELECT ID FROM %1$s WHERE V NOT IN (SELECT T2.V FROM %1$s T2 WHERE T2.S = %1$s.S) ORDER BY ID",
                // 内层常量谓词
                "SELECT ID FROM %1$s WHERE V IN (SELECT T2.V FROM %1$s T2 WHERE T2.K = %1$s.K AND T2.V >= 30) ORDER BY ID",
                // 投影是表达式：不折叠，交回 H2
                "SELECT ID FROM %1$s WHERE V IN (SELECT T2.V + 1 FROM %1$s T2 WHERE T2.K = %1$s.K) ORDER BY ID",
        };
        for (String query : queries) {
            assertEquals(rows(String.format(query, "M")), rows(String.format(query, "T")), query);
        }
    }

    /**
     * 1 万行关联 IN 的耗时断言：折成键值集合后应在毫秒级；未折叠的嵌套循环
     * 实测在 7 秒量级。预期行数用等值自连接独立验证。
     */
    @Test
    void correlatedInFasterThanNestedLoop() throws SQLException {
        execute(createTable("T", "ID INT PRIMARY KEY, K INT, X INT", "\"segmentSize=1000\""));
        execute("INSERT INTO T SELECT X, MOD(X, 100), MOD(X * 7919, 10000) FROM SYSTEM_RANGE(1, 10000)");
        // 期望值用 MVStore 表独立验证（MVStore 只作结果对照，不测耗时）
        execute("CREATE TABLE M(ID INT PRIMARY KEY, K INT, X INT) ENGINE \"org.h2.mvstore.db.Store\""
                + " AS SELECT X, MOD(X, 100), MOD(X * 7919, 10000) FROM SYSTEM_RANGE(1, 10000)");
        String expected = scalar("SELECT COUNT(*) FROM M WHERE X IN (SELECT T2.ID FROM M T2 WHERE T2.K = M.K)");

        long start = System.nanoTime();
        String actual = scalar("SELECT COUNT(*) FROM T WHERE X IN (SELECT T2.ID FROM T T2 WHERE T2.K = T.K)");
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        assertEquals(expected, actual);
        assertTrue(elapsedMs < 200, "correlated IN took " + elapsedMs + " ms");
    }

    /**
     * 1 万行关联标量聚合的耗时断言：折成预计算分组后应在毫秒级，
     * 未折叠的逐行嵌套循环实测在 3.5 秒量级。预期行数用两步物化独立验证。
     */
    @Test
    void correlatedAggregateFasterThanNestedLoop() throws SQLException {
        execute(createTable("T", "ID INT PRIMARY KEY, K INT, V INT", "\"segmentSize=1000\""));
        execute("INSERT INTO T SELECT X, MOD(X, 100), X FROM SYSTEM_RANGE(1, 10000)");
        execute("CREATE TABLE G(K INT PRIMARY KEY, A DOUBLE) AS SELECT K, AVG(V) FROM T GROUP BY K");
        String expected = scalar("SELECT COUNT(*) FROM T WHERE V > (SELECT A FROM G WHERE G.K = T.K)");

        long start = System.nanoTime();
        String actual = scalar("SELECT COUNT(*) FROM T WHERE V > (SELECT AVG(V) FROM T T2 WHERE T2.K = T.K)");
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        assertEquals(expected, actual);
        assertTrue(elapsedMs < 200, "correlated aggregate took " + elapsedMs + " ms");
    }

    // ------------------------------------------------------------------
    // 工具
    // ------------------------------------------------------------------

    private static String createTable(String name, String columns, String with) {
        return "CREATE TABLE " + name + "(" + columns + ") ENGINE \"" + ENGINE + "\""
                + (with == null ? "" : " WITH " + with);
    }

    private void execute(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    private String scalar(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    private List<String> rows(String sql) throws SQLException {
        List<String> result = new ArrayList<>();
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            int n = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder b = new StringBuilder();
                for (int i = 1; i <= n; i++) {
                    if (i > 1) {
                        b.append('/');
                    }
                    b.append(rs.getString(i));
                }
                result.add(b.toString());
            }
        }
        return result;
    }

    private static List<String> queryStrings(PreparedStatement ps) throws SQLException {
        List<String> result = new ArrayList<>();
        try (ResultSet rs = ps.executeQuery()) {
            int n = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder b = new StringBuilder();
                for (int i = 1; i <= n; i++) {
                    if (i > 1) {
                        b.append('/');
                    }
                    b.append(rs.getString(i));
                }
                result.add(b.toString());
            }
        }
        return result;
    }

    @Test
    void lowCardinalityNumericDictionaryAndCompaction() throws Exception {
        execute(createTable("T",
                "ID INT PRIMARY KEY, ST TINYINT, CODE INT, AMT BIGINT, RATE DOUBLE",
                "\"segmentSize=100\", \"compactionMinRows=5\""));
        for (int i = 1; i <= 250; i++) {
            if (i % 10 == 0) {
                execute("INSERT INTO T VALUES (" + i + ", NULL, NULL, NULL, NULL)");
            } else {
                int k = i % 3;
                execute("INSERT INTO T VALUES (" + i + ", " + k + ", " + (1000 + k)
                        + ", " + (100000L + k) + ", " + (0.25 * k) + ")");
            }
        }
        // 前两段已冻结并启用 DictValueVector
        try (Statement s = conn.createStatement()) {
            assertEquals("225", toString(s, "SELECT COUNT(*) FROM T WHERE ST IS NOT NULL"));
            assertEquals("75", toString(s, "SELECT COUNT(*) FROM T WHERE CODE = 1001"));
            assertEquals("75", toString(s, "SELECT COUNT(*) FROM T WHERE AMT = 100002"));
            assertEquals("75", toString(s, "SELECT COUNT(*) FROM T WHERE RATE = 0.5"));

            // 更新触发 compact() 并写入字典中原本不存在的新数值与 NULL
            execute("UPDATE T SET CODE = 9999, AMT = 999999, RATE = 9.5 WHERE ID BETWEEN 1 AND 10");
            execute("UPDATE T SET ST = NULL WHERE ID = 1");

            assertEquals("10", toString(s, "SELECT COUNT(*) FROM T WHERE CODE = 9999"));
            assertEquals("10", toString(s, "SELECT COUNT(*) FROM T WHERE AMT = 999999"));
            assertEquals("10", toString(s, "SELECT COUNT(*) FROM T WHERE RATE = 9.5"));
            assertEquals("224", toString(s, "SELECT COUNT(*) FROM T WHERE ST IS NOT NULL"));
        }
    }

    @Test
    void vectorizedGroupAndFilterQueriesMatchExpected() throws Exception {
        execute(createTable("T",
                "ID INT PRIMARY KEY, CAT VARCHAR(10), V INT, D DOUBLE, N INT",
                "\"segmentSize=100\", \"compactionMinRows=10\""));
        execute("CREATE TABLE T_MV (ID INT PRIMARY KEY, CAT VARCHAR(10), V INT, D DOUBLE, N INT) ENGINE \"org.h2.mvstore.db.Store\"");
        for (int i = 1; i <= 250; i++) {
            String cat = (i % 5 == 0) ? "NULL" : ("'C" + (i % 3) + "'");
            String n = (i % 4 == 0) ? "NULL" : String.valueOf(i);
            String vals = "(" + i + ", " + cat + ", " + i + ", " + (i * 0.5) + ", " + n + ")";
            execute("INSERT INTO T VALUES " + vals);
            execute("INSERT INTO T_MV VALUES " + vals);
        }
        // 制造 delta 更新与删除以及开放段行，验证快照一致性与边界情况
        execute("UPDATE T SET V = V + 1000 WHERE ID IN (1, 2)");
        execute("UPDATE T_MV SET V = V + 1000 WHERE ID IN (1, 2)");
        execute("DELETE FROM T WHERE ID = 3");
        execute("DELETE FROM T_MV WHERE ID = 3");

        try (Statement s = conn.createStatement()) {
            // OR + AND + 算术比较 + NULL 检查
            assertEquals(
                    toString(s, "SELECT COUNT(*) FROM T_MV WHERE (CAT = 'C1' OR V > 200) AND N IS NOT NULL"),
                    toString(s, "SELECT COUNT(*) FROM T WHERE (CAT = 'C1' OR V > 200) AND N IS NOT NULL"));
            // IN / NOT IN
            assertEquals(
                    toString(s, "SELECT COUNT(*) FROM T_MV WHERE CAT IN ('C0', 'C1')"),
                    toString(s, "SELECT COUNT(*) FROM T WHERE CAT IN ('C0', 'C1')"));
            assertEquals(
                    toString(s, "SELECT COUNT(*) FROM T_MV WHERE CAT NOT IN ('C0', 'C1')"),
                    toString(s, "SELECT COUNT(*) FROM T WHERE CAT NOT IN ('C0', 'C1')"));
            // 0 行命中时全局聚合返回 1 行 (0, NULL)
            try (ResultSet rs = s.executeQuery("SELECT COUNT(*), SUM(V), MIN(V), MAX(V) FROM T WHERE V > 999999")) {
                assertTrue(rs.next());
                assertEquals(0L, rs.getLong(1));
                assertNull(rs.getObject(2));
                assertNull(rs.getObject(3));
                assertNull(rs.getObject(4));
            }
            // 0 行命中时 GROUP BY 返回 0 行
            try (ResultSet rs = s.executeQuery("SELECT CAT, COUNT(*) FROM T WHERE V > 999999 GROUP BY CAT")) {
                assertFalse(rs.next());
            }
            // GROUP BY + ORDER BY + LIMIT + OFFSET + NULL 组
            try (ResultSet rs = s.executeQuery(
                    "SELECT CAT, COUNT(*) AS C, SUM(V) FROM T GROUP BY CAT ORDER BY C DESC LIMIT 2 OFFSET 1")) {
                assertTrue(rs.next());
                assertTrue(rs.next());
                assertFalse(rs.next());
            }
            // HAVING 路径正确
            assertEquals(
                    toString(s, "SELECT COUNT(*) FROM (SELECT CAT, COUNT(*) AS C FROM T_MV GROUP BY CAT HAVING COUNT(*) > 60)"),
                    toString(s, "SELECT COUNT(*) FROM (SELECT CAT, COUNT(*) AS C FROM T GROUP BY CAT HAVING COUNT(*) > 60)"));
        }
    }

    @Test
    void vectorizedAvgMultiGroupAndHavingMatchMvStore() throws Exception {
        execute(createTable("T",
                "ID INT PRIMARY KEY, CAT VARCHAR(10), SUBCAT INT, V INT, D DOUBLE",
                "\"segmentSize=80\", \"compactionMinRows=10\""));
        execute("CREATE TABLE T_MV (ID INT PRIMARY KEY, CAT VARCHAR(10), SUBCAT INT, V INT, D DOUBLE) ENGINE \"org.h2.mvstore.db.Store\"");
        for (int i = 1; i <= 260; i++) {
            String cat = (i % 7 == 0) ? "NULL" : ("'C" + (i % 3) + "'");
            String sub = (i % 11 == 0) ? "NULL" : String.valueOf(i % 4);
            String v = (i % 13 == 0) ? "NULL" : String.valueOf(i * 3 + 1);
            String d = (i % 17 == 0) ? "NULL" : String.valueOf(i * 0.25);
            String vals = "(" + i + ", " + cat + ", " + sub + ", " + v + ", " + d + ")";
            execute("INSERT INTO T VALUES " + vals);
            execute("INSERT INTO T_MV VALUES " + vals);
        }
        execute("UPDATE T SET V = V + 500 WHERE ID IN (2, 4)");
        execute("UPDATE T_MV SET V = V + 500 WHERE ID IN (2, 4)");
        execute("DELETE FROM T WHERE ID = 5");
        execute("DELETE FROM T_MV WHERE ID = 5");

        String[] queries = {
                // 全局 AVG（INT 与 DOUBLE）与空结果 AVG
                "SELECT AVG(V), AVG(D), COUNT(*) FROM T_MV",
                "SELECT AVG(V), AVG(D), COUNT(*) FROM T_MV WHERE V > 999999",
                // 多列 GROUP BY + AVG + NULL 组
                "SELECT CAT, SUBCAT, COUNT(*), SUM(V), AVG(V), AVG(D) FROM T_MV "
                        + "WHERE V >= 100 GROUP BY CAT, SUBCAT ORDER BY CAT NULLS FIRST, SUBCAT NULLS FIRST",
                // HAVING 引用未在 SELECT 中出现的聚合 + AND
                "SELECT CAT, COUNT(*) FROM T_MV GROUP BY CAT "
                        + "HAVING COUNT(*) > 20 AND SUM(V) > 5000 ORDER BY CAT NULLS FIRST",
                // HAVING 分组列 + 聚合 OR 条件
                "SELECT CAT, MAX(V) FROM T_MV GROUP BY CAT "
                        + "HAVING CAT = 'C0' OR MAX(V) > 1000 ORDER BY CAT NULLS FIRST",
                // 全局聚合带 HAVING（命中与未命中）
                "SELECT COUNT(*), AVG(V) FROM T_MV HAVING COUNT(*) > 100",
                "SELECT COUNT(*), AVG(V) FROM T_MV HAVING COUNT(*) > 999999"
        };

        try (Statement s = conn.createStatement()) {
            for (String mvSql : queries) {
                String omapSql = mvSql.replace("T_MV", "T");
                assertEquals(collectRows(s, mvSql), collectRows(s, omapSql), "Mismatch for: " + omapSql);
            }
        }
    }

    @Test
    void defaultEngineUsesOmapWithoutEngineClause() throws Exception {
        execute("CREATE TABLE T_DEFAULT (ID INT PRIMARY KEY, CAT VARCHAR(10), V INT)");
        execute("INSERT INTO T_DEFAULT VALUES (1, 'A', 10), (2, 'A', 20), (3, 'B', 30)");
        execute("CREATE LOCAL TEMPORARY TABLE T_TMP (ID INT PRIMARY KEY, V INT)");
        execute("INSERT INTO T_TMP VALUES (1, 100)");

        org.h2.engine.SessionLocal session = (org.h2.engine.SessionLocal)
                ((org.h2.jdbc.JdbcConnection) conn).getSession();
        org.h2.table.Table tDefault = session.getDatabase().getMainSchema().findTableOrView(session, "T_DEFAULT");
        org.h2.table.Table tTmp = session.findLocalTempTable("T_TMP");
        assertTrue(tDefault instanceof com.hyd.h2omap.table.OmapTable,
                "actual=" + (tDefault == null ? "null" : tDefault.getClass().getName()));
        assertFalse(tTmp instanceof com.hyd.h2omap.table.OmapTable);

        try (Statement s = conn.createStatement()) {
            assertEquals("BASE TABLE", toString(s,
                    "SELECT TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'T_DEFAULT'"));
            assertEquals("60", toString(s, "SELECT SUM(V) FROM T_DEFAULT"));
            assertEquals("100", toString(s, "SELECT SUM(V) FROM T_TMP"));
        }
    }

    @Test
    void persistenceCheckpointWalAndDropCleanup(@TempDir Path dir) throws Exception {
        Path dbFile = dir.resolve("omap_persist");
        String url = "jdbc:h2:" + dbFile;
        String expectedAll;
        String expectedGroup;

        try (Connection c1 = DriverManager.getConnection(url, "sa", "");
                Statement s = c1.createStatement()) {
            s.execute("CREATE TABLE T (ID BIGINT PRIMARY KEY, CAT VARCHAR, V DOUBLE, TAGS INT ARRAY) "
                    + "ENGINE \"" + ENGINE + "\" WITH \"segmentSize=4\", \"compactionMinRows=2\"");
            for (int i = 1; i <= 14; i++) {
                s.execute("INSERT INTO T VALUES (" + i + ", 'c" + (i % 3) + "', " + (i * 1.5)
                        + ", ARRAY[" + i + ", " + (i + 1) + "])");
            }
            // 更新已冻结段（触发 compact）+ 删除已冻结段与开放段行
            s.execute("UPDATE T SET V = 999.5, CAT = 'upd' WHERE ID IN (2, 6)");
            s.execute("DELETE FROM T WHERE ID IN (3, 14)");
            // 写入开放段新行（仅在 WAL 中，未冻结成段）
            s.execute("INSERT INTO T VALUES (15, 'c0', 22.5, ARRAY[15, 16])");
            s.execute("INSERT INTO T VALUES (16, NULL, NULL, NULL)");

            // 再建一张显式关闭备份的表
            s.execute("CREATE TABLE T_NOBACKUP (ID INT PRIMARY KEY) ENGINE \"" + ENGINE
                    + "\" WITH \"backup=false\"");
            s.execute("INSERT INTO T_NOBACKUP VALUES (100), (200)");

            expectedAll = collectRows(s, "SELECT ID, CAT, V, TAGS FROM T ORDER BY ID");
            expectedGroup = collectRows(s,
                    "SELECT CAT, COUNT(*), SUM(V) FROM T GROUP BY CAT ORDER BY CAT");
        }

        // 确认备份目录已生成 manifest 与段文件
        Path tableBackupDir = dir.resolve("omap_persist.omap").resolve("PUBLIC_T");
        assertTrue(java.nio.file.Files.exists(tableBackupDir.resolve("manifest.dat")));

        // 模拟进程重启后重新打开数据库
        try (Connection c2 = DriverManager.getConnection(url, "sa", "");
                Statement s = c2.createStatement()) {
            assertEquals("0", toString(s, "SELECT COUNT(*) FROM T_NOBACKUP"));
            assertEquals(expectedAll, collectRows(s, "SELECT ID, CAT, V, TAGS FROM T ORDER BY ID"));
            assertEquals(expectedGroup, collectRows(s,
                    "SELECT CAT, COUNT(*), SUM(V) FROM T GROUP BY CAT ORDER BY CAT"));
            // 主键索引在重启后已回填完成
            assertEquals("upd/999.5", toString(s, "SELECT CAT || '/' || V FROM T WHERE ID = 6"));

            // 重启后继续写入并 TRUNCATE，再重启验证 TRUNCATE 持久化
            s.execute("INSERT INTO T VALUES (17, 'c1', 1.0, ARRAY[17])");
            assertEquals("15", toString(s, "SELECT COUNT(*) FROM T"));
            s.execute("TRUNCATE TABLE T");
        }

        try (Connection c3 = DriverManager.getConnection(url, "sa", "");
                Statement s = c3.createStatement()) {
            assertEquals("0", toString(s, "SELECT COUNT(*) FROM T"));
            s.execute("DROP TABLE T");
            assertFalse(java.nio.file.Files.exists(tableBackupDir));
        }
    }

    @Test
    void vectorizedFlatLikeTimestampCountDistinctAndArithAggMatchMvStore() throws SQLException {
        try (Connection c = DriverManager.getConnection("jdbc:h2:mem:vec_ext;DB_CLOSE_DELAY=-1");
                Statement s = c.createStatement()) {
            s.execute("CREATE TABLE T (ID BIGINT PRIMARY KEY, CAT VARCHAR, TXT VARCHAR, "
                    + "IV INT, LV BIGINT, DV DOUBLE, TS TIMESTAMP) "
                    + "ENGINE \"com.hyd.h2omap.engine.OmapTableEngine\" "
                    + "WITH \"segmentSize=8\", \"compactionMinRows=100\"");
            s.execute("CREATE TABLE T_MV (ID BIGINT PRIMARY KEY, CAT VARCHAR, TXT VARCHAR, "
                    + "IV INT, LV BIGINT, DV DOUBLE, TS TIMESTAMP) "
                    + "ENGINE \"org.h2.mvstore.db.Store\"");

            for (int i = 1; i <= 30; i++) {
                String cat = (i % 6 == 0) ? "NULL" : ("'cat_" + (i % 3) + "'");
                String txt = (i % 7 == 0) ? "NULL" : ("'item_" + (i * 13) + (i % 2 == 0 ? "_ab" : "_xy") + "'");
                String iv = (i % 5 == 0) ? "NULL" : String.valueOf(i * 10);
                String lv = String.valueOf(i * 100L);
                String dv = (i % 4 == 0) ? "NULL" : String.valueOf(i * 1.5);
                String ts = (i % 8 == 0) ? "NULL"
                        : ("TIMESTAMP '2026-01-01 00:00:" + String.format("%02d", i) + ".123456'");
                String vals = i + ", " + cat + ", " + txt + ", " + iv + ", " + lv + ", " + dv + ", " + ts;
                s.execute("INSERT INTO T VALUES (" + vals + ")");
                s.execute("INSERT INTO T_MV VALUES (" + vals + ")");
            }
            // 更新与删除制造段内覆盖与屏蔽
            s.execute("UPDATE T SET IV = 999, CAT = 'cat_upd' WHERE ID IN (2, 18)");
            s.execute("UPDATE T_MV SET IV = 999, CAT = 'cat_upd' WHERE ID IN (2, 18)");
            s.execute("DELETE FROM T WHERE ID IN (3, 25)");
            s.execute("DELETE FROM T_MV WHERE ID IN (3, 25)");

            String[] queries = {
                    // 1. queryFlat 列裁剪投影 + ORDER BY + LIMIT/OFFSET
                    "SELECT ID, CAT, TS FROM %s WHERE IV >= 50 ORDER BY ID DESC LIMIT 8 OFFSET 2",
                    // 2. queryFlat 常量投影 + 主键点查
                    "SELECT 'tag', ID, TXT FROM %s WHERE ID = 18",
                    // 3. TIMESTAMP 范围过滤（TimestampVector）
                    "SELECT ID, TS FROM %s WHERE TS >= TIMESTAMP '2026-01-01 00:00:10.000000' "
                            + "AND TS < TIMESTAMP '2026-01-01 00:00:22.000000' ORDER BY ID",
                    "SELECT COUNT(*) FROM %s WHERE TS IS NULL",
                    // 4. LIKE / NOT LIKE（DictVarcharVector 与 VarcharVector）
                    "SELECT ID, CAT FROM %s WHERE CAT LIKE 'cat\\_%%' ESCAPE '\\' ORDER BY ID",
                    "SELECT ID, TXT FROM %s WHERE TXT LIKE '%%\\_ab' ESCAPE '\\' ORDER BY ID",
                    "SELECT ID, CAT FROM %s WHERE CAT NOT LIKE '%%upd%%' ORDER BY ID",
                    // 5. COUNT(DISTINCT col) 全局 + GROUP BY + HAVING
                    "SELECT COUNT(DISTINCT CAT), COUNT(DISTINCT TXT), COUNT(DISTINCT IV) FROM %s",
                    "SELECT CAT, COUNT(DISTINCT IV), COUNT(DISTINCT TS) FROM %s "
                            + "GROUP BY CAT HAVING COUNT(DISTINCT IV) >= 2 ORDER BY CAT NULLS FIRST",
                    // 6. 聚合内嵌算术表达式
                    "SELECT SUM(IV * 2), SUM(IV + LV), AVG(DV - 1.5), MIN(100 - IV), MAX(IV * 3) FROM %s",
                    "SELECT CAT, SUM(IV * 2), AVG(IV + 10.0) FROM %s "
                            + "WHERE TS IS NOT NULL GROUP BY CAT ORDER BY CAT NULLS FIRST"
            };

            for (String template : queries) {
                String expected = collectRows(s, String.format(template, "T_MV"));
                String actual = collectRows(s, String.format(template, "T"));
                assertEquals(expected, actual, "Mismatch on query: " + template);
            }

            // 无 ORDER BY 的 LIMIT 早停测试：验证恰好返回 5 行且满足过滤条件
            String flatLimit = collectRows(s,
                    "SELECT ID, IV FROM T WHERE IV >= 100 LIMIT 5 OFFSET 2");
            assertEquals(5, flatLimit.split("\n").length);
        }
    }

    @Test
    void orderedLimitMatchesMvStoreAndStopsEarly() throws Exception {
        execute("CREATE TABLE T (ID BIGINT PRIMARY KEY, TS TIMESTAMP, S VARCHAR) "
                + "ENGINE \"" + ENGINE + "\" WITH \"segmentSize=10\"");
        execute("CREATE TABLE T_MV (ID BIGINT PRIMARY KEY, TS TIMESTAMP, S VARCHAR) "
                + "ENGINE \"org.h2.mvstore.db.Store\"");
        long base = java.sql.Timestamp.valueOf("2026-01-01 00:00:00").getTime();
        for (int i = 0; i < 200; i++) {
            String ts = "TIMESTAMP '" + new java.sql.Timestamp(base + i * 1000L) + "'";
            String vals = i + ", " + ts + ", 's_" + (i % 7) + "'";
            execute("INSERT INTO T VALUES (" + vals + ")");
            execute("INSERT INTO T_MV VALUES (" + vals + ")");
        }
        // 制造 delta 新像与删除，验证 extraRows 也参与堆
        execute("UPDATE T SET TS = TIMESTAMP '2026-01-01 00:05:00' WHERE ID = 1");
        execute("UPDATE T_MV SET TS = TIMESTAMP '2026-01-01 00:05:00' WHERE ID = 1");
        execute("DELETE FROM T WHERE ID = 199");
        execute("DELETE FROM T_MV WHERE ID = 199");

        org.h2.engine.SessionLocal session = (org.h2.engine.SessionLocal)
                ((org.h2.jdbc.JdbcConnection) conn).getSession();
        com.hyd.h2omap.table.OmapTable table = (com.hyd.h2omap.table.OmapTable)
                session.getDatabase().getMainSchema().findTableOrView(session, "T");
        com.hyd.h2omap.storage.ColumnarStore store = table.getStore();

        String[] queries = {
                "SELECT ID, TS FROM %s ORDER BY TS DESC LIMIT 5",
                "SELECT ID, TS FROM %s ORDER BY TS ASC LIMIT 5",
                "SELECT ID, S FROM %s ORDER BY TS DESC LIMIT 7 OFFSET 3",
                "SELECT ID, TS FROM %s WHERE TS >= TIMESTAMP '2026-01-01 00:00:30' "
                        + "AND TS <= TIMESTAMP '2026-01-01 00:02:00' ORDER BY TS DESC LIMIT 6",
                "SELECT ID, TS FROM %s ORDER BY ID DESC LIMIT 4"
        };
        long beforeSkips = store.getOrderedSkippedSegmentCount();
        try (Statement s = conn.createStatement()) {
            for (String template : queries) {
                String expected = collectRows(s, String.format(template, "T_MV"));
                String actual = collectRows(s, String.format(template, "T"));
                assertEquals(expected, actual, "Mismatch on query: " + template);
            }
        }
        assertTrue(store.getOrderedSkippedSegmentCount() > beforeSkips,
                "有序 LIMIT 应跳过部分段，实际未跳：" + store.getOrderedSkippedSegmentCount());
    }

    @Test
    void vectorizedBoundsPruneSegmentsWithoutChangingResults() throws Exception {
        execute("CREATE TABLE T (ID BIGINT PRIMARY KEY, TS TIMESTAMP) "
                + "ENGINE \"" + ENGINE + "\" WITH \"segmentSize=10\"");
        execute("CREATE TABLE T_MV (ID BIGINT PRIMARY KEY, TS TIMESTAMP) "
                + "ENGINE \"org.h2.mvstore.db.Store\"");
        for (int i = 0; i < 200; i++) {
            String ts = "TIMESTAMP '2026-01-01 00:00:" + String.format("%02d", i / 4)
                    + "." + String.format("%03d", (i % 4) * 250) + "000'";
            execute("INSERT INTO T VALUES (" + i + ", " + ts + ")");
            execute("INSERT INTO T_MV VALUES (" + i + ", " + ts + ")");
        }

        org.h2.engine.SessionLocal session = (org.h2.engine.SessionLocal)
                ((org.h2.jdbc.JdbcConnection) conn).getSession();
        com.hyd.h2omap.table.OmapTable table = (com.hyd.h2omap.table.OmapTable)
                session.getDatabase().getMainSchema().findTableOrView(session, "T");
        com.hyd.h2omap.storage.ColumnarStore store = table.getStore();

        try (Statement s = conn.createStatement()) {
            String unbounded = "SELECT COUNT(*) FROM %s";
            assertEquals(toString(s, String.format(unbounded, "T_MV")),
                    toString(s, String.format(unbounded, "T")));
            long afterUnbounded = store.getPrunedSegmentCount();

            // 窄区间：只与少数段相交，应剪掉其余段
            String narrow = "SELECT COUNT(*) FROM %s "
                    + "WHERE TS >= TIMESTAMP '2026-01-01 00:00:10.000000' "
                    + "AND TS <= TIMESTAMP '2026-01-01 00:00:20.000000'";
            assertEquals(toString(s, String.format(narrow, "T_MV")),
                    toString(s, String.format(narrow, "T")));
            assertTrue(store.getPrunedSegmentCount() > afterUnbounded,
                    "窄区间应跳过部分段，实际未跳：" + store.getPrunedSegmentCount());
            long afterNarrow = store.getPrunedSegmentCount();

            // 单侧区间同样可剪，且结果不变
            String oneSided = "SELECT ID, TS FROM %s "
                    + "WHERE TS >= TIMESTAMP '2026-01-01 00:00:40.000000' ORDER BY ID";
            assertEquals(collectRows(s, String.format(oneSided, "T_MV")),
                    collectRows(s, String.format(oneSided, "T")));
            assertTrue(store.getPrunedSegmentCount() > afterNarrow,
                    "单侧区间应跳过部分段，实际未跳：" + store.getPrunedSegmentCount());

            // 区间覆盖全段、剪不掉任何段的查询，结果应与全表一致且不增加剪枝计数
            long beforeNoPrune = store.getPrunedSegmentCount();
            String noPrune = "SELECT SUM(ID) FROM %s WHERE ID >= 0";
            assertEquals(toString(s, String.format(noPrune, "T_MV")),
                    toString(s, String.format(noPrune, "T")));
            assertEquals(beforeNoPrune, store.getPrunedSegmentCount(),
                    "所有段都与区间相交时不应剪段");

            // 区间覆盖全表时结果应与全表一致
            String wide = "SELECT COUNT(*) FROM %s "
                    + "WHERE TS >= TIMESTAMP '2025-01-01 00:00:00' "
                    + "AND TS <= TIMESTAMP '2027-01-01 00:00:00'";
            assertEquals(toString(s, String.format(unbounded, "T_MV")),
                    toString(s, String.format(wide, "T")));
        }
    }

    /**
     * BETWEEN 谓词可下推：普通形态折叠成区间比较的 AND、NOT BETWEEN 折成 OR，
     * 结果与 MVStore 逐查询一致；SYMMETRIC、空边界与倒置边界交回 H2 后同样正确。
     */
    @Test
    void vectorizedBetweenPredicateMatchesMvStore() throws Exception {
        execute("CREATE TABLE T (ID BIGINT PRIMARY KEY, D DATE, V INT) "
                + "ENGINE \"" + ENGINE + "\" WITH \"segmentSize=10\"");
        execute("CREATE TABLE T_MV (ID BIGINT PRIMARY KEY, D DATE, V INT) "
                + "ENGINE \"org.h2.mvstore.db.Store\"");
        for (int i = 0; i < 200; i++) {
            // 每 5 行留一个 NULL 日：BETWEEN 与 NOT BETWEEN 对空列值的处理必须一致
            String d = i % 5 == 0 ? "NULL" : "DATE '2026-01-" + String.format("%02d", 1 + i / 10) + "'";
            execute("INSERT INTO T VALUES (" + i + ", " + d + ", " + i + ")");
            execute("INSERT INTO T_MV VALUES (" + i + ", " + d + ", " + i + ")");
        }

        org.h2.engine.SessionLocal session = (org.h2.engine.SessionLocal)
                ((org.h2.jdbc.JdbcConnection) conn).getSession();
        com.hyd.h2omap.table.OmapTable table = (com.hyd.h2omap.table.OmapTable)
                session.getDatabase().getMainSchema().findTableOrView(session, "T");
        com.hyd.h2omap.storage.ColumnarStore store = table.getStore();

        try (Statement s = conn.createStatement()) {
            String unbounded = "SELECT COUNT(*) FROM %s";
            assertEquals(toString(s, String.format(unbounded, "T_MV")),
                    toString(s, String.format(unbounded, "T")));
            long afterUnbounded = store.getPrunedSegmentCount();

            // 窄区间 BETWEEN：应下推并剪段，结果与 MVStore 一致
            String narrow = "SELECT COUNT(*) FROM %s "
                    + "WHERE D BETWEEN DATE '2026-01-05' AND DATE '2026-01-08'";
            assertEquals(toString(s, String.format(narrow, "T_MV")),
                    toString(s, String.format(narrow, "T")));
            assertTrue(store.getPrunedSegmentCount() > afterUnbounded,
                    "BETWEEN 窄区间应跳过部分段，实际未跳：" + store.getPrunedSegmentCount());
            long afterNarrow = store.getPrunedSegmentCount();

            // NOT BETWEEN：与 MVStore 一致（空列值两侧都不命中）
            String notBetween = "SELECT COUNT(*) FROM %s "
                    + "WHERE D NOT BETWEEN DATE '2026-01-05' AND DATE '2026-01-08'";
            assertEquals(toString(s, String.format(notBetween, "T_MV")),
                    toString(s, String.format(notBetween, "T")));

            // 两形态的并集应等于非空列值总数
            String nonNull = "SELECT COUNT(*) FROM %s WHERE D IS NOT NULL";
            String both = "SELECT COUNT(*) FROM %s "
                    + "WHERE D BETWEEN DATE '2026-01-05' AND DATE '2026-01-08' "
                    + "OR D NOT BETWEEN DATE '2026-01-05' AND DATE '2026-01-08'";
            assertEquals(toString(s, String.format(nonNull, "T_MV")),
                    toString(s, String.format(both, "T")));

            // SYMMETRIC（含倒置边界）交回 H2 行式路径：结果仍正确
            String symmetric = "SELECT COUNT(*) FROM %s "
                    + "WHERE D BETWEEN SYMMETRIC DATE '2026-01-08' AND DATE '2026-01-05'";
            assertEquals(toString(s, String.format(symmetric, "T_MV")),
                    toString(s, String.format(symmetric, "T")));

            // 倒置边界的普通 BETWEEN 是空区间；NOT BETWEEN 则恒真
            String inverted = "SELECT COUNT(*) FROM %s "
                    + "WHERE D BETWEEN DATE '2026-01-08' AND DATE '2026-01-05'";
            assertEquals(toString(s, String.format(inverted, "T_MV")),
                    toString(s, String.format(inverted, "T")));
            String invertedNot = "SELECT COUNT(*) FROM %s "
                    + "WHERE D NOT BETWEEN DATE '2026-01-08' AND DATE '2026-01-05'";
            assertEquals(toString(s, String.format(nonNull, "T_MV")),
                    toString(s, String.format(invertedNot, "T")));

            // 空边界：两侧都是空结果
            String nullBound = "SELECT COUNT(*) FROM %s "
                    + "WHERE D BETWEEN NULL AND DATE '2026-01-08'";
            assertEquals("0", toString(s, String.format(nullBound, "T")));
            assertEquals("0", toString(s, String.format(nullBound, "T_MV")));

            // 值下推：窄区间 BETWEEN 配合投影与排序键
            String project = "SELECT ID, V FROM %s "
                    + "WHERE D BETWEEN DATE '2026-01-05' AND DATE '2026-01-07' "
                    + "AND V > 100 ORDER BY ID";
            assertEquals(collectRows(s, String.format(project, "T_MV")),
                    collectRows(s, String.format(project, "T")));
            assertTrue(store.getPrunedSegmentCount() >= afterNarrow,
                    "复合谓词不应丢失剪枝能力");
        }
    }

    /**
     * 高基数 VARCHAR 列（段内不同值个数超过字典容量，段里存打包字节与偏移）：
     * 变长更新经合并写回、NULL、删除，重启前后都与 MVStore 逐行一致。
     */
    @Test
    void varcharColumnSurvivesRestartAndCompaction() throws Exception {
        // 不设 DB_CLOSE_DELAY：关掉最后一个连接即关闭数据库，第二次连接真正从段文件恢复
        Path dir = java.nio.file.Files.createTempDirectory("h2omap_varchar_persist_");
        String url = "jdbc:h2:" + dir.resolve("vc_db").toAbsolutePath();
        String[] templates = {
                "SELECT ID, S FROM %s ORDER BY ID",
                "SELECT ID FROM %s WHERE S LIKE '长值-%%' ORDER BY ID",
                "SELECT ID FROM %s WHERE S = 'q' ORDER BY ID",
                "SELECT ID FROM %s WHERE S > 'value-0000000300' ORDER BY ID",
        };
        String expectedBefore;
        try (Connection c = DriverManager.getConnection(url, "sa", "");
                Statement s = c.createStatement()) {
            for (String table : new String[] { "T", "T_MV" }) {
                String engine = table.equals("T_MV") ? " ENGINE \"org.h2.mvstore.db.Store\"" : "";
                s.execute("CREATE TABLE " + table + "(ID BIGINT PRIMARY KEY, S VARCHAR(64))"
                        + engine + " WITH \"segmentSize=128\", \"compactionMinRows=20\"");
                for (int i = 1; i <= 400; i++) {
                    s.execute("INSERT INTO " + table + " VALUES (" + i + ", 'value-"
                            + String.format("%010d", i) + "-padding')");
                }
                s.execute("UPDATE " + table + " SET S = CONCAT('长值-', S) WHERE MOD(ID, 7) = 0");
                s.execute("UPDATE " + table + " SET S = 'q' WHERE MOD(ID, 11) = 1");
                s.execute("UPDATE " + table + " SET S = NULL WHERE MOD(ID, 13) = 0");
                s.execute("DELETE FROM " + table + " WHERE MOD(ID, 17) = 0");
                // 再写满一段：冻结与检查点把合并结果写进段文件，重启后段里的 zone map 与列值一致
                for (int i = 401; i <= 528; i++) {
                    s.execute("INSERT INTO " + table + " VALUES (" + i + ", 'value-"
                            + String.format("%010d", i) + "-padding')");
                }
            }
            assertQueriesMatch(s, templates);
            assertEquals("长值-value-0000000007-padding", toString(s,
                    "SELECT S FROM T WHERE ID = 7"));
            expectedBefore = collectRows(s, "SELECT ID, S FROM T ORDER BY ID");
        }
        try (Connection c = DriverManager.getConnection(url, "sa", "");
                Statement s = c.createStatement()) {
            assertEquals(expectedBefore, collectRows(s, "SELECT ID, S FROM T ORDER BY ID"));
            assertQueriesMatch(s, templates);
            assertEquals("长值-value-0000000007-padding", toString(s,
                    "SELECT S FROM T WHERE ID = 7"));
        }
    }

    /**
     * CLOB 列（方案 B2）：段内打包为「字节池 + 偏移」（ClobVector 复用 VarcharVector
     * 机制），装箱时才生成 ValueClob，消掉逐值对象开销；过滤与投影路径不读盘。
     * 覆盖 NULL、变长更新、删除、合并、真实重启（VEC_CLOB 段格式往返）、
 * LIKE 与等值过滤，并与 MVStore 逐查询比对。
     */
    @Test
    void clobColumnSurvivesRestartAndCompaction() throws Exception {
        // 不设 DB_CLOSE_DELAY：关掉最后一个连接即关闭数据库，第二次连接真正从段文件恢复
        Path dir = java.nio.file.Files.createTempDirectory("h2omap_clob_persist_");
        String url = "jdbc:h2:" + dir.resolve("clob_db").toAbsolutePath();
        String[] templates = {
                "SELECT ID, C FROM %s ORDER BY ID",
                "SELECT ID FROM %s WHERE C LIKE '长值-%%' ORDER BY ID",
                "SELECT ID FROM %s WHERE C = 'q' ORDER BY ID",
                "SELECT ID FROM %s WHERE C IS NULL ORDER BY ID",
                "SELECT COUNT(*) FROM %s WHERE ID >= 200 AND C LIKE '值%%'",
        };
        String pad = "值".repeat(60);
        String expectedBefore;
        try (Connection c = DriverManager.getConnection(url, "sa", "");
                Statement s = c.createStatement()) {
            for (String table : new String[] { "T", "T_MV" }) {
                String engine = table.equals("T_MV") ? " ENGINE \"org.h2.mvstore.db.Store\"" : "";
                s.execute("CREATE TABLE " + table + "(ID BIGINT PRIMARY KEY, C CLOB)"
                        + engine + " WITH \"segmentSize=128\", \"compactionMinRows=20\"");
                for (int i = 1; i <= 400; i++) {
                    s.execute("INSERT INTO " + table + " VALUES (" + i + ", '" + pad + i + "')");
                }
                s.execute("UPDATE " + table + " SET C = CONCAT('长值-', C) WHERE MOD(ID, 7) = 0");
                s.execute("UPDATE " + table + " SET C = 'q' WHERE MOD(ID, 11) = 1");
                s.execute("UPDATE " + table + " SET C = NULL WHERE MOD(ID, 13) = 0");
                s.execute("DELETE FROM " + table + " WHERE MOD(ID, 17) = 0");
                // 再写满一段：冻结与检查点把合并结果写进段文件，重启后段里的 zone map 与列值一致
                for (int i = 401; i <= 528; i++) {
                    s.execute("INSERT INTO " + table + " VALUES (" + i + ", '" + pad + i + "')");
                }
            }
            assertQueriesMatch(s, templates);
            assertEquals("长值-" + pad + "7", toString(s, "SELECT C FROM T WHERE ID = 7"));
            // 类型不丢：JDBC 侧仍是 CLOB。
            try (java.sql.ResultSet rs = s.executeQuery("SELECT C FROM T WHERE ID = 7")) {
                rs.next();
                assertEquals(java.sql.Types.CLOB, rs.getMetaData().getColumnType(1));
            }
            expectedBefore = collectRows(s, "SELECT ID, C FROM T ORDER BY ID");
        }
        try (Connection c = DriverManager.getConnection(url, "sa", "");
                Statement s = c.createStatement()) {
            assertEquals(expectedBefore, collectRows(s, "SELECT ID, C FROM T ORDER BY ID"));
            assertQueriesMatch(s, templates);
            assertEquals("长值-" + pad + "7", toString(s, "SELECT C FROM T WHERE ID = 7"));
        }
    }

    /**
     * 行式路径的区间剪枝不丢 delta 新像：段的 zone map 可能落后于 delta
     * （更新量低于合并阈值时新像留在段外；重开后 WAL 重放又把它放回 delta），
     * 被剪段内已更新行必须仍可见。DISTINCT 拒绝向量化下推，走 H2 行式路径。
     */
    @Test
    void rowPathBoundsKeepDeltaRows() throws Exception {
        // 不设 DB_CLOSE_DELAY：关掉最后一个连接即关闭数据库，第二次连接从段文件与 WAL 重放恢复
        Path dir = java.nio.file.Files.createTempDirectory("h2omap_delta_prune_");
        String url = "jdbc:h2:" + dir.resolve("dp_db").toAbsolutePath();
        try (Connection c = DriverManager.getConnection(url, "sa", "");
                Statement s = c.createStatement()) {
            s.execute("CREATE TABLE T(ID BIGINT PRIMARY KEY, S VARCHAR(64))"
                    + " WITH \"segmentSize=128\", \"compactionMinRows=20\"");
            for (int i = 1; i <= 400; i++) {
                s.execute("INSERT INTO T VALUES (" + i + ", 'value-"
                        + String.format("%010d", i) + "-pad')");
            }
            // 4 行更新（ID = 83、166、249、332，全在冻结段）低于合并阈值：
            // 新像留在 delta，段 zone map 仍是更新前的 'value-...'
            s.execute("UPDATE T SET S = CONCAT('zzz-', S) WHERE MOD(ID, 83) = 0");
            // 区间下界 'y' 使更新前 zone map 不相交，剪枝若丢 delta 新像则返回 0
            assertEquals(4, countRows(s, "SELECT DISTINCT ID FROM T WHERE S >= 'y'"));
        }
        try (Connection c = DriverManager.getConnection(url, "sa", "");
                Statement s = c.createStatement()) {
            assertEquals(4, countRows(s, "SELECT DISTINCT ID FROM T WHERE S >= 'y'"));
        }
    }

    @Test
    void timestampVectorSurvivesRestartAndCompaction() throws Exception {
        Path dir = java.nio.file.Files.createTempDirectory("h2omap_ts_persist_");
        String url = "jdbc:h2:" + dir.resolve("ts_db").toAbsolutePath() + ";DB_CLOSE_DELAY=-1";
        String expectedBefore;
        try (Connection c1 = DriverManager.getConnection(url, "sa", "");
                Statement s = c1.createStatement()) {
            s.execute("CREATE TABLE T (ID BIGINT PRIMARY KEY, TS TIMESTAMP) "
                    + "WITH \"segmentSize=4\", \"compactionMinRows=2\"");
            for (int i = 1; i <= 10; i++) {
                String ts = (i == 5) ? "NULL"
                        : ("TIMESTAMP '2026-06-01 12:00:" + String.format("%02d", i) + ".654321'");
                s.execute("INSERT INTO T VALUES (" + i + ", " + ts + ")");
            }
            s.execute("UPDATE T SET TS = TIMESTAMP '2026-06-01 12:00:59.999999' WHERE ID = 2");
            s.execute("DELETE FROM T WHERE ID = 3");
            expectedBefore = collectRows(s,
                    "SELECT ID, TS FROM T WHERE TS >= TIMESTAMP '2026-06-01 12:00:04.000000' ORDER BY ID");
        }
        try (Connection c2 = DriverManager.getConnection(url, "sa", "");
                Statement s = c2.createStatement()) {
            String actualAfter = collectRows(s,
                    "SELECT ID, TS FROM T WHERE TS >= TIMESTAMP '2026-06-01 12:00:04.000000' ORDER BY ID");
            assertEquals(expectedBefore, actualAfter);
        }
    }

    @Test
    void arrayContainsMatchesMvStoreAndSurvivesRestart() throws Exception {
        Path dir = java.nio.file.Files.createTempDirectory("h2omap_arr_persist_");
        String url = "jdbc:h2:" + dir.resolve("arr_db").toAbsolutePath() + ";DB_CLOSE_DELAY=-1";
        String[] templates = {
                "SELECT ID FROM %s WHERE ARRAY_CONTAINS(V, 100) ORDER BY ID",
                "SELECT ID FROM %s WHERE NOT ARRAY_CONTAINS(V, 100) ORDER BY ID",
                "SELECT ID FROM %s WHERE ARRAY_CONTAINS(V, NULL) ORDER BY ID",
                "SELECT ID FROM %s WHERE NOT ARRAY_CONTAINS(V, NULL) ORDER BY ID",
                "SELECT ID FROM %s WHERE ARRAY_CONTAINS(V, 100) OR ID < 5 ORDER BY ID",
                "SELECT ID FROM %s WHERE ARRAY_CONTAINS(V, 100) AND ARRAY_CONTAINS(S, 'abc') ORDER BY ID",
                "SELECT ID FROM %s WHERE ARRAY_CONTAINS(S, 'abc') ORDER BY ID",
                "SELECT ID FROM %s WHERE NOT ARRAY_CONTAINS(S, 'abc') ORDER BY ID",
                "SELECT ID FROM %s WHERE ARRAY_CONTAINS(V, 100.0) ORDER BY ID",
                "SELECT ID FROM %s WHERE ARRAY_CONTAINS(V, 100.5) ORDER BY ID",
                "SELECT ID FROM %s WHERE ARRAY_CONTAINS(B, 100) ORDER BY ID",
                "SELECT ID FROM %s WHERE ARRAY_CONTAINS(D, 2.5) ORDER BY ID",
                "SELECT ID FROM %s WHERE ARRAY_CONTAINS(T, TIMESTAMP '2026-01-02 03:04:05') ORDER BY ID",
                "SELECT ID FROM %s WHERE ARRAY_CONTAINS(ID, 1) ORDER BY ID",
                "SELECT COUNT(*) FROM %s WHERE ARRAY_CONTAINS(V, 100) AND ID > 2 AND IV < 900",
                "SELECT CAT, COUNT(*) FROM %s WHERE ARRAY_CONTAINS(V, 100) GROUP BY CAT ORDER BY CAT NULLS FIRST",
                "SELECT ID, S FROM %s WHERE ARRAY_CONTAINS(S, 'abc') AND ID >= 3 ORDER BY ID DESC LIMIT 4",
        };
        try (Connection c1 = DriverManager.getConnection(url, "sa", "");
                Statement s = c1.createStatement()) {
            String ddl = " (ID INT PRIMARY KEY, CAT VARCHAR(8), IV INT, V INT ARRAY, S VARCHAR ARRAY,"
                    + " B BIGINT ARRAY, D DOUBLE ARRAY, T TIMESTAMP ARRAY) ";
            s.execute("CREATE TABLE T" + ddl + "WITH \"segmentSize=8\", \"compactionMinRows=2\"");
            s.execute("CREATE TABLE T_MV" + ddl + "ENGINE \"org.h2.mvstore.db.Store\"");
            for (int i = 1; i <= 30; i++) {
                String v = switch (i % 6) {
                case 0 -> "NULL";
                case 1 -> "ARRAY[]";
                case 2 -> "ARRAY[100]";
                case 3 -> "ARRAY[100, " + i + "]";
                case 4 -> "ARRAY[" + i + ", " + (i + 1) + "]";
                default -> "ARRAY[NULL, 100]";
                };
                String sv = switch (i % 5) {
                case 0 -> "NULL";
                case 1 -> "ARRAY[]";
                case 2 -> "ARRAY['abc', 'x']";
                case 3 -> "ARRAY['abd']";
                default -> "ARRAY['abc']";
                };
                String b = switch (i % 4) {
                case 0 -> "NULL";
                case 1 -> "ARRAY[100]";
                default -> "ARRAY[" + i + "]";
                };
                String d = switch (i % 4) {
                case 0 -> "NULL";
                case 1 -> "ARRAY[2.5]";
                case 2 -> "ARRAY[2.0]";
                default -> "ARRAY[" + i + " * 0.5]";
                };
                String t = switch (i % 4) {
                case 0 -> "NULL";
                case 1 -> "ARRAY[TIMESTAMP '2026-01-02 03:04:05']";
                default -> "ARRAY[TIMESTAMP '2026-01-0" + (1 + i % 3) + " 03:04:05']";
                };
                String row = i + ", 'c" + (i % 3) + "', " + (i * 10) + ", " + v + ", " + sv
                        + ", " + b + ", " + d + ", " + t;
                s.execute("INSERT INTO T VALUES (" + row + ")");
                s.execute("INSERT INTO T_MV VALUES (" + row + ")");
            }
            assertEquals("15", toString(s,
                    "SELECT COUNT(*) FROM T WHERE ARRAY_CONTAINS(V, 100)"));
            assertQueriesMatch(s, templates);

            // 更新与删除让命中的行同时出现在已冻结段与 delta 里
            for (String table : new String[] { "T", "T_MV" }) {
                s.execute("UPDATE " + table + " SET V = ARRAY[100] WHERE ID IN (5, 8)");
                s.execute("UPDATE " + table + " SET V = ARRAY[] WHERE ID = 11");
                s.execute("DELETE FROM " + table + " WHERE ID IN (2, 20)");
            }
            assertEquals("12", toString(s,
                    "SELECT COUNT(*) FROM T WHERE ARRAY_CONTAINS(V, 100)"));
            assertQueriesMatch(s, templates);
        }
        try (Connection c2 = DriverManager.getConnection(url, "sa", "");
                Statement s = c2.createStatement()) {
            assertEquals("12", toString(s,
                    "SELECT COUNT(*) FROM T WHERE ARRAY_CONTAINS(V, 100)"));
            assertQueriesMatch(s, templates);
        }
    }

    /**
     * 段级部分聚合缓存（MOR 批次 3）：同签名同段的聚合命中缓存；
     * 含改动行的段按「generation + delta 版本」失效，其余段不受跨段写影响。
     */
    @Test
    void segmentAggCacheReusesPartials() throws Exception {
        execute("CREATE TABLE T (ID INT PRIMARY KEY, CAT VARCHAR(8), IV INT, DV DOUBLE) "
                + "ENGINE \"" + ENGINE + "\" WITH \"segmentSize=8\", \"compactionMinRows=1000\"");
        execute("CREATE TABLE T_MV (ID INT PRIMARY KEY, CAT VARCHAR(8), IV INT, DV DOUBLE) "
                + "ENGINE \"org.h2.mvstore.db.Store\"");
        for (int i = 1; i <= 40; i++) {
            String vals = i + ", 'c" + (i % 4) + "', " + (i * 10) + ", " + (i * 0.5);
            execute("INSERT INTO T VALUES (" + vals + ")");
            execute("INSERT INTO T_MV VALUES (" + vals + ")");
        }

        org.h2.engine.SessionLocal session = (org.h2.engine.SessionLocal)
                ((org.h2.jdbc.JdbcConnection) conn).getSession();
        com.hyd.h2omap.table.OmapTable table = (com.hyd.h2omap.table.OmapTable)
                session.getDatabase().getMainSchema().findTableOrView(session, "T");
        com.hyd.h2omap.vectorized.SegmentAggCache cache = table.getStore().getAggCache();

        // 同一模板列表跑多轮：全局聚合、单列分组、常量过滤、组合过滤、IN 两档。
        // 过滤常量都取值在全段值域内，避免段级剪枝使各轮命中数不可精确断言。
        String[] templates = {
                "SELECT COUNT(*), SUM(IV), MIN(IV), MAX(IV), AVG(IV) FROM %s",
                "SELECT CAT, COUNT(*), SUM(IV) FROM %s GROUP BY CAT ORDER BY CAT",
                "SELECT COUNT(*) FROM %s WHERE IV > 0",
                "SELECT CAT, SUM(IV) FROM %s WHERE IV > 0 AND CAT LIKE 'c1%%' GROUP BY CAT ORDER BY CAT",
                "SELECT COUNT(*) FROM %s WHERE CAT IN ('c1', 'c2')",
                "SELECT COUNT(*) FROM %s WHERE CAT IN ('c3')",
        };

        // 排除 H2 结果缓存的干扰：命中断言要求每次都真正进入执行器
        try (java.sql.Connection qc = DriverManager.getConnection(
                "jdbc:h2:" + dbName + ";QUERY_CACHE_SIZE=0", "sa", "");
                Statement s = qc.createStatement()) {
            assertQueriesMatch(s, templates);
            assertEquals(0, cache.hitCount(), "首轮只填充，不应命中");

            assertQueriesMatch(s, templates);
            assertEquals(30, cache.hitCount(), "5 个已冻结段 × 6 个查询应全部命中");

            // 改动只落在段 0（ID 1..8）：该段必须失效重算，其余 4 段继续命中
            for (String t : new String[] { "T", "T_MV" }) {
                execute("UPDATE " + t + " SET IV = IV + 1000 WHERE ID IN (2, 3)");
                execute("DELETE FROM " + t + " WHERE ID = 5");
            }
            assertQueriesMatch(s, templates);
            assertEquals(54, cache.hitCount(), "只有段 0 失效：30 + 4 段 × 6 查询 = 54");

            assertQueriesMatch(s, templates);
            assertEquals(84, cache.hitCount(), "段 0 重算后再次全量命中：54 + 30 = 84");
        }
    }

    /**
     * 预算小于单个段估算内存的边界域：每个段读回后立即被逐出，所有读路径
     * 都退回「读一份仅供本次使用」的临时读，行仍完整可见。
     */
    @Test
    void evictionBelowOneSegmentBudgetKeepsPointQueriesCorrect(@TempDir Path dir) throws Exception {
        String url = "jdbc:h2:" + dir.resolve("evict1");
        String pad = "v".repeat(60);
        try (Connection c = DriverManager.getConnection(url, "sa", "");
                Statement s = c.createStatement()) {
            s.execute(createTable("T", "ID INT PRIMARY KEY, V VARCHAR(64), IV INT",
                    "\"segmentSize=50\", \"compactionMinRows=100000\", \"residentBytes=1\""));
            for (int i = 0; i < 200; i++) {
                s.execute("INSERT INTO T VALUES (" + i + ", '" + pad + i + "', " + i + ")");
            }
            // 扫描路径：向量化与行式都退回临时读。
            assertEquals("200", toString(s, "SELECT COUNT(*) FROM T"));
            assertEquals("19900", toString(s, "SELECT SUM(ID) FROM T"));
            assertEquals("1225", toString(s, "SELECT SUM(ID) FROM T WHERE ID < 50"));
            assertEquals("100", toString(s, "SELECT COUNT(DISTINCT V) FROM T WHERE ID < 100"));
            // 点查路径：读回后立即被逐出，临时读仍能物化出行。
            assertEquals("125", toString(s, "SELECT IV FROM T WHERE ID = 125"));
            assertEquals("0", toString(s, "SELECT IV FROM T WHERE ID = 0"));
            assertEquals(pad + "199", toString(s, "SELECT V FROM T WHERE ID = 199"));
            // 逐出段上的更新与删除。
            s.execute("UPDATE T SET IV = -1 WHERE ID = 125");
            assertEquals("-1", toString(s, "SELECT IV FROM T WHERE ID = 125"));
            s.execute("DELETE FROM T WHERE ID = 125");
            assertEquals("199", toString(s, "SELECT COUNT(*) FROM T"));
        }
        // 重启后同样正确。
        try (Connection c = DriverManager.getConnection(url, "sa", "");
                Statement s = c.createStatement()) {
            assertEquals("199", toString(s, "SELECT COUNT(*) FROM T"));
            assertEquals("19775", toString(s, "SELECT SUM(ID) FROM T"));
            // 删除过的行不再命中。
            try (java.sql.ResultSet rs = s.executeQuery("SELECT IV FROM T WHERE ID = 125")) {
                assertFalse(rs.next());
            }
        }
    }

    /**
     * residentBytes 的单位后缀：k/m/g 不区分大小写，按 1024 换算；不带单位即字节；
     * 非法后缀建表即报错。3 列表形每段约 15 KB：30k=30720 容两段容不下三段，
     * 若被误解析为 30000 则只容一段，驻留集可以区分两者。
     */
    @Test
    void residentBytesAcceptsUnitSuffixes(@TempDir Path dir) throws Exception {
        String pad = "v".repeat(60);
        try (Connection c = DriverManager.getConnection(
                "jdbc:h2:" + dir.resolve("sfx"), "sa", "");
                Statement s = c.createStatement()) {
            // 逐出只在文件模式库生效，本用例必须建在文件库上。
            s.execute(createTable("T_SFX", "ID INT PRIMARY KEY, V VARCHAR(64), IV INT",
                    "\"segmentSize=50\", \"compactionMinRows=100000\", \"residentBytes=30k\""));
            for (int i = 0; i < 200; i++) {
                s.execute("INSERT INTO T_SFX VALUES (" + i + ", '" + pad + i + "', " + i + ")");
            }
            org.h2.engine.SessionLocal session = (org.h2.engine.SessionLocal)
                    ((org.h2.jdbc.JdbcConnection) c).getSession();
            com.hyd.h2omap.table.OmapTable t30k = (com.hyd.h2omap.table.OmapTable)
                    session.getDatabase().getMainSchema().findTableOrView(session, "T_SFX");
            // 30k = 30720：最新两段驻留（误解析为 30000 时只有一段驻留）。
            assertTrue(t30k.getStore().isSegmentResident(3), "30k 应容最新两段驻留");
            assertTrue(t30k.getStore().isSegmentResident(2), "30k 应容两段驻留，与 30000 可区分");
            assertFalse(t30k.getStore().isSegmentResident(0), "30k 应把最老的段逐出");
            assertEquals("200", toString(s, "SELECT COUNT(*) FROM T_SFX"));

            s.execute(createTable("T_SFX2", "ID INT PRIMARY KEY, V VARCHAR(64), IV INT",
                    "\"segmentSize=50\", \"compactionMinRows=100000\", \"residentBytes=8G\""));
            for (int i = 0; i < 200; i++) {
                s.execute("INSERT INTO T_SFX2 VALUES (" + i + ", '" + pad + i + "', " + i + ")");
            }
            com.hyd.h2omap.table.OmapTable t8g = (com.hyd.h2omap.table.OmapTable)
                    session.getDatabase().getMainSchema().findTableOrView(session, "T_SFX2");
            assertEquals(0, t8g.getStore().getEvictedSegmentCount(), "8G 预算下不应逐出");

            // 非法后缀与缺失数字都在建表时报错，不会静默忽略。
            assertThrows(SQLException.class, () -> s.execute(createTable("T_SFX3", "ID INT",
                    "\"residentBytes=8x\"")));
            assertThrows(SQLException.class, () -> s.execute(createTable("T_SFX4", "ID INT",
                    "\"residentBytes=k\"")));
        }
    }

    /**
     * ALTER TABLE SET RESIDENT_BYTES：调小立即逐出、查询仍正确（定界与部分列读）；
     * 调大后向量化查询不填驻留，行式路径触达的段按新预算读回驻留（不重启预载）；
     * 改动同步进 H2 目录的建表 SQL，重开库后按新值生效。
     */
    @Test
    void alterTableSetResidentBytesAppliesAndPersists(@TempDir Path dir) throws Exception {
        String url = "jdbc:h2:" + dir.resolve("alterres");
        String pad = "v".repeat(60);
        try (Connection c = DriverManager.getConnection(url, "sa", "");
                Statement s = c.createStatement()) {
            s.execute(createTable("T", "ID INT PRIMARY KEY, V VARCHAR(64), IV INT",
                    "\"segmentSize=50\", \"compactionMinRows=100000\""));
            for (int i = 0; i < 200; i++) {
                s.execute("INSERT INTO T VALUES (" + i + ", '" + pad + i + "', " + i + ")");
            }
            org.h2.engine.SessionLocal session = (org.h2.engine.SessionLocal)
                    ((org.h2.jdbc.JdbcConnection) c).getSession();
            com.hyd.h2omap.table.OmapTable t = (com.hyd.h2omap.table.OmapTable)
                    session.getDatabase().getMainSchema().findTableOrView(session, "T");
            assertTrue(t.getStore().isSegmentResident(0), "无预算时全部驻留");
            assertTrue(t.getStore().isSegmentResident(3), "无预算时全部驻留");

            // 调小到 1：立即逐出全部段。
            s.execute("ALTER TABLE T SET RESIDENT_BYTES '1'");
            assertFalse(t.getStore().isSegmentResident(0), "预算 1 应立即逐出");
            assertFalse(t.getStore().isSegmentResident(3), "预算 1 应立即逐出");
            assertEquals("200", toString(s, "SELECT COUNT(*) FROM T"));
            assertEquals("19900", toString(s, "SELECT SUM(ID) FROM T"));
            assertEquals("125", toString(s, "SELECT IV FROM T WHERE ID = 125"));
            String sql = createTableSqlFromScript(s, "T");
            assertTrue(sql.contains("residentBytes=1"), "目录 SQL 应含新预算: " + sql);
            assertTrue(sql.contains("segmentSize=50"), "其他 WITH 参数保留: " + sql);

            // 调大到 400k（= 形式）：向量化查询走骨架定界与部分列读，不填驻留；
            // 行式路径触达的段按新预算读回驻留（不重启预载）。
            // 注意 COUNT(*) 在预算 1 阶段已执行过且数据未变，会命中 H2 结果缓存，
            // 不会触碰存储层；用没执行过的新查询触发重载。
            s.execute("ALTER TABLE T SET RESIDENT_BYTES = '400k'");
            assertEquals("199", toString(s, "SELECT MAX(ID) FROM T"));
            assertFalse(t.getStore().isSegmentResident(0), "定界求值不进驻留模型");
            assertEquals(200, countRows(s, "SELECT DISTINCT V FROM T"));
            assertTrue(t.getStore().isSegmentResident(0), "行式路径触达后按新预算驻留");
            assertTrue(t.getStore().isSegmentResident(3), "行式路径触达后按新预算驻留");
            assertEquals("200", toString(s, "SELECT COUNT(*) FROM T"));
            sql = createTableSqlFromScript(s, "T");
            assertTrue(sql.contains("residentBytes=409600"), "单位后缀按 1024 换算: " + sql);
        }
        // 重开库：预算随建表 SQL 持久化，行为一致。
        try (Connection c = DriverManager.getConnection(url, "sa", "");
                Statement s = c.createStatement()) {
            String sql = createTableSqlFromScript(s, "T");
            assertTrue(sql.contains("residentBytes=409600"), "重开后仍应保持新预算: " + sql);
            assertEquals("200", toString(s, "SELECT COUNT(*) FROM T"));
            assertEquals("19900", toString(s, "SELECT SUM(ID) FROM T"));
            assertEquals(pad + "199", toString(s, "SELECT V FROM T WHERE ID = 199"));
        }
    }

    /**
     * 建表时没有 WITH 子句的表：TableBase 持有不可变空参数列表，新预算只能在
     * 渲染结果末尾追加 WITH 子句；重开后走参数列表路径，行为一致。
     */
    @Test
    void alterTableSetResidentBytesWithoutWithClausePersists(@TempDir Path dir) throws Exception {
        String url = "jdbc:h2:" + dir.resolve("alternowith");
        try (Connection c = DriverManager.getConnection(url, "sa", "");
                Statement s = c.createStatement()) {
            s.execute(createTable("T", "ID INT PRIMARY KEY, V INT", null));
            for (int i = 0; i < 100; i++) {
                s.execute("INSERT INTO T VALUES (" + i + ", " + i + ")");
            }
            s.execute("ALTER TABLE T SET RESIDENT_BYTES '1'");
            assertEquals("100", toString(s, "SELECT COUNT(*) FROM T"));
            String sql = createTableSqlFromScript(s, "T");
            assertTrue(sql.contains("residentBytes=1"), "无 WITH 子句的表也应持久化新参数: " + sql);
        }
        try (Connection c = DriverManager.getConnection(url, "sa", "");
                Statement s = c.createStatement()) {
            String sql = createTableSqlFromScript(s, "T");
            assertTrue(sql.contains("residentBytes=1"), "重开后仍应保持新参数: " + sql);
            assertEquals("100", toString(s, "SELECT COUNT(*) FROM T"));
            assertEquals("4950", toString(s, "SELECT SUM(ID) FROM T"));
        }
    }

    /**
     * ALTER TABLE SET RESIDENT_BYTES 的拒绝路径：非 OMAP 表、非法后缀、NULL 值都报错；
     * IF EXISTS 的缺失表是空操作。
     */
    @Test
    void alterTableSetResidentBytesRejectsBadTargets() throws Exception {
        execute(createTable("T", "ID INT", null));
        execute("CREATE TABLE T_MV(ID INT) ENGINE \"org.h2.mvstore.db.Store\"");
        assertThrows(SQLException.class, () -> execute("ALTER TABLE T_MV SET RESIDENT_BYTES '1'"));
        assertThrows(SQLException.class, () -> execute("ALTER TABLE T SET RESIDENT_BYTES '40x'"));
        assertThrows(SQLException.class, () -> execute("ALTER TABLE T SET RESIDENT_BYTES NULL"));
        execute("ALTER TABLE IF EXISTS T_NOSUCH SET RESIDENT_BYTES '1'");
    }

    /**
     * SET OMAP_SLICE_CACHE_BYTES：数据库全局变量设置逐出段列切片缓存预算，对库内全部
     * OMAP 表生效；默认 32 MiB 且不接受更小值（32768k 恰为下限，允许）；带后缀的值写成
     * 字符串字面量、纯字节数可不加引号、等号可选；值以规范化字节数持久化进 H2 目录，
     * 重开库重放 SET 后新表构造时读到同一预算。
     */
    @Test
    void setOmapSliceCacheBytesIsGlobalWithLowerBoundAndPersists(@TempDir Path dir) throws Exception {
        String url = "jdbc:h2:" + dir.resolve("slicecache");
        try (Connection c = DriverManager.getConnection(url, "sa", "");
                Statement s = c.createStatement()) {
            s.execute(createTable("T", "ID INT", "\"segmentSize=4\""));
            org.h2.engine.SessionLocal session = (org.h2.engine.SessionLocal)
                    ((org.h2.jdbc.JdbcConnection) c).getSession();
            com.hyd.h2omap.table.OmapTable t = (com.hyd.h2omap.table.OmapTable)
                    session.getDatabase().getMainSchema().findTableOrView(session, "T");
            assertEquals(32L * 1024 * 1024, t.getStore().getSliceCacheBudget(),
                    "未设置过时应为默认 32 MiB");

            s.execute("SET OMAP_SLICE_CACHE_BYTES '32768k'");
            assertEquals(32L * 1024 * 1024, t.getStore().getSliceCacheBudget(),
                    "32768k 恰为下限，应允许");
            s.execute("SET OMAP_SLICE_CACHE_BYTES = 33554432");
            s.execute("SET OMAP_SLICE_CACHE_BYTES '64m'");
            assertEquals(64L * 1024 * 1024, t.getStore().getSliceCacheBudget());

            assertThrows(SQLException.class, () -> s.execute("SET OMAP_SLICE_CACHE_BYTES '10m'"),
                    "小于 32 MiB 应报错");
            assertThrows(SQLException.class, () -> s.execute("SET OMAP_SLICE_CACHE_BYTES '40x'"),
                    "非法后缀应报错");
            assertThrows(SQLException.class, () -> s.execute("SET OMAP_SLICE_CACHE_BYTES NULL"));
            assertEquals(64L * 1024 * 1024, t.getStore().getSliceCacheBudget(),
                    "被拒绝的语句不应改变预算");
            assertEquals("67108864", toString(s, "SELECT SETTING_VALUE FROM INFORMATION_SCHEMA.SETTINGS"
                    + " WHERE SETTING_NAME = 'OMAP_SLICE_CACHE_BYTES'"),
                    "值应以规范化字节数进目录");
        }
        try (Connection c = DriverManager.getConnection(url, "sa", "");
                Statement s = c.createStatement()) {
            s.execute(createTable("T2", "ID INT", "\"segmentSize=4\""));
            org.h2.engine.SessionLocal session = (org.h2.engine.SessionLocal)
                    ((org.h2.jdbc.JdbcConnection) c).getSession();
            com.hyd.h2omap.table.OmapTable t2 = (com.hyd.h2omap.table.OmapTable)
                    session.getDatabase().getMainSchema().findTableOrView(session, "T2");
            assertEquals(64L * 1024 * 1024, t2.getStore().getSliceCacheBudget(),
                    "重开库后新表应从目录读到 64 MiB");
        }
    }

    private static void assertQueriesMatch(Statement s, String[] templates) throws SQLException {
        for (String template : templates) {
            String expected = collectRows(s, String.format(template, "T_MV"));
            String actual = collectRows(s, String.format(template, "T"));
            assertEquals(expected, actual, "Mismatch on query: " + template);
        }
    }

    /**
     * 随机散布写压力用例：更新、删除、插入均匀散到全部段，每轮与 MVStore 逐查询比对，
     * 并断言无写的第二遍重复聚合命中段级部分聚合缓存。
     * 这条用例防止后续批次把失效/回写/缓存机制窄化到「只改最老几段」的场景。
     */
    @Test
    void randomSpreadWritesKeepResultsCorrect() throws Exception {
        try (Connection c = DriverManager.getConnection(
                "jdbc:h2:mem:omaprand;DB_CLOSE_DELAY=-1;QUERY_CACHE_SIZE=0", "sa", "");
                Statement s = c.createStatement()) {
            s.execute("CREATE TABLE T (ID INT PRIMARY KEY, CAT VARCHAR(8), IV INT, DV DOUBLE)"
                    + " WITH \"segmentSize=64\", \"compactionMinRows=96\"");
            s.execute("CREATE TABLE T_MV (ID INT PRIMARY KEY, CAT VARCHAR(8), IV INT, DV DOUBLE)"
                    + " ENGINE \"org.h2.mvstore.db.Store\"");
            java.util.Random rnd = new java.util.Random(20261120);
            java.util.Set<Integer> live = new java.util.TreeSet<>();
            for (int i = 1; i <= 512; i++) {
                insertRandomRow(s, i, rnd);
                live.add(i);
            }
            org.h2.engine.SessionLocal session = (org.h2.engine.SessionLocal)
                    ((org.h2.jdbc.JdbcConnection) c).getSession();
            com.hyd.h2omap.table.OmapTable table = (com.hyd.h2omap.table.OmapTable)
                    session.getDatabase().getMainSchema().findTableOrView(session, "T");
            com.hyd.h2omap.vectorized.SegmentAggCache cache = table.getStore().getAggCache();

            String[] battery = {
                    "SELECT COUNT(*), SUM(IV), MIN(IV), MAX(IV), AVG(IV) FROM %s WHERE IV >= 0",
                    "SELECT CAT, COUNT(*), SUM(IV) FROM %s GROUP BY CAT ORDER BY CAT",
                    "SELECT CAT, SUM(IV) FROM %s WHERE IV > 100 GROUP BY CAT ORDER BY CAT",
                    "SELECT COUNT(*) FROM %s WHERE IV > 50 AND IV < 400",
                    "SELECT DISTINCT CAT FROM %s ORDER BY CAT",
                    "SELECT COUNT(DISTINCT CAT) FROM %s",
            };
            int maxId = 512;
            for (int round = 0; round < 6; round++) {
                java.util.List<Integer> liveList = new java.util.ArrayList<>(live);
                for (int op = 0; op < 40; op++) {
                    int kind = rnd.nextInt(10);
                    int id = liveList.get(rnd.nextInt(liveList.size()));
                    if (kind < 5) {
                        int iv = rnd.nextInt(1000);
                        String cat = "c" + rnd.nextInt(8);
                        for (String t : new String[] { "T", "T_MV" }) {
                            s.execute("UPDATE " + t + " SET IV = " + iv + ", CAT = '" + cat
                                    + "' WHERE ID = " + id);
                        }
                    } else if (kind < 8) {
                        for (String t : new String[] { "T", "T_MV" }) {
                            s.execute("DELETE FROM " + t + " WHERE ID = " + id);
                        }
                        live.remove(id);
                        liveList.remove(Integer.valueOf(id));
                    } else {
                        maxId++;
                        insertRandomRow(s, maxId, rnd);
                        live.add(maxId);
                        liveList.add(maxId);
                    }
                }
                assertQueriesMatch(s, battery);
                long hitsAfterFill = cache.hitCount();
                assertQueriesMatch(s, battery);
                assertTrue(cache.hitCount() > hitsAfterFill,
                        "无写的第二遍应命中段级部分聚合缓存，实际命中增量 "
                                + (cache.hitCount() - hitsAfterFill));
                // 点查走 rowId 索引与物化路径，同样逐条与 MVStore 比对。
                for (int k = 0; k < 5; k++) {
                    int id = liveList.get(rnd.nextInt(liveList.size()));
                    assertEquals(toString(s, "SELECT IV, CAT FROM T_MV WHERE ID = " + id),
                            toString(s, "SELECT IV, CAT FROM T WHERE ID = " + id));
                }
            }
            assertEquals(Integer.toString(live.size()),
                    toString(s, "SELECT COUNT(*) FROM T"));
        }
    }

    private static void insertRandomRow(Statement s, int id, java.util.Random rnd) throws SQLException {
        String vals = id + ", 'c" + rnd.nextInt(8) + "', " + rnd.nextInt(1000) + ", "
                + (rnd.nextInt(2000) * 0.5);
        s.execute("INSERT INTO T VALUES (" + vals + ")");
        s.execute("INSERT INTO T_MV VALUES (" + vals + ")");
    }

    /**
     * 段级逐出用例：预算 6000 字节只容得下一个段，逐出后行仍可见，
     * 命中时同步读回；逐出顺序按访问时间而非段号新老。
     */
    @Test
    void segmentEvictionKeepsRowsVisibleAndReloads(@TempDir Path dir) throws Exception {
        String url = "jdbc:h2:" + dir.resolve("evict");
        // 60 字符的变长列使每段估算内存稳定在约 15 KB，预算 40000 恰好容两段容不下三段；
        // 边界相切（如 ID < 50 会连带段 1）时两段都能驻留，逐出顺序仍可判定。
        String pad = "v".repeat(60);
        try (Connection c = DriverManager.getConnection(url, "sa", "");
                Statement s = c.createStatement()) {
            s.execute(createTable("T", "ID INT PRIMARY KEY, V VARCHAR(64), IV INT",
                    "\"segmentSize=50\", \"compactionMinRows=100000\", \"residentBytes=40000\""));
            for (int i = 0; i < 400; i++) {
                s.execute("INSERT INTO T VALUES (" + i + ", '" + pad + i + "', " + i + ")");
            }
            org.h2.engine.SessionLocal session = (org.h2.engine.SessionLocal)
                    ((org.h2.jdbc.JdbcConnection) c).getSession();
            com.hyd.h2omap.table.OmapTable table = (com.hyd.h2omap.table.OmapTable)
                    session.getDatabase().getMainSchema().findTableOrView(session, "T");
            com.hyd.h2omap.storage.ColumnarStore store = table.getStore();

            // 400 行 = 8 个段，预算只容一段：除最新段外全部逐出。
            assertTrue(store.getEvictedSegmentCount() >= 5, "应发生多次逐出");
            assertTrue(store.getResidentBytes() <= 40000, "驻留字节不应超过预算");
            assertFalse(store.isSegmentResident(0), "最老的段不应驻留");
            assertEquals("400", toString(s, "SELECT COUNT(*) FROM T"));
            assertEquals("79800", toString(s, "SELECT SUM(ID) FROM T"));

            // 向量化聚合走部分列读：结果正确，但命中的段 0/5 不进驻留，驻留集不变。
            assertEquals("1225", toString(s, "SELECT SUM(ID) FROM T WHERE ID < 50"));
            assertFalse(store.isSegmentResident(0), "部分列读不进驻留模型");
            assertEquals("13725", toString(s, "SELECT SUM(ID) FROM T WHERE ID >= 250 AND ID < 300"));
            assertFalse(store.isSegmentResident(5), "部分列读不进驻留模型");

            // 行式路径（DISTINCT 不下推）命中的段读回并驻留。
            assertEquals(50, countRows(s, "SELECT DISTINCT V FROM T WHERE ID < 50"));
            assertTrue(store.isSegmentResident(0), "行式路径命中的段应读回驻留");
            assertEquals(50, countRows(s, "SELECT DISTINCT V FROM T WHERE ID >= 250 AND ID < 300"));
            assertTrue(store.isSegmentResident(5), "行式路径命中的段应读回驻留");

            // 再触碰段 2（ID 100..149）：上一轮刚访问的段 0 比段 5 更久未访问，先让位；
            // 这证明逐出按访问时间而非段号新老。
            assertEquals(50, countRows(s, "SELECT DISTINCT V FROM T WHERE ID >= 100 AND ID < 150"));
            assertTrue(store.isSegmentResident(2));
            assertFalse(store.isSegmentResident(0), "访问时间更早的段应先被逐出");

            // 行式路径（DISTINCT）与逐出段上的点查、更新、删除。
            assertEquals(50, countRows(s, "SELECT DISTINCT V FROM T WHERE ID >= 100 AND ID < 150"));
            s.execute("UPDATE T SET IV = -1 WHERE ID = 125");
            assertEquals("-1", toString(s, "SELECT IV FROM T WHERE ID = 125"));
            s.execute("DELETE FROM T WHERE ID IN (100, 101)");
            assertEquals("398", toString(s, "SELECT COUNT(*) FROM T"));
        }
        // 重启：恢复收尾把驻留裁回预算（最老先出），重开后数据与更新都完整。
        try (Connection c = DriverManager.getConnection(url, "sa", "");
                Statement s = c.createStatement()) {
            org.h2.engine.SessionLocal session = (org.h2.engine.SessionLocal)
                    ((org.h2.jdbc.JdbcConnection) c).getSession();
            com.hyd.h2omap.table.OmapTable table = (com.hyd.h2omap.table.OmapTable)
                    session.getDatabase().getMainSchema().findTableOrView(session, "T");
            com.hyd.h2omap.storage.ColumnarStore store = table.getStore();
            assertFalse(store.isSegmentResident(0), "恢复后最老的段先被裁出预算");
            assertTrue(store.isSegmentResident(7), "最新的段应驻留");
            assertEquals("398", toString(s, "SELECT COUNT(*) FROM T"));
            assertEquals("79599", toString(s, "SELECT SUM(ID) FROM T"));
            assertEquals("-1", toString(s, "SELECT IV FROM T WHERE ID = 125"));
        }
    }

    /**
     * 裸 MIN/MAX 的骨架定界：段按目标方向的 zone 边界排序后依次扫描，命中即停。
     * 逐出段只读目标列，不进驻留模型、不改变逐出计数；zone 边界含已删行
     * （delta 删除未回写段）时逐段推进仍精确；delta 新像与开放段的行无条件参与。
     * ID 与 TS 随插入递增（聚簇同向），IV 逆向分布（定界须跳过值域更大的老段）。
     */
    @Test
    void bareExtremeStopsAtZoneBounds(@TempDir Path dir) throws Exception {
        String url = "jdbc:h2:" + dir.resolve("extreme");
        String pad = "v".repeat(60);
        try (Connection c = DriverManager.getConnection(url, "sa", "");
                Statement s = c.createStatement()) {
            s.execute(createTable("T", "ID INT, TS TIMESTAMP, V VARCHAR(64), IV INT",
                    "\"segmentSize=50\", \"compactionMinRows=100000\", \"residentBytes=40000\""));
            for (int i = 0; i < 400; i++) {
                s.execute("INSERT INTO T VALUES (" + i
                        + ", TIMESTAMPADD(MINUTE, " + i + ", TIMESTAMP '2026-01-01 00:00:00')"
                        + ", '" + pad + i + "', " + (399 - i) + ")");
            }
            org.h2.engine.SessionLocal session = (org.h2.engine.SessionLocal)
                    ((org.h2.jdbc.JdbcConnection) c).getSession();
            com.hyd.h2omap.storage.ColumnarStore store =
                    ((com.hyd.h2omap.table.OmapTable) session.getDatabase().getMainSchema()
                            .findTableOrView(session, "T")).getStore();
            assertFalse(store.isSegmentResident(0), "最老的段应已逐出");
            assertFalse(store.isSegmentResident(5), "中段应已逐出");
            long evictedBefore = store.getEvictedSegmentCount();

            // 聚簇同向：MIN 只扫首段；逆向列：MIN/MAX 分别只扫值域最小的段。
            long scans = store.getExtremeScannedSegments();
            assertEquals("0", toString(s, "SELECT MIN(ID) FROM T"));
            assertEquals(1, store.getExtremeScannedSegments() - scans, "MIN(ID) 只扫一个候选段");
            scans = store.getExtremeScannedSegments();
            assertEquals("399", toString(s, "SELECT MAX(ID) FROM T"));
            assertEquals(1, store.getExtremeScannedSegments() - scans, "MAX(ID) 只扫一个候选段");
            scans = store.getExtremeScannedSegments();
            assertEquals("0", toString(s, "SELECT MIN(IV) FROM T"));
            assertEquals(1, store.getExtremeScannedSegments() - scans, "逆向列 MIN 只扫值域最小的段");
            scans = store.getExtremeScannedSegments();
            assertEquals("399", toString(s, "SELECT MAX(IV) FROM T"));
            assertEquals(1, store.getExtremeScannedSegments() - scans, "逆向列 MAX 只扫值域最大的段");
            try (java.sql.ResultSet rs = s.executeQuery("SELECT MIN(TS) FROM T")) {
                assertTrue(rs.next());
                assertEquals(java.sql.Timestamp.valueOf("2026-01-01 00:00:00").getTime(),
                        rs.getTimestamp(1).getTime());
            }
            assertFalse(store.isSegmentResident(0), "定界求值不进驻留模型");
            assertFalse(store.isSegmentResident(5), "定界求值不进驻留模型");
            assertEquals(evictedBefore, store.getEvictedSegmentCount(), "定界求值不触发逐出");

            // zone 边界含已删行：删掉段 0 的最小行后 MIN 仍来自段 0（delta 删除参与定界）。
            s.execute("DELETE FROM T WHERE ID = 0");
            scans = store.getExtremeScannedSegments();
            assertEquals("1", toString(s, "SELECT MIN(ID) FROM T"));
            assertEquals(1, store.getExtremeScannedSegments() - scans, "删掉最小行后仍只扫段 0");
            // 整段删空触发整段摘除：MIN 落到段 1，候选只剩一段。
            s.execute("DELETE FROM T WHERE ID < 50");
            scans = store.getExtremeScannedSegments();
            assertEquals("50", toString(s, "SELECT MIN(ID) FROM T"));
            assertEquals(1, store.getExtremeScannedSegments() - scans, "整段摘除后只扫段 1");

            // delta 新像与开放段的行参与定界；被更新行的旧像不参与。
            s.execute("UPDATE T SET ID = 1000 WHERE ID = 50");
            assertEquals("51", toString(s, "SELECT MIN(ID) FROM T"));
            s.execute("UPDATE T SET ID = -5 WHERE ID = 52");
            assertEquals("-5", toString(s, "SELECT MIN(ID) FROM T"));
            s.execute("INSERT INTO T VALUES (-100, TIMESTAMP '2025-12-31 23:00:00', 'x', 400)");
            assertEquals("-100", toString(s, "SELECT MIN(ID) FROM T"));
            try (java.sql.ResultSet rs = s.executeQuery("SELECT MIN(TS) FROM T")) {
                assertTrue(rs.next());
                assertEquals(java.sql.Timestamp.valueOf("2025-12-31 23:00:00").getTime(),
                        rs.getTimestamp(1).getTime());
            }
            assertEquals("351", toString(s, "SELECT COUNT(*) FROM T"));
        }
    }

    /**
     * 向量化查询对逐出段走部分列读：命中段不进驻留模型、不触发 LRU 逐出；
     * 全表聚合、过滤聚合、点查投影与裸 MAX 的结果与全驻留一致（含 delta 删除与更新）；
     * 行式路径（DISTINCT）经驻留通道逐段读回，扫描集超预算时先扫的段按 LRU 让位；重开后同样的查询仍正确。
     */
    @Test
    void evictedVectorizedQueriesUsePartialColumnReads(@TempDir Path dir) throws Exception {
        String url = "jdbc:h2:" + dir.resolve("partial");
        String pad = "v".repeat(60);
        try (Connection c = DriverManager.getConnection(url, "sa", "");
                Statement s = c.createStatement()) {
            s.execute(createTable("T", "ID INT, V VARCHAR(64), IV INT, CAT VARCHAR(8), TS TIMESTAMP",
                    "\"segmentSize=50\", \"compactionMinRows=100000\", \"residentBytes=40000\""));
            for (int i = 0; i < 400; i++) {
                s.execute("INSERT INTO T VALUES (" + i + ", '" + pad + i + "', " + i
                        + ", 'c" + (i % 4) + "', TIMESTAMPADD(MINUTE, " + i
                        + ", TIMESTAMP '2026-01-01 00:00:00'))");
            }
            org.h2.engine.SessionLocal session = (org.h2.engine.SessionLocal)
                    ((org.h2.jdbc.JdbcConnection) c).getSession();
            com.hyd.h2omap.storage.ColumnarStore store =
                    ((com.hyd.h2omap.table.OmapTable) session.getDatabase().getMainSchema()
                            .findTableOrView(session, "T")).getStore();
            assertFalse(store.isSegmentResident(0), "最老的段应已逐出");
            assertFalse(store.isSegmentResident(5), "中段应已逐出");
            long evictedBefore = store.getEvictedSegmentCount();

            // 全表与过滤聚合、点查投影、裸 MAX：命中段走部分列读。
            assertEquals("400", toString(s, "SELECT COUNT(*) FROM T"));
            assertEquals("79800", toString(s, "SELECT SUM(ID) FROM T"));
            assertEquals("100", toString(s, "SELECT COUNT(*) FROM T WHERE ID < 100"));
            assertEquals("4950", toString(s, "SELECT SUM(ID) FROM T WHERE ID < 100"));
            assertEquals("100", toString(s, "SELECT COUNT(*) FROM T WHERE CAT = 'c1'"));
            assertEquals("19900", toString(s, "SELECT SUM(ID) FROM T WHERE CAT = 'c1'"));
            assertEquals("399", toString(s, "SELECT MAX(ID) FROM T"));
            assertEquals("55", toString(s, "SELECT ID FROM T WHERE ID = 55"));
            assertEquals(pad + "55", toString(s, "SELECT V FROM T WHERE ID = 55"));
            assertEquals(10, countRows(s, "SELECT V FROM T WHERE ID >= 50 AND ID < 60"));
            assertFalse(store.isSegmentResident(0), "部分列读不进驻留模型");
            assertFalse(store.isSegmentResident(5), "部分列读不进驻留模型");
            assertEquals(evictedBefore, store.getEvictedSegmentCount(), "部分列读不触发逐出");
            assertTrue(store.getTransientSliceReads() > 0, "向量化命中逐出段应走部分列读");
            assertEquals(0, store.getTransientFullReads(), "向量化路径不应误走整段临时读");

            // delta 删除与更新在部分列读上仍精确；行式扫描（DELETE/UPDATE）按
            // 预算可临时整段读，这里只约束向量化查询仍走切片读。
            long sliceBefore = store.getTransientSliceReads();
            s.execute("DELETE FROM T WHERE ID IN (0, 1)");
            assertEquals("398", toString(s, "SELECT COUNT(*) FROM T"));
            assertEquals("79799", toString(s, "SELECT SUM(ID) FROM T"));
            s.execute("UPDATE T SET IV = -1 WHERE ID = 100");
            assertEquals("6124", toString(s, "SELECT SUM(IV) FROM T WHERE ID >= 100 AND ID < 150"));
            assertTrue(store.getTransientSliceReads() > sliceBefore,
                    "delta 改动后的向量化查询仍走部分列读");

            // 行式路径（DISTINCT 不下推）经驻留通道逐段读回，与部分列读形成对照：
            // 读回段进入驻留模型由 LRU 管理，扫描集超出预算时先扫的段被后扫的段挤出，
            // 但全程不走临时读。ID 0 与 1 已删：段 0 剩 48 行；段 2 因 delta 更新无条件参与扫描。
            long fullBefore = store.getTransientFullReads();
            assertEquals(48, countRows(s, "SELECT DISTINCT V FROM T WHERE ID < 50"));
            assertEquals(fullBefore, store.getTransientFullReads(), "行式路径走驻留读回而非临时读");
            assertTrue(store.isSegmentResident(2), "行式扫描最后读回的段保持驻留");
        }
        // 重开：骨架可见性不变，同样的查询仍正确。
        try (Connection c = DriverManager.getConnection(url, "sa", "");
                Statement s = c.createStatement()) {
            assertEquals("398", toString(s, "SELECT COUNT(*) FROM T"));
            assertEquals("79799", toString(s, "SELECT SUM(ID) FROM T"));
            assertEquals("6124", toString(s, "SELECT SUM(IV) FROM T WHERE ID >= 100 AND ID < 150"));
            assertEquals("399", toString(s, "SELECT MAX(ID) FROM T"));
        }
    }

    /**
     * 无 ORDER BY 的 LIMIT 优先扫已驻留段：驻留段可见行数足够时零磁盘读；
     * 不足时才按需读回逐出段补足差额（懒加载段列向量），读回不进驻留模型、不触发 LRU 逐出。
     */
    @Test
    void unorderedLimitPrefersResidentSegments(@TempDir Path dir) throws Exception {
        String url = "jdbc:h2:" + dir.resolve("unorderedlimit");
        String pad = "v".repeat(60);
        try (Connection c = DriverManager.getConnection(url, "sa", "");
                Statement s = c.createStatement()) {
            s.execute(createTable("T", "ID INT, V VARCHAR(64), CAT VARCHAR(8), TS TIMESTAMP",
                    "\"segmentSize=50\", \"compactionMinRows=100000\", \"residentBytes=80000\""));
            for (int i = 0; i < 400; i++) {
                s.execute("INSERT INTO T VALUES (" + i + ", '" + pad + i + "', 'c" + (i % 4)
                        + "', TIMESTAMPADD(MINUTE, " + i
                        + ", TIMESTAMP '2026-01-01 00:00:00'))");
            }
            org.h2.engine.SessionLocal session = (org.h2.engine.SessionLocal)
                    ((org.h2.jdbc.JdbcConnection) c).getSession();
            com.hyd.h2omap.storage.ColumnarStore store =
                    ((com.hyd.h2omap.table.OmapTable) session.getDatabase().getMainSchema()
                            .findTableOrView(session, "T")).getStore();
            int minResident = -1;
            long residentRows = 0;
            for (int seg = 0; seg <= 7; seg++) {
                if (store.isSegmentResident(seg)) {
                    if (minResident < 0) {
                        minResident = seg;
                    }
                    residentRows += 50;
                }
            }
            assertTrue(minResident > 0, "应有驻留段且最老段已逐出");
            assertTrue(residentRows >= 100, "预算应保证至少两段驻留");

            // 驻留段行数足够：零磁盘读，且返回行全部来自驻留段。
            long sliceBefore = store.getTransientSliceReads();
            long fullBefore = store.getTransientFullReads();
            long evictedBefore = store.getEvictedSegmentCount();
            try (ResultSet rs = s.executeQuery("SELECT ID FROM T LIMIT 100")) {
                int n = 0;
                int minId = Integer.MAX_VALUE;
                while (rs.next()) {
                    n++;
                    minId = Math.min(minId, rs.getInt(1));
                }
                assertEquals(100, n);
                assertTrue(minId >= minResident * 50, "LIMIT 100 全部来自驻留段");
            }
            assertEquals(sliceBefore, store.getTransientSliceReads(), "驻留段足够时零磁盘读");
            assertEquals(fullBefore, store.getTransientFullReads(), "驻留段足够时零磁盘读");
            assertEquals(evictedBefore, store.getEvictedSegmentCount(), "冷查询不触发逐出");

            // 驻留行数不足：按需读回逐出段补足差额，切片读增加且读回段不进驻留模型。
            try (ResultSet rs = s.executeQuery("SELECT ID FROM T LIMIT " + (residentRows + 50))) {
                int n = 0;
                while (rs.next()) {
                    n++;
                }
                assertEquals((int) residentRows + 50, n);
            }
            assertTrue(store.getTransientSliceReads() > sliceBefore, "不足时按需读回逐出段");
            assertFalse(store.isSegmentResident(minResident - 1), "读回段不进驻留模型");
            assertEquals(fullBefore, store.getTransientFullReads(), "向量化路径不整段临时读");
        }
    }

    private static int countRows(Statement s, String sql) throws SQLException {
        try (ResultSet rs = s.executeQuery(sql)) {
            int n = 0;
            while (rs.next()) {
                n++;
            }
            return n;
        }
    }

    private static String collectRows(Statement s, String sql) throws SQLException {
        StringBuilder sb = new StringBuilder();
        try (ResultSet rs = s.executeQuery(sql)) {
            int cols = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                for (int c = 1; c <= cols; c++) {
                    sb.append(rs.getString(c)).append('|');
                }
                sb.append('\n');
            }
        }
        return sb.toString();
    }

    /** SCRIPT 输出里找指定表的建表语句，验证 H2 目录里持久化的 SQL。 */
    // ------------------------------------------------------------------
    // M20：无 LIMIT 的 ORDER BY 有序流式执行
    // ------------------------------------------------------------------

    /** 主数据：3 个冻结段 + 开放段 2 行 + delta 更新新像 + 段内删除，TS 段间区间不相交。 */
    private void createOrderedStreamingTable() throws SQLException {
        execute(createTable("T", "ID INT, TS BIGINT NOT NULL, V INT",
                "\"segmentSize=4\", \"compactionMinRows=100000\""));
        execute("INSERT INTO T VALUES (1, 10, 1), (2, 5, 2), (3, 8, 1), (4, 2, 3)");
        execute("INSERT INTO T VALUES (5, 20, 9), (6, 11, 5), (7, 15, 6), (8, 13, 7)");
        execute("INSERT INTO T VALUES (9, 30, 1), (10, 25, 2), (11, 29, 3), (12, 21, 4)");
        execute("INSERT INTO T VALUES (13, 22, 6), (14, 35, 5)");
        execute("UPDATE T SET TS = 17, V = 8 WHERE ID = 6");
        execute("DELETE FROM T WHERE ID = 11");
    }

    @Test
    void orderedStreamingEmitsSortedRowsWithDeltaAndDeletes() throws SQLException {
        createOrderedStreamingTable();
        // DESC：开放段两行乱序（rowId 序 ≠ TS 序），22 落在段 [21,30] 内逐行归并；delta 新像 17 取代旧像 13；已删 29 不出现
        assertEquals(List.of(
                "14/35/5", "9/30/1", "10/25/2", "13/22/6", "12/21/4",
                "5/20/9", "6/17/8", "7/15/6", "8/13/7",
                "1/10/1", "3/8/1", "2/5/2", "4/2/3"),
                rows("SELECT ID, TS, V FROM T ORDER BY TS DESC"));
        assertEquals(List.of(
                "4/2/3", "2/5/2", "3/8/1", "1/10/1",
                "8/13/7", "7/15/6", "6/17/8", "5/20/9",
                "12/21/4", "13/22/6", "10/25/2", "9/30/1", "14/35/5"),
                rows("SELECT ID, TS, V FROM T ORDER BY TS"));
        com.hyd.h2omap.storage.ColumnarStore store = omapStore("T");
        assertTrue(store.getOrderedStreamingClaims() >= 2,
                "两条查询都应命中有序流式下推");
    }

    @Test
    void orderedStreamingMultiColumnOrderWithTiesAndNullSecondKey() throws SQLException {
        execute(createTable("T2", "ID INT, TS BIGINT NOT NULL, V INT",
                "\"segmentSize=4\", \"compactionMinRows=100000\""));
        execute("INSERT INTO T2 VALUES (1, 10, 3), (2, 10, 9), (3, 10, 5), (4, 12, 1)");
        execute("INSERT INTO T2 VALUES (5, 20, NULL), (6, 20, 8), (7, 20, 4), (8, 22, 6)");
        execute("INSERT INTO T2 VALUES (9, 30, 7)");
        // 首列区间不相交，段间顺序由 TS 决定；TS 并列时按 V DESC、NULLS LAST 定序
        assertEquals(List.of("9", "8", "6", "7", "5", "4", "2", "3", "1"),
                rows("SELECT ID FROM T2 ORDER BY TS DESC, V DESC NULLS LAST"));
        // ASC 默认 NULL 位次是 NULLS FIRST（H2 把 NULL 视作最小值）
        assertEquals(List.of("1", "3", "2", "4", "5", "7", "6", "8", "9"),
                rows("SELECT ID FROM T2 ORDER BY TS, V"));
        assertTrue(omapStore("T2").getOrderedStreamingClaims() >= 2);
    }

    @Test
    void orderedStreamingSelectStarReadsProjectionLazily() throws SQLException {
        execute(createTable("T3", "ID INT, TS TIMESTAMP NOT NULL, V INT, NAME VARCHAR",
                "\"segmentSize=2\", \"compactionMinRows=100000\""));
        execute("INSERT INTO T3 VALUES (1, TIMESTAMP '2026-01-01 00:00:10', 1, 'a')");
        execute("INSERT INTO T3 VALUES (2, TIMESTAMP '2026-01-01 00:00:05', 2, 'b')");
        execute("INSERT INTO T3 VALUES (3, TIMESTAMP '2026-01-01 00:00:30', 3, 'c')");
        execute("INSERT INTO T3 VALUES (4, TIMESTAMP '2026-01-01 00:00:25', 4, 'd')");
        execute("INSERT INTO T3 VALUES (5, TIMESTAMP '2026-01-01 00:00:35', 5, 'e')");
        com.hyd.h2omap.storage.ColumnarStore store = omapStore("T3");
        long deferredBefore = store.getDeferredProjectionReads();
        List<String> out = new ArrayList<>();
        try (Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT * FROM T3 ORDER BY TS DESC")) {
            while (rs.next()) {
                out.add(rs.getInt(1) + "/" + rs.getTimestamp(2).getTime() / 1000 + "/"
                        + rs.getInt(3) + "/" + rs.getString(4));
            }
        }
        assertEquals(List.of(
                "5/1767196835/5/e", "3/1767196830/3/c",
                "4/1767196825/4/d", "1/1767196810/1/a", "2/1767196805/2/b"), out);
        assertTrue(store.getOrderedStreamingClaims() >= 1);
        assertTrue(store.getDeferredProjectionReads() > deferredBefore,
                "SELECT * 的投影列应在行发出时才读回");
    }

    @Test
    void orderedStreamingWithWhereAndOffset() throws SQLException {
        createOrderedStreamingTable();
        // V >= 4 的行按 TS DESC：35,22,21,20,17,15,13 → OFFSET 2 跳过 35/22
        assertEquals(List.of("12", "5", "6", "7", "8"),
                rows("SELECT ID FROM T WHERE V >= 4 ORDER BY TS DESC OFFSET 2"));
        assertEquals(List.of("14", "13", "12", "5", "6", "7", "8"),
                rows("SELECT ID FROM T WHERE V >= 4 ORDER BY TS DESC"));
    }

    @Test
    void orderedStreamingAbandonedCursorStopsScanning(@TempDir Path dir) throws Exception {
        String url = "jdbc:h2:" + dir.resolve("db").toAbsolutePath().toString().replace('\\', '/');
        try (Connection c = DriverManager.getConnection(url, "sa", "")) {
            try (Statement s = c.createStatement()) {
                s.execute(createTable("T", "ID INT, TS BIGINT NOT NULL",
                        "\"segmentSize=4\", \"residentBytes=1\""));
                for (int i = 1; i <= 40; i++) {
                    s.execute("INSERT INTO T VALUES (" + i + ", " + (i * 10) + ")");
                }
            }
            org.h2.engine.SessionLocal session = (org.h2.engine.SessionLocal)
                    ((org.h2.jdbc.JdbcConnection) c).getSession();
            com.hyd.h2omap.table.OmapTable table = (com.hyd.h2omap.table.OmapTable)
                    session.getDatabase().getMainSchema().findTableOrView(session, "T");
            com.hyd.h2omap.storage.ColumnarStore store = table.getStore();
            long sliceBefore = store.getTransientSliceReads();
            long deferredBefore = store.getDeferredProjectionReads();
            long claimsBefore = store.getOrderedStreamingClaims();
            // 只取 3 行即弃游标：最新一段（4 行）足够，其余 9 个逐出段零读取
            try (Statement st = c.createStatement();
                    ResultSet rs = st.executeQuery("SELECT ID FROM T ORDER BY TS DESC")) {
                assertTrue(rs.next());
                assertEquals("40", rs.getString(1));
                assertTrue(rs.next());
                assertTrue(rs.next());
            }
            assertEquals(claimsBefore + 1, store.getOrderedStreamingClaims(),
                    "应命中有序流式下推");
            assertEquals(sliceBefore + 2, store.getTransientSliceReads(),
                    "只应读回首个段的排序列切片与投影列切片");
            assertEquals(deferredBefore + 1, store.getDeferredProjectionReads(),
                    "只有首个段产生延迟投影读回");
            // 逐出段上的多列延迟投影必须逐列对位读回（ID 与 TS 各自正确）
            List<String> full = new ArrayList<>();
            try (Statement st = c.createStatement();
                    ResultSet rs = st.executeQuery("SELECT ID, TS FROM T ORDER BY TS DESC")) {
                while (rs.next()) {
                    full.add(rs.getInt(1) + "/" + rs.getLong(2));
                }
            }
            List<String> expected = new ArrayList<>();
            for (int i = 40; i >= 1; i--) {
                expected.add(i + "/" + (i * 10));
            }
            assertEquals(expected, full);
        }
    }

    @Test
    void orderedStreamingAdjacentOverlapIsClaimedAndSorted() throws SQLException {
        execute(createTable("T4", "ID INT, TS BIGINT NOT NULL, V INT",
                "\"segmentSize=4\", \"compactionMinRows=100000\""));
        execute("INSERT INTO T4 VALUES (1, 10, 1), (2, 5, 2), (3, 8, 3), (4, 2, 4)");
        execute("INSERT INTO T4 VALUES (5, 9, 5), (6, 7, 6), (7, 10, 7), (8, 3, 8)");
        // 两段区间 [2,10] 与 [3,10] 相邻相交，次邻门控（两段时恒成立）应命中流式，
        // 10 并列按 ID 定序
        long claims = omapStore("T4").getOrderedStreamingClaims();
        assertEquals(List.of("1/10", "7/10", "5/9", "3/8", "6/7", "2/5", "8/3", "4/2"),
                rows("SELECT ID, TS FROM T4 ORDER BY TS DESC, ID"));
        assertEquals(claims + 1, omapStore("T4").getOrderedStreamingClaims(),
                "相邻段边界相交（次邻分离成立）应命中有序流式下推");
    }

    /**
     * M22 迟到带归并：相邻段边界相交时，相交带的行归入下一发射单元一起排序。
     * 段区间 [2,10]/[3,11]/[12,16]/[13,19]：seg0∩seg1 与 seg2∩seg3 相邻相交、
     * 次邻严格分离；开放段行（17:4、18:7）与 delta 新像（9:6）落在早段区间内按序归并。
     */
    @Test
    void orderedStreamingLateRowsMergeAcrossBoundaries() throws SQLException {
        execute(createTable("T5", "ID INT, TS BIGINT NOT NULL, V INT, NAME VARCHAR",
                "\"segmentSize=4\", \"compactionMinRows=100000\""));
        execute("INSERT INTO T5 VALUES (1, 10, 1, 'a'), (2, 5, 2, 'b'), (3, 8, 1, 'c'), (4, 2, 3, 'd')");
        execute("INSERT INTO T5 VALUES (5, 9, 5, 'e'), (6, 11, 6, 'f'), (7, 10, 7, 'g'), (8, 3, 8, 'h')");
        execute("INSERT INTO T5 VALUES (9, 14, 9, 'i'), (10, 15, 1, 'j'), (11, 12, 2, 'k'), (12, 16, 3, 'l')");
        execute("INSERT INTO T5 VALUES (13, 17, 4, 'm'), (14, 13, 5, 'n'), (15, 18, 6, 'o'), (16, 19, 7, 'p')");
        execute("INSERT INTO T5 VALUES (17, 4, 8, 'q'), (18, 7, 9, 'r')");
        execute("UPDATE T5 SET TS = 6 WHERE ID = 9");
        long claims = omapStore("T5").getOrderedStreamingClaims();
        assertEquals(List.of("4", "8", "17", "2", "9", "18", "3", "5", "1", "7",
                "6", "11", "14", "10", "12", "13", "15", "16"),
                rows("SELECT ID FROM T5 ORDER BY TS"));
        assertEquals(List.of("16", "15", "13", "12", "10", "14", "11", "6", "1", "7",
                "5", "3", "18", "9", "2", "17", "8", "4"),
                rows("SELECT ID FROM T5 ORDER BY TS DESC"));
        assertEquals(claims + 2, omapStore("T5").getOrderedStreamingClaims(),
                "相邻相交、次邻分离的形态两条查询都应命中流式下推");
    }

    /** 迟到带形态叠加 WHERE 与 SELECT *：单元内行来自两个段，延迟投影按行来源段读回。 */
    @Test
    void orderedStreamingLateRowsWithWhereAndSelectStar() throws SQLException {
        execute(createTable("T5B", "ID INT, TS BIGINT NOT NULL, V INT, NAME VARCHAR",
                "\"segmentSize=4\", \"compactionMinRows=100000\""));
        execute("INSERT INTO T5B VALUES (1, 10, 1, 'a'), (2, 5, 2, 'b'), (3, 8, 1, 'c'), (4, 2, 3, 'd')");
        execute("INSERT INTO T5B VALUES (5, 9, 5, 'e'), (6, 11, 6, 'f'), (7, 10, 7, 'g'), (8, 3, 8, 'h')");
        execute("INSERT INTO T5B VALUES (9, 14, 9, 'i'), (10, 15, 1, 'j'), (11, 12, 2, 'k'), (12, 16, 3, 'l')");
        execute("INSERT INTO T5B VALUES (13, 17, 4, 'm'), (14, 13, 5, 'n'), (15, 18, 6, 'o'), (16, 19, 7, 'p')");
        execute("INSERT INTO T5B VALUES (17, 4, 8, 'q'), (18, 7, 9, 'r')");
        execute("UPDATE T5B SET TS = 6 WHERE ID = 9");
        assertEquals(List.of("16", "15", "14", "6", "7", "5", "18", "9", "17", "8"),
                rows("SELECT ID FROM T5B WHERE V >= 5 ORDER BY TS DESC"));
        // SELECT *：迟到带行（1/a、2/b、3/c 来自 seg0，10/j、12/l 来自 seg2）与
        // 主区间行在同一单元内交错发射，投影列按行来源段读回
        assertEquals(List.of(
                "4/2/3/d", "8/3/8/h", "17/4/8/q", "2/5/2/b", "9/6/9/i", "18/7/9/r",
                "3/8/1/c", "5/9/5/e", "1/10/1/a", "7/10/7/g", "6/11/6/f", "11/12/2/k",
                "14/13/5/n", "10/15/1/j", "12/16/3/l", "13/17/4/m", "15/18/6/o", "16/19/7/p"),
                rows("SELECT * FROM T5B ORDER BY TS"));
        assertTrue(omapStore("T5B").getOrderedStreamingClaims() >= 2);
    }

    /** 次邻区间相交（跨两段的极迟写入）整条交回 H2。 */
    @Test
    void orderedStreamingSecondNeighborOverlapFallsBackToH2() throws SQLException {
        execute(createTable("T6", "ID INT, TS BIGINT NOT NULL, V INT",
                "\"segmentSize=2\", \"compactionMinRows=100000\""));
        execute("INSERT INTO T6 VALUES (1, 12, 1), (2, 2, 2)");
        execute("INSERT INTO T6 VALUES (3, 9, 3), (4, 3, 4)");
        execute("INSERT INTO T6 VALUES (5, 11, 5), (6, 8, 6)");
        // 发射序 seg0[2,12]、seg1[3,9]、seg2[8,11]：seg0.max=12 >= seg2.min=8，次邻相交
        long claims = omapStore("T6").getOrderedStreamingClaims();
        assertEquals(List.of("2", "4", "6", "3", "5", "1"),
                rows("SELECT ID FROM T6 ORDER BY TS"));
        assertEquals(claims, omapStore("T6").getOrderedStreamingClaims(),
                "次邻区间相交应交回 H2 行式路径，不命中流式下推");
    }

    /**
     * 次邻区间相接（边界值相等）同样拒绝：等值键跨单元时无法按 rowId 定序。
     * ASC 侧 seg2[5,10] 与 seg0[10,15] 次邻相接必须回退；
     * DESC 侧次邻严格分离（seg0.min=10 > seg2.max=9），命中流式且 10 并列按 rowId 定序。
     */
    @Test
    void orderedStreamingTouchingSecondNeighborFallsBackToH2() throws SQLException {
        execute(createTable("T7", "ID INT, TS BIGINT NOT NULL, V INT",
                "\"segmentSize=2\", \"compactionMinRows=100000\""));
        execute("INSERT INTO T7 VALUES (1, 10, 1), (2, 15, 2)");
        execute("INSERT INTO T7 VALUES (3, 8, 3), (4, 9, 4)");
        execute("INSERT INTO T7 VALUES (5, 5, 5), (6, 10, 6)");
        long claims = omapStore("T7").getOrderedStreamingClaims();
        assertEquals(List.of("5", "3", "4", "1", "6", "2"),
                rows("SELECT ID FROM T7 ORDER BY TS"));
        assertEquals(claims, omapStore("T7").getOrderedStreamingClaims(),
                "次邻区间相接（等值键跨单元）应交回 H2 行式路径");
        assertEquals(List.of("2", "1", "6", "4", "3", "5"),
                rows("SELECT ID FROM T7 ORDER BY TS DESC"));
        assertEquals(claims + 1, omapStore("T7").getOrderedStreamingClaims(),
                "DESC 侧次邻严格分离应命中流式下推");
    }

    @Test
    void orderedStreamingNullWrittenBackByCompactionFallsBack() throws SQLException {
        execute(createTable("T8", "ID INT, TS BIGINT, V INT",
                "\"segmentSize=4\", \"compactionMinRows=1\""));
        execute("INSERT INTO T8 VALUES (1, 10, 1), (2, 5, 2), (3, 8, 3), (4, 2, 4)");
        execute("INSERT INTO T8 VALUES (5, 20, 5), (6, 11, 6), (7, 15, 7), (8, 13, 8)");
        execute("INSERT INTO T8 VALUES (9, 25, 9), (10, 30, 10)");
        // 无 NULL 时命中流式
        rows("SELECT ID, TS FROM T8 ORDER BY TS DESC");
        long claims = omapStore("T8").getOrderedStreamingClaims();
        // delta 新像把 TS 改为 NULL 并同步触发合并写回段：段 hasNull 必须随之置真
        execute("UPDATE T8 SET TS = NULL WHERE ID = 6");
        assertEquals(List.of(
                "10/30", "9/25", "5/20", "7/15", "8/13", "1/10", "3/8", "2/5", "4/2", "6/null"),
                rows("SELECT ID, TS FROM T8 ORDER BY TS DESC"));
        assertEquals(claims, omapStore("T8").getOrderedStreamingClaims(),
                "合并把 NULL 写回段后应交回 H2 行式路径");
    }

    @Test
    void orderedStreamingSegmentsContainingNullFallBackToH2() throws SQLException {
        execute(createTable("T5", "ID INT, TS BIGINT, V INT",
                "\"segmentSize=4\", \"compactionMinRows=100000\""));
        execute("INSERT INTO T5 VALUES (1, 10, 1), (2, NULL, 2), (3, 5, 3), (4, 8, 4)");
        execute("INSERT INTO T5 VALUES (5, 20, 5), (6, NULL, 6), (7, 15, 7), (8, 25, 8)");
        long claims = omapStore("T5").getOrderedStreamingClaims();
        // NULL 组跨段必须整体位于末尾（H2 默认 DESC 视 NULL 最小）；若误入流式，两段的 NULL 会被拆散
        assertEquals(List.of("8/25", "5/20", "7/15", "1/10", "4/8", "3/5", "2/null", "6/null"),
                rows("SELECT ID, TS FROM T5 ORDER BY TS DESC"));
        assertEquals(claims, omapStore("T5").getOrderedStreamingClaims(),
                "参与段首列实际含 NULL 应交回 H2 行式路径");
    }

    @Test
    void orderedStreamingNullableColumnWithoutNullsIsClaimed() throws SQLException {
        execute(createTable("T6", "ID INT, TS BIGINT, V INT",
                "\"segmentSize=4\", \"compactionMinRows=100000\""));
        execute("INSERT INTO T6 VALUES (1, 10, 1), (2, 5, 2), (3, 8, 3), (4, 2, 4)");
        execute("INSERT INTO T6 VALUES (5, 20, 5), (6, 11, 6), (7, 15, 7), (8, 13, 8)");
        execute("INSERT INTO T6 VALUES (9, 25, 9), (10, 30, 10)");
        long claims = omapStore("T6").getOrderedStreamingClaims();
        assertEquals(List.of(
                "10/30", "9/25", "5/20", "7/15", "8/13", "6/11", "1/10", "3/8", "2/5", "4/2"),
                rows("SELECT ID, TS FROM T6 ORDER BY TS DESC"));
        assertTrue(omapStore("T6").getOrderedStreamingClaims() > claims,
                "首列声明可空但全部参与段不含 NULL 应命中有序流式下推");
    }

    @Test
    void orderedStreamingNullNewImageSortsAcrossSegments() throws SQLException {
        execute(createTable("T7", "ID INT, TS BIGINT, V INT",
                "\"segmentSize=4\", \"compactionMinRows=100000\""));
        execute("INSERT INTO T7 VALUES (1, 10, 1), (2, 5, 2), (3, 8, 3), (4, 2, 4)");
        execute("INSERT INTO T7 VALUES (5, 20, 5), (6, 11, 6), (7, 15, 7), (8, 13, 8)");
        execute("INSERT INTO T7 VALUES (9, 25, 9), (10, 30, 10)");
        // delta 新像把冻结段内一行的 TS 改为 NULL：合并前不写回段，段 hasNull 仍为 false
        execute("UPDATE T7 SET TS = NULL WHERE ID = 6");
        // DESC 默认 NULL 视最小：NULL 新像整体排在末尾，不得拆散进段流
        assertEquals(List.of(
                "10/30", "9/25", "5/20", "7/15", "8/13", "1/10", "3/8", "2/5", "4/2", "6/null"),
                rows("SELECT ID, TS FROM T7 ORDER BY TS DESC"));
        // 显式 NULLS FIRST：NULL 新像整体排在最前
        assertEquals(List.of(
                "6/null", "10/30", "9/25", "5/20", "7/15", "8/13", "1/10", "3/8", "2/5", "4/2"),
                rows("SELECT ID, TS FROM T7 ORDER BY TS DESC NULLS FIRST"));
        assertTrue(omapStore("T7").getOrderedStreamingClaims() >= 2,
                "NULL 只出现在 delta 新像时两条查询都应命中流式下推");
    }

    // ------------------------------------------------------------------
    // M20-2：表达式投影在行发出时逐行求值
    // ------------------------------------------------------------------

    @Test
    void orderedStreamingExpressionProjectionEvaluatesPerRow() throws SQLException {
        createOrderedStreamingTable();
        assertEquals(List.of(
                "145/35", "91/30", "102/25", "136/22", "124/21",
                "59/20", "68/17", "76/15", "87/13",
                "11/10", "31/8", "22/5", "43/2"),
                rows("SELECT ID * 10 + V, TS FROM T ORDER BY TS DESC"));
        assertTrue(omapStore("T").getOrderedStreamingClaims() >= 1,
                "表达式投影应命中有序流式下推");
    }

    @Test
    void orderedStreamingExpressionWithDirectColumnsAndWhere() throws SQLException {
        createOrderedStreamingTable();
        // 表达式、直接列引用混排共享同一列；delta 新像（ID 6）与开放段行（13/14）走活像发射路径
        assertEquals(List.of(
                "14/28/6", "13/26/7", "12/24/5", "5/10/10", "6/12/9", "7/14/7", "8/16/8"),
                rows("SELECT ID, ID * 2, V + 1 FROM T WHERE V >= 4 ORDER BY TS DESC"));
        assertTrue(omapStore("T").getOrderedStreamingClaims() >= 1);
    }

    @Test
    void orderedStreamingFunctionProjectionOnVarcharColumn() throws SQLException {
        execute(createTable("T6", "ID INT, TS TIMESTAMP NOT NULL, V INT, NAME VARCHAR",
                "\"segmentSize=2\", \"compactionMinRows=100000\""));
        execute("INSERT INTO T6 VALUES (1, TIMESTAMP '2026-01-01 00:00:10', 1, 'a')");
        execute("INSERT INTO T6 VALUES (2, TIMESTAMP '2026-01-01 00:00:05', 2, 'b')");
        execute("INSERT INTO T6 VALUES (3, TIMESTAMP '2026-01-01 00:00:30', 3, 'c')");
        execute("INSERT INTO T6 VALUES (4, TIMESTAMP '2026-01-01 00:00:25', 4, 'd')");
        execute("INSERT INTO T6 VALUES (5, TIMESTAMP '2026-01-01 00:00:35', 5, 'e')");
        assertEquals(List.of("E/5", "C/3", "D/4", "A/1", "B/2"),
                rows("SELECT UPPER(NAME), ID FROM T6 ORDER BY TS DESC"));
        assertTrue(omapStore("T6").getOrderedStreamingClaims() >= 1);
    }

    @Test
    void orderedStreamingNonDeterministicExpressionFallsBackToH2() throws SQLException {
        createOrderedStreamingTable();
        long claims = omapStore("T").getOrderedStreamingClaims();
        assertEquals("14", rows("SELECT ID, RAND() FROM T ORDER BY TS DESC").get(0).split("/")[0]);
        assertEquals(claims, omapStore("T").getOrderedStreamingClaims(),
                "非确定表达式（RAND）应交回 H2 行式路径，不命中流式下推");
    }

    @Test
    void orderedStreamingSubqueryProjectionFallsBackToH2() throws SQLException {
        createOrderedStreamingTable();
        long claims = omapStore("T").getOrderedStreamingClaims();
        // MAX(V) = 9（ID 5）；顶层子查询交回 H2，但结果仍正确
        assertEquals(List.of(
                "14/9", "9/9", "10/9", "13/9", "12/9", "5/9", "6/9", "7/9", "8/9",
                "1/9", "3/9", "2/9", "4/9"),
                rows("SELECT ID, (SELECT MAX(V) FROM T) FROM T ORDER BY TS DESC"));
        assertEquals(claims, omapStore("T").getOrderedStreamingClaims(),
                "顶层子查询投影应交回 H2 行式路径");
    }

    @Test
    void orderedStreamingExpressionOnEvictedSegmentsReadsColumnsDeferred(@TempDir Path dir)
            throws Exception {
        String url = "jdbc:h2:" + dir.resolve("db").toAbsolutePath().toString().replace('\\', '/');
        try (Connection c = DriverManager.getConnection(url, "sa", "")) {
            try (Statement s = c.createStatement()) {
                s.execute(createTable("T", "ID INT, TS BIGINT NOT NULL",
                        "\"segmentSize=4\", \"residentBytes=1\""));
                for (int i = 1; i <= 40; i++) {
                    s.execute("INSERT INTO T VALUES (" + i + ", " + (i * 10) + ")");
                }
            }
            org.h2.engine.SessionLocal session = (org.h2.engine.SessionLocal)
                    ((org.h2.jdbc.JdbcConnection) c).getSession();
            com.hyd.h2omap.table.OmapTable table = (com.hyd.h2omap.table.OmapTable)
                    session.getDatabase().getMainSchema().findTableOrView(session, "T");
            com.hyd.h2omap.storage.ColumnarStore store = table.getStore();
            long deferredBefore = store.getDeferredProjectionReads();
            long claimsBefore = store.getOrderedStreamingClaims();
            List<String> expected = new ArrayList<>();
            for (int i = 40; i >= 1; i--) {
                expected.add(String.valueOf(i + 1000));
            }
            List<String> got = new ArrayList<>();
            try (Statement st = c.createStatement();
                    ResultSet rs = st.executeQuery("SELECT ID + 1000 FROM T ORDER BY TS DESC")) {
                while (rs.next()) {
                    got.add(rs.getString(1));
                }
            }
            assertEquals(expected, got);
            assertEquals(claimsBefore + 1, store.getOrderedStreamingClaims());
            assertTrue(store.getDeferredProjectionReads() > deferredBefore,
                    "表达式引用列应在段内首行发出时才经切片读回");
        }
    }

    @Test
    void orderedStreamingNotClaimedWhenLimitPresent() throws SQLException {
        createOrderedStreamingTable();
        long claims = omapStore("T").getOrderedStreamingClaims();
        assertEquals(List.of("14/35", "9/30", "10/25"),
                rows("SELECT ID, TS FROM T ORDER BY TS DESC LIMIT 3"));
        assertEquals(claims, omapStore("T").getOrderedStreamingClaims(),
                "带 LIMIT 的有序查询走 M11 有序模式，不走流式下推");
    }

    private com.hyd.h2omap.storage.ColumnarStore omapStore(String table) {
        org.h2.engine.SessionLocal session = (org.h2.engine.SessionLocal)
                ((org.h2.jdbc.JdbcConnection) conn).getSession();
        com.hyd.h2omap.table.OmapTable t = (com.hyd.h2omap.table.OmapTable)
                session.getDatabase().getMainSchema().findTableOrView(session, table);
        return t.getStore();
    }

    private static String createTableSqlFromScript(Statement s, String table) throws SQLException {
        try (ResultSet rs = s.executeQuery("SCRIPT")) {
            while (rs.next()) {
                String row = rs.getString(1);
                if (row != null && row.startsWith("CREATE")
                        && (row.contains("\"" + table + "\"(") || row.contains("TABLE " + table + "("))) {
                    return row;
                }
            }
        }
        return "";
    }

    private static String toString(Statement s, String sql) throws SQLException {
        try (ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }
}

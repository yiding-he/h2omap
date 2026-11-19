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
            // 连接断开后库已关闭，这次打开走惰性加载：先只登记 manifest，段在后台逐段读入。
            try (Connection second = DriverManager.getConnection(url, "sa", "")) {
                try (Statement s = second.createStatement()) {
                    assertEquals("true", toString(s,
                            "SELECT SETTING_VALUE FROM INFORMATION_SCHEMA.SETTINGS"
                                    + " WHERE SETTING_NAME = 'OMAP_LAZY_LOAD'"));
                    // 加载未完成时客户端就能写入：新行进开放段，冻结时触发 checkpoint，
                    // 未加载的历史段不会被重写。
                    s.execute("INSERT INTO T VALUES (1000, 'new')");
                    for (int i = 1001; i <= 1059; i++) {
                        s.execute("INSERT INTO T VALUES (" + i + ", 'new')");
                    }
                    // 加载完成前行数只包含已加载的段加新写入的行，完成后应等于真实行数。
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

            // 只触碰段 0（ID 0..49）：读回后段 0 驻留，且按访问时间保护，
            // 未被访问的驻留段让位；这证明逐出按访问时间而非段号新老。
            assertEquals("1225", toString(s, "SELECT SUM(ID) FROM T WHERE ID < 50"));
            assertTrue(store.isSegmentResident(0), "刚被访问的段应已读回");
            assertFalse(store.isSegmentResident(7), "未被访问的驻留段应让位");

            // 再访问段 5（ID 250..299）：上一轮刚访问的段 0 比段 5 更久未访问，先让位。
            assertEquals("13725", toString(s, "SELECT SUM(ID) FROM T WHERE ID >= 250 AND ID < 300"));
            assertTrue(store.isSegmentResident(5));
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

    private static String toString(Statement s, String sql) throws SQLException {
        try (ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }
}

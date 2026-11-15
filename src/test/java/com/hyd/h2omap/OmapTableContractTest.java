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

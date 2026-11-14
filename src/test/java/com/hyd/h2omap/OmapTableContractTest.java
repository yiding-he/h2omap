/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
                assertEquals("0", toString(s, "SELECT COUNT(*) FROM T"));
                s.execute("INSERT INTO T VALUES (5)");
                assertEquals("5", toString(s, "SELECT ID FROM T"));
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
                assertEquals("0", toString(s, "SELECT COUNT(*) FROM T"));
                s.execute("INSERT INTO T VALUES (7, 'g'), (8, 'h')");
                assertEquals("8/h", toString(s, "SELECT ID || '/' || NAME FROM T WHERE ID = 8"));
                try (ResultSet rs = s.executeQuery("SELECT ID FROM T ORDER BY ID")) {
                    assertTrue(rs.next());
                    assertEquals("7", rs.getString(1));
                }
            }
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

    private static String toString(Statement s, String sql) throws SQLException {
        try (ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }
}

/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.bench;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 生成器产出的数据在中等规模下的正确性。
 *
 * <p>用 {@code segmentSize=1000} 让 1 万行跨 10 个段，用
 * {@code compactionMinRows=1000} 让更新触发合并，从而覆盖段边界与合并路径。
 */
class DataScaleTest {

    private static final int ROWS = 10_000;

    private static final int SEGMENT_SIZE = 1000;

    private static final int COMPACTION_MIN_ROWS = 1000;

    private static final String OMAP_TABLE = "T";

    private static final String MV_TABLE = "T_MV";

    private static final AtomicInteger SEQ = new AtomicInteger();

    private Connection conn;

    @BeforeEach
    void setUp() throws SQLException {
        conn = DriverManager.getConnection(
                "jdbc:h2:mem:omapscale" + SEQ.incrementAndGet() + ";DB_CLOSE_DELAY=-1", "sa", "");
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

    @Test
    void generatedDataIsConsistentAcrossEngines() throws SQLException {
        DataGenerator.generate(conn, ROWS, true, SEGMENT_SIZE, COMPACTION_MIN_ROWS);

        assertEquals(ROWS, DataGenerator.count(conn, OMAP_TABLE));
        assertEquals(ROWS, DataGenerator.count(conn, MV_TABLE));

        long nulls = scalarLong("SELECT COUNT(*) FROM " + OMAP_TABLE + " WHERE NULLABLE_COL IS NULL");
        assertTrue(nulls >= ROWS * 4 / 100 && nulls <= ROWS * 6 / 100,
                "null ratio out of range: " + nulls);

        assertEquals(scalarLong("SELECT COUNT(*) FROM " + OMAP_TABLE),
                scalarLong("SELECT COUNT(DISTINCT ID) FROM " + OMAP_TABLE),
                "primary key not unique");

        assertArrayEquals(DataGenerator.signature(conn, OMAP_TABLE), DataGenerator.signature(conn, MV_TABLE));

        DataGenerator.verifyQueries(conn, OMAP_TABLE, MV_TABLE);
    }

    @Test
    void updatesAndDeletesStayConsistentAcrossEngines() throws SQLException {
        DataGenerator.generate(conn, ROWS, true, SEGMENT_SIZE, COMPACTION_MIN_ROWS);

        long before = scalarLong("SELECT INT_COL FROM " + OMAP_TABLE + " WHERE ID = 1000");
        execute("UPDATE " + OMAP_TABLE + " SET INT_COL = INT_COL + 1 WHERE ID <= 1000");
        execute("UPDATE " + MV_TABLE + " SET INT_COL = INT_COL + 1 WHERE ID <= 1000");
        execute("DELETE FROM " + OMAP_TABLE + " WHERE ID > 9900");
        execute("DELETE FROM " + MV_TABLE + " WHERE ID > 9900");

        assertEquals(9900, DataGenerator.count(conn, OMAP_TABLE));
        assertEquals(9900, DataGenerator.count(conn, MV_TABLE));
        assertArrayEquals(DataGenerator.signature(conn, OMAP_TABLE), DataGenerator.signature(conn, MV_TABLE));

        // 合并把 delta 写回段之后，被改过的行仍可读且是新值。
        assertEquals(before + 1, scalarLong("SELECT INT_COL FROM " + OMAP_TABLE + " WHERE ID = 1000"));
    }

    @Test
    void scalarSubqueryMatchesMvStoreAndReusesResult() throws SQLException {
        DataGenerator.generate(conn, ROWS, true, SEGMENT_SIZE, COMPACTION_MIN_ROWS);

        long t0 = System.nanoTime();
        long omapCount = scalarLong(
                "SELECT COUNT(*) FROM " + OMAP_TABLE + " WHERE INT_COL > (SELECT AVG(INT_COL) FROM " + OMAP_TABLE + ")");
        long omapMs = (System.nanoTime() - t0) / 1_000_000L;

        long t1 = System.nanoTime();
        long mvCount = scalarLong(
                "SELECT COUNT(*) FROM " + MV_TABLE + " WHERE INT_COL > (SELECT AVG(INT_COL) FROM " + MV_TABLE + ")");
        long mvMs = (System.nanoTime() - t1) / 1_000_000L;

        System.out.printf("10k scalar subquery: OMAP=%d ms (count=%d), MVStore=%d ms (count=%d)%n",
                omapMs, omapCount, mvMs, mvCount);
        assertEquals(mvCount, omapCount);
        assertTrue(omapMs < 2000, "scalar subquery did not reuse result, took " + omapMs + " ms");
    }

    private long scalarLong(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private void execute(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }
}

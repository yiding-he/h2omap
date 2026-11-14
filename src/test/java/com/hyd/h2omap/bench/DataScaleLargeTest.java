/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.bench;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 100 万行规模下的正确性与引擎对比。
 *
 * <p>默认不运行。触发方式：
 * <pre>mvn -B test -Domap.scale=large -DargLine=-Xmx2g</pre>
 *
 * <p>100 万行在默认 {@code segmentSize=65536} 下约 16 个段，
 * {@code compactionMinRows=16384} 由后面的 UPDATE 触发合并。
 */
@EnabledIfSystemProperty(named = "omap.scale", matches = "large")
class DataScaleLargeTest {

    private static final int ROWS = 1_000_000;

    private static final String OMAP_TABLE = "T";

    private static final String MV_TABLE = "T_MV";

    @Test
    void largeScaleDataIsConsistent() throws Exception {
        try (Connection conn = DriverManager.getConnection(
                "jdbc:h2:mem:omaplarge;DB_CLOSE_DELAY=-1", "sa", "")) {
            DataGenerator.generate(conn, ROWS, true);

            assertEquals(ROWS, DataGenerator.count(conn, OMAP_TABLE));
            assertEquals(ROWS, DataGenerator.count(conn, MV_TABLE));
            assertArrayEquals(DataGenerator.signature(conn, OMAP_TABLE),
                    DataGenerator.signature(conn, MV_TABLE));

            // 超过默认 compactionMinRows，触发就地合并。
            long before = scalarLong(conn, "SELECT INT_COL FROM " + OMAP_TABLE + " WHERE ID = 500000");
            execute(conn, "UPDATE " + OMAP_TABLE + " SET INT_COL = INT_COL + 1 WHERE ID <= 20000");
            execute(conn, "UPDATE " + MV_TABLE + " SET INT_COL = INT_COL + 1 WHERE ID <= 20000");

            assertArrayEquals(DataGenerator.signature(conn, OMAP_TABLE),
                    DataGenerator.signature(conn, MV_TABLE));
            assertEquals(before, scalarLong(conn, "SELECT INT_COL FROM " + OMAP_TABLE + " WHERE ID = 500000"));

            DataGenerator.bench(conn, OMAP_TABLE);
        }
    }

    private static long scalarLong(Connection conn, String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private static void execute(Connection conn, String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }
}

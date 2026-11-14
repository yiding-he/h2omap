/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.bench;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * 大规模测试数据生成器。
 *
 * <p>固定随机种子，同一行数重复执行产出相同的数据。同时灌 OMAP 表与
 * MVStore 对比表，两张表的数据完全一致，供 M5 的量化对比使用。
 *
 * <p>表结构固定 8 列，覆盖列类型与分布：
 * <ul>
 *   <li>{@code ID} 主键，连续</li>
 *   <li>{@code LOW_CARD} 低基数 VARCHAR，10 个取值，用于字典编码收益测量</li>
 *   <li>{@code HIGH_CARD} 高基数 VARCHAR</li>
 *   <li>{@code INT_COL} 部分连续、部分随机，用于 zone map 剪枝</li>
 *   <li>{@code DOUBLE_COL} 随机浮点</li>
 *   <li>{@code TS_COL} 随机时间戳，走 {@code ValueVector} 回退</li>
 *   <li>{@code NULLABLE_COL} 约 5% 为 NULL</li>
 *   <li>{@code ARRAY_COL} 随机长度整型数组，当前整值存放</li>
 *   <li>{@code STR_ARRAY_COL} 随机长度字符串数组，覆盖变长元素</li>
 * </ul>
 *
 * <p>命令行：{@code java com.hyd.h2omap.bench.DataGenerator [rows] [url] [omap|mv]}。
 * 默认 100 万行、内存库 {@code jdbc:h2:mem:omapbench;DB_CLOSE_DELAY=-1}、OMAP 引擎。
 * 两种引擎分开跑，每次只建一张表。
 */
public final class DataGenerator {

    public static final String ENGINE = "com.hyd.h2omap.engine.OmapTableEngine";

    /** 单引擎模式：只建 OMAP 表。*/
    public static final String ENGINE_OMAP = "omap";

    /** 单引擎模式：只建 MVStore 基准表。*/
    public static final String ENGINE_MV = "mv";

    public static final int DEFAULT_SEGMENT_SIZE = 65536;

    public static final int DEFAULT_COMPACTION_MIN_ROWS = 16384;

    private static final long SEED = 20260923L;

    /**
     * 基准查询集：条件复杂多变的组合查询，不包含主键列 ID，
     * 使行式基线无法通过索引优化，只能全表扫描。
     *
     * <p>第 10、11 条是列裁剪对照（读 1 列对读 9 列），
     * 最后一条的子查询需要两个表名参数。
     */
    public static final String[] QUERIES = {
            "SELECT COUNT(*) FROM %s WHERE LOW_CARD = 'C3' AND INT_COL > 500000",
            "SELECT COUNT(*) FROM %s WHERE LOW_CARD = 'C3' AND INT_COL > 500000 "
                    + "AND DOUBLE_COL < 900",
            "SELECT COUNT(*) FROM %s WHERE LOW_CARD IN ('C1','C2','C3','C4','C5')",
            "SELECT COUNT(*) FROM %s WHERE NULLABLE_COL = 123456",
            "SELECT COUNT(*) FROM %s WHERE INT_COL * 2 > 1000000",
            "SELECT COUNT(*) FROM %s WHERE TS_COL >= TIMESTAMP '2026-06-01' "
                    + "AND TS_COL < TIMESTAMP '2026-07-01'",
            "SELECT COUNT(*) FROM %s WHERE HIGH_CARD LIKE 'user-9%'",
            "SELECT LOW_CARD, COUNT(*) FROM %s GROUP BY LOW_CARD HAVING COUNT(*) > 50000",
            "SELECT HIGH_CARD, INT_COL FROM %s ORDER BY INT_COL DESC, ID LIMIT 100",
            "SELECT SUM(INT_COL) FROM %s WHERE INT_COL > 500000",
            "SELECT * FROM %s WHERE INT_COL > 500000",
            "SELECT COUNT(*) FROM %s WHERE INT_COL > (SELECT AVG(INT_COL) FROM %s)",
            "SELECT COUNT(*) FROM %s WHERE LOW_CARD <> 'C9' AND INT_COL >= 10000 "
                    + "AND DOUBLE_COL < 950 AND TS_COL >= TIMESTAMP '2026-02-01' "
                    + "AND NULLABLE_COL IS NOT NULL AND CARDINALITY(ARRAY_COL) >= 1 "
                    + "AND CARDINALITY(STR_ARRAY_COL) >= 1",
    };

    private static final int BATCH = 2000;

    private static final String OMAP_TABLE = "T";

    private static final String MV_TABLE = "T_MV";

    private static final LocalDateTime BASE = LocalDateTime.of(2026, 1, 1, 0, 0);

    private static final String DDL = """
            CREATE TABLE %s(
              ID BIGINT PRIMARY KEY,
              LOW_CARD VARCHAR,
              HIGH_CARD VARCHAR,
              INT_COL INT,
              DOUBLE_COL DOUBLE,
              TS_COL TIMESTAMP,
              NULLABLE_COL INT,
              ARRAY_COL INT ARRAY,
              STR_ARRAY_COL VARCHAR ARRAY
            )""";

    private DataGenerator() {
    }

    public static void main(String[] args) throws Exception {
        int rows = args.length > 0 ? Integer.parseInt(args[0]) : 1_000_000;
        String url = args.length > 1 ? args[1] : "jdbc:h2:mem:omapbench;DB_CLOSE_DELAY=-1";
        String engine = args.length > 2 ? args[2] : ENGINE_OMAP;
        try (Connection conn = DriverManager.getConnection(url, "sa", "")) {
            benchSetup(conn, rows, engine);
        }
    }

    /**
     * 建表并灌入数据。
     *
     * @param conn          连接
     * @param rows          行数
     * @param mvCompare     是否同时建 MVStore 对比表
     * @param segmentSize   段大小
     * @param compactionMinRows 合并阈值
     */
    public static void generate(Connection conn, int rows, boolean mvCompare,
            int segmentSize, int compactionMinRows) throws SQLException {
        boolean autoCommit = conn.getAutoCommit();
        conn.setAutoCommit(false);
        try {
            createTables(conn, mvCompare, segmentSize, compactionMinRows);
            insert(conn, OMAP_TABLE, rows);
            if (mvCompare) {
                insert(conn, MV_TABLE, rows);
            }
            conn.commit();
        } finally {
            conn.setAutoCommit(autoCommit);
        }
    }

    public static void generate(Connection conn, int rows, boolean mvCompare) throws SQLException {
        generate(conn, rows, mvCompare, DEFAULT_SEGMENT_SIZE, DEFAULT_COMPACTION_MIN_ROWS);
    }

    /**
     * 单引擎：预热后清空重插，再跑查询。
     *
     * <p>预热轮让 JIT 与页缓存进入稳态；清空重插让表从空表开始，
     * 插入耗时与查询都在同一状态下测量。两种引擎分开跑，每次只建一张表；
     * MVStore 是基准，只在测试条件变化时才需要重跑。
     */
    public static void benchSetup(Connection conn, int rows, String engine) throws SQLException {
        benchSetup(conn, rows, engine, DEFAULT_SEGMENT_SIZE, DEFAULT_COMPACTION_MIN_ROWS);
    }

    public static void benchSetup(Connection conn, int rows, String engine,
            int segmentSize, int compactionMinRows) throws SQLException {
        createOneTable(conn, engine, segmentSize, compactionMinRows);
        insert(conn, OMAP_TABLE, rows);
        printMemory("after_warmup");
        truncate(conn, OMAP_TABLE);
        printMemory("after_truncate");
        boolean autoCommit = conn.getAutoCommit();
        conn.setAutoCommit(false);
        try {
            System.out.println("engine=" + engine + " rows=" + rows
                    + " insert_ms=" + insert(conn, OMAP_TABLE, rows));
            conn.commit();
        } finally {
            conn.setAutoCommit(autoCommit);
        }
        printMemory("after_insert");
        assertFullScan(conn, OMAP_TABLE);
        bench(conn, OMAP_TABLE);
        printMemory("after_bench");
    }

    /** 建一张表：OMAP 引擎或 MVStore 默认引擎。*/
    private static void createOneTable(Connection conn, String engine, int segmentSize,
            int compactionMinRows) throws SQLException {
        String ddl = String.format(DDL, OMAP_TABLE);
        if (ENGINE_OMAP.equals(engine)) {
            ddl += " ENGINE \"" + ENGINE + "\" WITH "
                    + "\"segmentSize=" + segmentSize + "\", \"compactionMinRows=" + compactionMinRows + "\"";
        } else if (!ENGINE_MV.equals(engine)) {
            throw new IllegalArgumentException("engine must be " + ENGINE_OMAP + " or " + ENGINE_MV);
        }
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS " + OMAP_TABLE);
            s.execute(ddl);
        }
    }

    /** 打印 JVM 堆的当前使用量与上限。*/
    public static void printMemory(String label) {
        Runtime rt = Runtime.getRuntime();
        long usedMb = (rt.totalMemory() - rt.freeMemory()) / (1024 * 1024);
        long maxMb = rt.maxMemory() / (1024 * 1024);
        System.out.println("mem " + label + " used_mb=" + usedMb + " max_mb=" + maxMb);
    }

    private static void truncate(Connection conn, String table) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("TRUNCATE TABLE " + table);
        }
    }

    private static void createTables(Connection conn, boolean mvCompare, int segmentSize,
            int compactionMinRows) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS " + OMAP_TABLE);
            s.execute(String.format(DDL, OMAP_TABLE) + " ENGINE \"" + ENGINE + "\" WITH "
                    + "\"segmentSize=" + segmentSize + "\", \"compactionMinRows=" + compactionMinRows + "\"");
            if (mvCompare) {
                s.execute("DROP TABLE IF EXISTS " + MV_TABLE);
                s.execute(String.format(DDL, MV_TABLE));
            }
        }
    }

    private static long insert(Connection conn, String table, int rows) throws SQLException {
        String sql = "INSERT INTO " + table + " VALUES (?,?,?,?,?,?,?,?,?)";
        Random random = new Random(SEED);
        long t0 = System.nanoTime();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 1; i <= rows; i++) {
                bind(ps, i, random);
                ps.addBatch();
                if (i % BATCH == 0) {
                    ps.executeBatch();
                }
            }
            ps.executeBatch();
        }
        return (System.nanoTime() - t0) / 1_000_000L;
    }

    private static void bind(PreparedStatement ps, int i, Random random) throws SQLException {
        ps.setLong(1, i);
        ps.setString(2, "C" + random.nextInt(10));
        ps.setString(3, "user-" + i);
        ps.setInt(4, random.nextInt(3) == 0 ? i : random.nextInt(1_000_000));
        ps.setDouble(5, random.nextDouble() * 1000);
        ps.setTimestamp(6, Timestamp.valueOf(BASE.plusSeconds(random.nextInt(365 * 24 * 3600))));
        if (random.nextInt(20) == 0) {
            ps.setNull(7, Types.INTEGER);
        } else {
            ps.setInt(7, random.nextInt(1_000_000));
        }
        ps.setObject(8, randomArray(random));
        ps.setObject(9, randomStringArray(random));
    }

    private static String[] randomStringArray(Random random) {
        int len = random.nextInt(5);
        String[] values = new String[len];
        for (int i = 0; i < len; i++) {
            values[i] = "s" + random.nextInt(50);
        }
        return values;
    }

    /** H2 的 {@code setObject} 只把 {@code Object[]} 转成数组，{@code int[]} 会被当成 Java 对象序列化。 */
    private static Integer[] randomArray(Random random) {
        int len = random.nextInt(10);
        Integer[] values = new Integer[len];
        for (int i = 0; i < len; i++) {
            values[i] = random.nextInt(100);
        }
        return values;
    }

    public static long count(Connection conn, String table) throws SQLException {
        try (Statement s = conn.createStatement();
                ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM " + table)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    /**
     * 两表内容的整数聚合签名，必须逐项相同。
     *
     * <p>只用整数列，避免浮点求和顺序与数组、时间戳的表示差异。
     */
    public static long[] signature(Connection conn, String table) throws SQLException {
        String sql = "SELECT COUNT(*), COALESCE(SUM(ID),0), COALESCE(SUM(INT_COL),0), "
                + "COALESCE(SUM(LENGTH(LOW_CARD)),0), COALESCE(SUM(LENGTH(HIGH_CARD)),0), "
                + "COALESCE(SUM(CARDINALITY(ARRAY_COL)),0), "
                + "COALESCE(SUM(CARDINALITY(STR_ARRAY_COL)),0), "
                + "COALESCE(SUM(CASE WHEN NULLABLE_COL IS NULL THEN 1 ELSE 0 END),0) "
                + "FROM " + table;
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            rs.next();
            long[] values = new long[8];
            for (int i = 0; i < values.length; i++) {
                values[i] = rs.getLong(i + 1);
            }
            return values;
        }
    }

    /** 在指定表上跑一批查询，打印耗时。 */
    public static void bench(Connection conn, String table) throws SQLException {
        for (String q : QUERIES) {
            String sql = q.replace("%s", table);
            StringBuilder info = new StringBuilder();
            long ms = query(conn, sql, info);
            System.out.println("bench " + table + " " + ms + " ms" + info + " : " + sql);
        }
    }

    /**
     * 断言查询集全部走全表扫描，不走索引。
     *
     * <p>对比前提是行式基线无法通过索引优化；一旦计划出现 PRIMARY_KEY，
     * 说明查询命中了索引列，对比失效。
     */
    public static void assertFullScan(Connection conn, String table) throws SQLException {
        for (String q : QUERIES) {
            String sql = "EXPLAIN " + q.replace("%s", table);
            String plan = plan(conn, sql);
            if (plan.contains("PRIMARY_KEY")) {
                throw new IllegalStateException("query uses an index, comparison is invalid: "
                        + sql + "\n" + plan);
            }
        }
    }

    /**
     * 比对两表在同一查询集上的结果，只比对聚合类查询。
     *
     * <p>{@code SELECT *} 结果集大且无确定顺序，只计时不比对。
     */
    public static void verifyQueries(Connection conn, String omapTable, String mvTable)
            throws SQLException {
        for (String q : QUERIES) {
            if (q.startsWith("SELECT *")) {
                continue;
            }
            String a = q.replace("%s", omapTable);
            String b = q.replace("%s", mvTable);
            String ra = result(conn, a);
            String rb = result(conn, b);
            if (!ra.equals(rb)) {
                throw new IllegalStateException("result mismatch\nomap: " + a + "\n" + ra
                        + "\nmv:   " + b + "\n" + rb);
            }
        }
    }

    private static String plan(Connection conn, String sql) throws SQLException {
        StringBuilder sb = new StringBuilder();
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            while (rs.next()) {
                sb.append(rs.getString(1)).append('\n');
            }
        }
        return sb.toString();
    }

    private static String result(Connection conn, String sql) throws SQLException {
        StringBuilder sb = new StringBuilder();
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            int cols = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                for (int i = 1; i <= cols; i++) {
                    sb.append(rs.getString(i)).append('|');
                }
                sb.append('\n');
            }
        }
        return sb.toString();
    }

    /** 在实际存在的所有表上跑查询。 */
    public static void bench(Connection conn) throws SQLException {
        for (String table : existingTables(conn)) {
            bench(conn, table);
        }
    }

    private static List<String> existingTables(Connection conn) throws SQLException {
        List<String> result = new ArrayList<>();
        for (String name : new String[] { OMAP_TABLE, MV_TABLE }) {
            if (exists(conn, name)) {
                result.add(name);
            }
        }
        return result;
    }

    private static boolean exists(Connection conn, String table) throws SQLException {
        String sql = "SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '" + table + "'";
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next();
        }
    }

    private static long query(Connection conn, String sql, StringBuilder info) throws SQLException {
        boolean singleScalar = sql.startsWith("SELECT COUNT(*)");
        long t0 = System.nanoTime();
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            if (singleScalar && rs.next()) {
                info.append(" (count=").append(rs.getLong(1)).append(')');
            } else {
                while (rs.next()) {
                    // 只计时，不取值。
                }
            }
        }
        return (System.nanoTime() - t0) / 1_000_000L;
    }
}

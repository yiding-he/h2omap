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
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.locks.LockSupport;

/**
 * 多线程并发查询基准：100 万行 OMAP 表，每条查询均命中超过 50% 的记录。
 *
 * <p>命令行：{@code java com.hyd.h2omap.bench.ConcurrentQueryBench [threads] [rows] [rounds] [withWriter] [engine] [url]}。
 */
public final class ConcurrentQueryBench {

    private static final String[] FIXED_QUERIES = {
            "SELECT COUNT(*) FROM T WHERE INT_COL * 2 >= 900000",
            "SELECT COUNT(*) FROM T WHERE LOW_CARD IN ('C0','C1','C2','C3','C4','C5')",
            "SELECT SUM(INT_COL) FROM T WHERE INT_COL > (SELECT AVG(INT_COL) FROM T T2 "
                    + "WHERE T2.LOW_CARD = T.LOW_CARD)",
            "SELECT COUNT(*) FROM T WHERE LOW_CARD <> 'C9' AND INT_COL >= 10000 "
                    + "AND DOUBLE_COL < 950 AND TS_COL >= TIMESTAMP '2026-02-01' "
                    + "AND NULLABLE_COL IS NOT NULL AND CARDINALITY(ARRAY_COL) >= 1 "
                    + "AND CARDINALITY(STR_ARRAY_COL) >= 1",
    };

    /**
     * 有序扫描查询：{@code SORT_TS} 与插入顺序同向，窗口只与末尾少量段相交。
     *
     * <p>窗口按行数算出（末尾 3 天、每天 86400 行），行数变化时结果仍非空。
     */
    private static String orderedQuery(int rows) {
        int windowRows = Math.min(rows, 3 * 86400);
        java.time.LocalDateTime start = DataGenerator.BASE.plusSeconds(Math.max(0, rows - windowRows));
        String since = start.toString().replace('T', ' ');
        return "SELECT ID, INT_COL, SORT_TS FROM T WHERE SORT_TS >= TIMESTAMP '" + since
                + "' AND LOW_CARD = 'C3' ORDER BY SORT_TS DESC LIMIT 100";
    }

    private static String[] queries;

    private ConcurrentQueryBench() {
    }

    public static void main(String[] args) throws Exception {
        int threads = Math.max(1, args.length > 0 ? Integer.parseInt(args[0]) : 1);
        int rows = args.length > 1 ? Integer.parseInt(args[1]) : 1_000_000;
        int rounds = Math.max(1, args.length > 2 ? Integer.parseInt(args[2]) : 5);
        boolean withWriter = args.length > 3 && Boolean.parseBoolean(args[3]);
        String engine = args.length > 4 ? args[4] : DataGenerator.ENGINE_OMAP;
        String url = args.length > 5 ? args[5]
                : "jdbc:h2:mem:omapconcurrent;DB_CLOSE_DELAY=-1;QUERY_CACHE_SIZE=0";
        int writerRatePerSec = args.length > 6 ? Integer.parseInt(args[6]) : 0;

        int compactionMinRows = withWriter ? 1 : DataGenerator.DEFAULT_COMPACTION_MIN_ROWS;
        queries = Arrays.copyOf(FIXED_QUERIES, FIXED_QUERIES.length + 1);
        queries[FIXED_QUERIES.length] = orderedQuery(rows);
        try (Connection setupConn = DriverManager.getConnection(url, "sa", "")) {
            DataGenerator.generateSingle(setupConn, rows, engine,
                    DataGenerator.DEFAULT_SEGMENT_SIZE, compactionMinRows);
            warmupAndVerify(setupConn, rows);
        }

        AtomicLongArray totalNanos = new AtomicLongArray(queries.length);
        List<Throwable> errors = new ArrayList<>();
        int totalWorkers = threads + (withWriter ? 1 : 0);
        CountDownLatch ready = new CountDownLatch(totalWorkers);
        CountDownLatch start = new CountDownLatch(1);
        AtomicBoolean stopWriter = new AtomicBoolean(false);
        AtomicLong writerRounds = new AtomicLong(0);
        List<Thread> readers = new ArrayList<>(threads);

        for (int t = 0; t < threads; t++) {
            Thread worker = new Thread(() -> {
                try (Connection c = DriverManager.getConnection(url, "sa", "");
                        Statement s = c.createStatement()) {
                    ready.countDown();
                    start.await();
                    for (int r = 0; r < rounds; r++) {
                        for (int q = 0; q < queries.length; q++) {
                            long t0 = System.nanoTime();
                            try (ResultSet rs = s.executeQuery(queries[q])) {
                                if (!rs.next()) {
                                    throw new IllegalStateException("empty result");
                                }
                            }
                            totalNanos.addAndGet(q, System.nanoTime() - t0);
                        }
                    }
                } catch (Throwable ex) {
                    synchronized (errors) {
                        errors.add(ex);
                    }
                }
            }, "bench-reader-" + t);
            readers.add(worker);
            worker.start();
        }

        Thread writer = null;
        if (withWriter) {
            writer = new Thread(() -> {
                String insertSql = "INSERT INTO T VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                String updateSql = "UPDATE T SET INT_COL = ? WHERE ID = ?";
                String deleteSql = "DELETE FROM T WHERE ID = ?";
                try (Connection c = DriverManager.getConnection(url, "sa", "");
                        PreparedStatement ins = c.prepareStatement(insertSql);
                        PreparedStatement upd = c.prepareStatement(updateSql);
                        PreparedStatement del = c.prepareStatement(deleteSql)) {
                    Timestamp ts = Timestamp.valueOf(LocalDateTime.of(2026, 6, 15, 0, 0));
                    Object[] intArr = new Integer[] { 1, 2 };
                    Object[] strArr = new String[] { "a", "b" };
                    ready.countDown();
                    start.await();

                    long step = 0;
                    long half = Math.max(1, rows / 2);
                    long intervalNanos = writerRatePerSec > 0 ? 1_000_000_000L / writerRatePerSec : 0L;
                    long nextDeadline = intervalNanos > 0 ? System.nanoTime() + intervalNanos : 0L;
                    while (!stopWriter.get()) {
                        step++;
                        long newId = rows + step;
                        ins.setLong(1, newId);
                        ins.setString(2, "C" + (step % 10));
                        ins.setString(3, "user-" + newId);
                        ins.setInt(4, (int) (500_000 + (step % 500_000)));
                        ins.setDouble(5, 100.0);
                        ins.setTimestamp(6, ts);
                        ins.setInt(7, 42);
                        ins.setObject(8, intArr);
                        ins.setObject(9, strArr);
                        ins.setTimestamp(10, Timestamp.valueOf(DataGenerator.BASE.plusSeconds(newId)));
                        ins.executeUpdate();

                        long updId = ((step - 1) % half) + 1;
                        upd.setInt(1, (int) (500_000 + (step % 500_000)));
                        upd.setLong(2, updId);
                        upd.executeUpdate();

                        long delId = half + ((step - 1) % half) + 1;
                        del.setLong(1, delId);
                        del.executeUpdate();

                        writerRounds.incrementAndGet();
                        if (intervalNanos > 0) {
                            long waitNanos = nextDeadline - System.nanoTime();
                            if (waitNanos > 0) {
                                LockSupport.parkNanos(waitNanos);
                            }
                            nextDeadline += intervalNanos;
                        }
                    }
                } catch (Throwable ex) {
                    synchronized (errors) {
                        errors.add(ex);
                    }
                }
            }, "bench-writer");
            writer.start();
        }

        ready.await();
        long wallStart = System.nanoTime();
        start.countDown();
        for (Thread reader : readers) {
            reader.join();
        }
        stopWriter.set(true);
        if (writer != null) {
            writer.join();
        }
        long wallMs = (System.nanoTime() - wallStart) / 1_000_000;

        if (!errors.isEmpty()) {
            throw new RuntimeException("concurrent bench failed", errors.get(0));
        }

        long totalExecutions = (long) threads * rounds;
        long sumAllNanos = 0;
        for (int q = 0; q < queries.length; q++) {
            long nanos = totalNanos.get(q);
            sumAllNanos += nanos;
            double avgMs = nanos / 1_000_000.0 / totalExecutions;
            System.out.printf("engine=%s threads=%d writer=%s q%d avg_ms=%.2f : %s%n",
                    engine, threads, withWriter, q + 1, avgMs, queries[q]);
        }
        long totalQueryCount = totalExecutions * queries.length;
        double overallAvgMs = sumAllNanos / 1_000_000.0 / totalQueryCount;
        double qps = totalQueryCount * 1000.0 / Math.max(1L, wallMs);
        System.out.printf("SUMMARY engine=%s threads=%d writer=%s writer_rounds=%d rows=%d rounds=%d "
                + "total_queries=%d wall_ms=%d overall_avg_ms=%.2f qps=%.1f%n",
                engine, threads, withWriter, writerRounds.get(), rows, rounds,
                totalQueryCount, wallMs, overallAvgMs, qps);
    }

    private static void warmupAndVerify(Connection conn, int rows) throws SQLException {
        try (Statement s = conn.createStatement()) {
            for (int q = 0; q < queries.length; q++) {
                try (ResultSet rs = s.executeQuery(queries[q])) {
                    if (!rs.next()) {
                        throw new IllegalStateException("warmup empty");
                    }
                    long val = rs.getLong(1);
                    if (queries[q].startsWith("SELECT COUNT(*)")) {
                        double ratio = (double) val / rows;
                        if (ratio <= 0.5) {
                            throw new IllegalStateException(
                                    "query " + (q + 1) + " matched <= 50%: " + val + "/" + rows);
                        }
                    }
                }
            }
            for (String sql : queries) {
                try (ResultSet rs = s.executeQuery(sql)) {
                    rs.next();
                }
            }
        }
    }
}

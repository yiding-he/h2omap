/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.bench;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * 多线程并发查询基准：100 万行 OMAP 表，每条查询均命中超过 50% 的记录。
 *
 * <p>命令行：{@code java com.hyd.h2omap.bench.ConcurrentQueryBench [threads] [rows] [rounds] [url]}。
 */
public final class ConcurrentQueryBench {

    private static final String[] QUERIES = {
            "SELECT COUNT(*) FROM T WHERE INT_COL * 2 >= 900000",
            "SELECT COUNT(*) FROM T WHERE LOW_CARD IN ('C0','C1','C2','C3','C4','C5')",
            "SELECT SUM(INT_COL) FROM T WHERE INT_COL >= 400000",
            "SELECT COUNT(*) FROM T WHERE LOW_CARD <> 'C9' AND INT_COL >= 10000 "
                    + "AND DOUBLE_COL < 950 AND TS_COL >= TIMESTAMP '2026-02-01' "
                    + "AND NULLABLE_COL IS NOT NULL AND CARDINALITY(ARRAY_COL) >= 1 "
                    + "AND CARDINALITY(STR_ARRAY_COL) >= 1",
    };

    private ConcurrentQueryBench() {
    }

    public static void main(String[] args) throws Exception {
        int threads = Math.max(1, args.length > 0 ? Integer.parseInt(args[0]) : 1);
        int rows = args.length > 1 ? Integer.parseInt(args[1]) : 1_000_000;
        int rounds = Math.max(1, args.length > 2 ? Integer.parseInt(args[2]) : 5);
        String url = args.length > 3 ? args[3]
                : "jdbc:h2:mem:omapconcurrent;DB_CLOSE_DELAY=-1;QUERY_CACHE_SIZE=0";

        try (Connection setupConn = DriverManager.getConnection(url, "sa", "")) {
            DataGenerator.generate(setupConn, rows, false);
            warmupAndVerify(setupConn, rows);
        }

        AtomicLongArray totalNanos = new AtomicLongArray(QUERIES.length);
        List<Throwable> errors = new ArrayList<>();
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Thread> workers = new ArrayList<>(threads);

        for (int t = 0; t < threads; t++) {
            Thread worker = new Thread(() -> {
                try (Connection c = DriverManager.getConnection(url, "sa", "");
                        Statement s = c.createStatement()) {
                    ready.countDown();
                    start.await();
                    for (int r = 0; r < rounds; r++) {
                        for (int q = 0; q < QUERIES.length; q++) {
                            long t0 = System.nanoTime();
                            try (ResultSet rs = s.executeQuery(QUERIES[q])) {
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
            workers.add(worker);
            worker.start();
        }

        ready.await();
        long wallStart = System.nanoTime();
        start.countDown();
        for (Thread worker : workers) {
            worker.join();
        }
        long wallMs = (System.nanoTime() - wallStart) / 1_000_000;

        if (!errors.isEmpty()) {
            throw new RuntimeException("concurrent bench failed", errors.get(0));
        }

        long totalExecutions = (long) threads * rounds;
        long sumAllNanos = 0;
        for (int q = 0; q < QUERIES.length; q++) {
            long nanos = totalNanos.get(q);
            sumAllNanos += nanos;
            double avgMs = nanos / 1_000_000.0 / totalExecutions;
            System.out.printf("threads=%d q%d avg_ms=%.2f : %s%n",
                    threads, q + 1, avgMs, QUERIES[q]);
        }
        long totalQueryCount = totalExecutions * QUERIES.length;
        double overallAvgMs = sumAllNanos / 1_000_000.0 / totalQueryCount;
        double qps = totalQueryCount * 1000.0 / Math.max(1L, wallMs);
        System.out.printf("SUMMARY threads=%d rows=%d rounds=%d total_queries=%d wall_ms=%d "
                + "overall_avg_ms=%.2f qps=%.1f%n",
                threads, rows, rounds, totalQueryCount, wallMs, overallAvgMs, qps);
    }

    private static void warmupAndVerify(Connection conn, int rows) throws SQLException {
        try (Statement s = conn.createStatement()) {
            for (int q = 0; q < QUERIES.length; q++) {
                try (ResultSet rs = s.executeQuery(QUERIES[q])) {
                    if (!rs.next()) {
                        throw new IllegalStateException("warmup empty");
                    }
                    long val = rs.getLong(1);
                    if (QUERIES[q].startsWith("SELECT COUNT(*)")) {
                        double ratio = (double) val / rows;
                        if (ratio <= 0.5) {
                            throw new IllegalStateException(
                                    "query " + (q + 1) + " matched <= 50%: " + val + "/" + rows);
                        }
                    }
                }
            }
            // 第二轮预热确保 JIT 稳定
            for (String sql : QUERIES) {
                try (ResultSet rs = s.executeQuery(sql)) {
                    rs.next();
                }
            }
        }
    }
}

/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.stream.Stream;

import org.h2.engine.Database;
import org.h2.engine.SessionLocal;
import org.h2.jdbc.JdbcConnection;
import org.h2.message.DbException;
import org.h2.value.CompareMode;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueInteger;
import org.h2.value.ValueNull;
import org.h2.value.ValueVarchar;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.hyd.h2omap.engine.OmapOptions;
import com.hyd.h2omap.persistence.OmapPersistence;
import com.hyd.h2omap.stats.QueryIoStats;
import com.hyd.h2omap.stats.ZoneMap;

/**
 * 段文件 v2 格式：尾部列目录、全量读免重算 zone map、部分列读、manifest 只认 v2 魔数。
 */
class SegmentFileFormatTest {

    @TempDir
    Path dir;

    private static Database database;

    @BeforeAll
    static void openSession() throws Exception {
        Connection c = DriverManager.getConnection(
                "jdbc:h2:mem:segmentFileTest;DB_CLOSE_DELAY=-1", "sa", "");
        database = ((SessionLocal) ((JdbcConnection) c).getSession()).getDatabase();
    }

    private static ColumnarStore newStore(int segmentSize) {
        return new ColumnarStore(
                new org.h2.table.Column[] {
                        new org.h2.table.Column("A", TypeInfo.getTypeInfo(Value.INTEGER)),
                        new org.h2.table.Column("B", TypeInfo.getTypeInfo(Value.VARCHAR)) },
                OmapOptions.from(List.of("segmentSize=" + segmentSize,
                        "compactionMinRows=" + Integer.MAX_VALUE)),
                CompareMode.getInstance(null, 0));
    }

    private static Value[] row(int a, String b) {
        return new Value[] {
                ValueInteger.get(a),
                b == null ? ValueNull.INSTANCE : ValueVarchar.get(b) };
    }

    private static OmapPersistence newPersistence(Path dir) {
        return new OmapPersistence(dir, database, CompareMode.getInstance(null, 0), 60_000);
    }

    /**
     * 全量读 round-trip：值、删除计数与尾部目录恢复的 zone map 全部一致。
     */
    @Test
    void segmentRoundTripRestoresValuesZonesAndDeletedCount() throws IOException {
        Path storeDir = dir.resolve("t1");
        ColumnarStore store = newStore(4);
        OmapPersistence p = newPersistence(storeDir);
        store.setPersistence(p);
        long[] ids = new long[8];
        ids[0] = store.append(row(0, "v0"));
        ids[1] = store.append(row(1, "v1"));
        ids[2] = store.append(row(2, null));
        ids[3] = store.append(row(3, "v3"));
        ids[4] = store.append(row(40, "v4"));
        ids[5] = store.append(row(50, "v5"));
        ids[6] = store.append(row(60, "v6"));
        ids[7] = store.append(row(70, "v7"));
        store.remove(ids[1]);
        store.compact();
        store.checkpoint();
        List<ColumnSegment> segs = store.segments();
        assertEquals(2, segs.size());
        p.close();

        OmapPersistence r = newPersistence(storeDir);
        try {
            ColumnSegment s0 = r.loadSegment(0, segs.get(0).getGeneration());
            assertEquals(4, s0.getRowCount());
            assertEquals(1, s0.getDeletedCount());
            for (int i = 0; i < 4; i++) {
                assertEquals(row(i, i == 1 ? "v1" : i == 2 ? null : "v" + i)[0],
                        s0.getColumn(0).get(i));
            }
            assertEquals("v0", s0.getColumn(1).get(0).getString());
            assertEquals("v1", s0.getColumn(1).get(1).getString());
            assertEquals(ValueNull.INSTANCE, s0.getColumn(1).get(2));
            assertEquals("v3", s0.getColumn(1).get(3).getString());

            // 尾部目录恢复的 zone map 与重算一致。
            for (int c = 0; c < 2; c++) {
                ZoneMap footer = s0.getZoneMap(c);
                ZoneMap recomputed = ZoneMap.of(s0.getColumn(c), CompareMode.getInstance(null, 0));
                assertEquals(recomputed.getMin(), footer.getMin());
                assertEquals(recomputed.getMax(), footer.getMax());
            }
            assertEquals(ValueInteger.get(3), s0.getZoneMap(0).getMax());
            assertEquals("v3", s0.getZoneMap(1).getMax().getString());

            ColumnSegment s1 = r.loadSegment(1, segs.get(1).getGeneration());
            assertEquals(4, s1.getRowCount());
            assertEquals(0, s1.getDeletedCount());
            assertEquals(ValueInteger.get(70), s1.getZoneMap(0).getMax());
            assertEquals("v7", s1.getColumn(1).get(7 - 4).getString());

            // 部分列读：只请求 B 列，A 列不在切片里，值与 zone map 与全量读一致。
            ColumnSlice slice = r.readSegmentColumns(0, segs.get(0).getGeneration(),
                    new int[] { 1 });
            assertEquals(4, slice.getRowCount());
            assertEquals(1, slice.getDeletedCount());
            assertTrue(slice.isDeleted(1));
            assertEquals("v0", slice.getVector(1).get(0).getString());
            assertEquals(ValueNull.INSTANCE, slice.getVector(1).get(2));
            assertEquals("v3", slice.getVector(1).get(3).getString());
            assertEquals("v0", slice.getZoneMap(1).getMin().getString());
            assertThrows(IllegalArgumentException.class, () -> slice.getVector(0));
        } finally {
            r.close();
        }
    }

    /**
     * 全 null 列的 zone map 恒相交：尾部目录存 hasMin=false，恢复后不参与剪枝。
     */
    @Test
    void allNullColumnKeepsAlwaysIntersectZone() throws IOException {
        Path storeDir = dir.resolve("t2");
        ColumnarStore store = newStore(4);
        OmapPersistence p = newPersistence(storeDir);
        store.setPersistence(p);
        for (int i = 0; i < 4; i++) {
            store.append(row(i, null));
        }
        store.checkpoint();
        p.close();

        OmapPersistence r = newPersistence(storeDir);
        try {
            ColumnSegment s0 = r.loadSegment(0, 0);
            assertEquals(4, s0.getRowCount());
            assertNull(s0.getZoneMap(1).getMin());
            assertNull(s0.getZoneMap(1).getMax());
            assertTrue(s0.getZoneMap(1).mayIntersect(null, null));
        } finally {
            r.close();
        }
    }

    /**
     * v1 魔数（OMAP）的 manifest 不再可读：旧格式回退已删除。
     */
    @Test
    void v1ManifestIsRejected() throws IOException {
        Path storeDir = dir.resolve("t3");
        Files.createDirectories(storeDir);
        // 0x4F4D4150 = "OMAP"，旧 v1 魔数
        Files.write(storeDir.resolve("manifest.dat"),
                new byte[] { 0x4F, 0x4D, 0x41, 0x50, 0, 0, 0, 0 });
        OmapPersistence p = newPersistence(storeDir);
        try {
            assertThrows(DbException.class, () -> p.recover(newStore(4)));
        } finally {
            p.close();
        }
    }

    /**
     * 恢复链路：checkpoint 后重开库，行数与内容与源一致。
     */
    @Test
    void recoverRestoresFrozenSegmentsIntoStore() throws IOException {
        Path storeDir = dir.resolve("t4");
        ColumnarStore source = newStore(4);
        OmapPersistence p = newPersistence(storeDir);
        source.setPersistence(p);
        long[] ids = new long[9];
        for (int i = 0; i < 9; i++) {
            ids[i] = source.append(row(i, "v" + i));
        }
        source.remove(ids[2]);
        source.compact();
        source.checkpoint();
        p.close();

        OmapPersistence r = newPersistence(storeDir);
        try {
            ColumnarStore target = newStore(4);
            r.recover(target);
            assertEquals(8, target.getRowCount());
            assertEquals("v0", target.materialize(ids[0]).getValueList()[1].getString());
            assertEquals("v8", target.materialize(ids[8]).getValueList()[1].getString());
            assertNull(target.materialize(ids[2]));
        } finally {
            r.close();
        }
    }

    /**
     * 并行恢复中任一段文件损坏：恢复整体失败并抛出异常，
     * 不允许静默丢段后以缺数据的库打开。
     */
    @Test
    void recoverFailsWhenSegmentFileCorrupted() throws IOException {
        Path storeDir = dir.resolve("t5");
        ColumnarStore source = newStore(4);
        OmapPersistence p = newPersistence(storeDir);
        source.setPersistence(p);
        for (int i = 0; i < 9; i++) {
            source.append(row(i, "v" + i));
        }
        source.checkpoint();
        p.close();

        // 损坏最后一段（段号最大的文件）的魔数
        List<Path> segFiles;
        try (Stream<Path> list = Files.list(storeDir)) {
            segFiles = list.filter(f -> f.getFileName().toString().startsWith("seg_")).sorted().toList();
        }
        assertTrue(segFiles.size() >= 2, "应有至少两个冻结段");
        Path victim = segFiles.get(segFiles.size() - 1);
        byte[] head = Files.readAllBytes(victim);
        head[0] = 'X';
        Files.write(victim, head);

        OmapPersistence r = newPersistence(storeDir);
        try {
            ColumnarStore target = newStore(4);
            assertThrows(DbException.class, () -> r.recover(target));
        } finally {
            r.close();
        }
    }

    /**
     * 并发读同一段同一列集时合并成一次读盘：只有一个线程产生 IO 记录，总字节数不超过单份的两倍。
     */
    @Test
    void concurrentSegmentReadsAreCoalesced() throws Exception {
        Path storeDir = dir.resolve("t-concurrent");
        ColumnarStore store = newStore(4);
        OmapPersistence p = newPersistence(storeDir);
        store.setPersistence(p);
        for (int i = 0; i < 8; i++) {
            store.append(row(i, "v" + i));
        }
        List<ColumnSegment> segs = store.segments();
        assertTrue(segs.size() >= 1);
        int segmentId = segs.get(0).getSegmentId();
        int generation = segs.get(0).getGeneration();

        boolean previous = QueryIoStats.isEnabled();
        QueryIoStats.setEnabled(true);
        try {
            QueryIoStats.begin();
            ColumnSlice single = p.readSegmentColumns(segmentId, generation, new int[] { 1 });
            QueryIoStats.end();
            QueryIoStats singleStats = QueryIoStats.pollLast();
            assertTrue(singleStats != null && singleStats.getTotalBytes() > 0, "串行读应产生统计");

            int threads = 16;
            long sequential = 0;
            for (int i = 0; i < threads; i++) {
                QueryIoStats.begin();
                p.readSegmentColumns(segmentId, generation, new int[] { 1 });
                QueryIoStats.end();
                QueryIoStats st = QueryIoStats.pollLast();
                sequential += st.getTotalBytes();
            }
            assertEquals(singleStats.getTotalBytes() * threads, sequential, "串行读每次都应读盘");

            java.util.concurrent.CyclicBarrier barrier = new java.util.concurrent.CyclicBarrier(threads);
            java.util.concurrent.atomic.AtomicLong totalBytes = new java.util.concurrent.atomic.AtomicLong();
            java.util.concurrent.atomic.AtomicInteger failures = new java.util.concurrent.atomic.AtomicInteger();
            java.util.concurrent.atomic.AtomicInteger ioThreads = new java.util.concurrent.atomic.AtomicInteger();
            java.util.concurrent.CountDownLatch done = new java.util.concurrent.CountDownLatch(threads);
            for (int t = 0; t < threads; t++) {
                Thread th = new Thread(() -> {
                    try {
                        barrier.await();
                        QueryIoStats.begin();
                        ColumnSlice slice = p.readSegmentColumns(segmentId, generation, new int[] { 1 });
                        QueryIoStats.end();
                        QueryIoStats st = QueryIoStats.pollLast();
                        if (!"v0".equals(slice.getVector(1).get(0).getString())) {
                            failures.incrementAndGet();
                        }
                        if (st != null && !st.isEmpty()) {
                            ioThreads.incrementAndGet();
                            totalBytes.addAndGet(st.getTotalBytes());
                        }
                    } catch (Exception e) {
                        failures.incrementAndGet();
                    } finally {
                        done.countDown();
                    }
                });
                th.start();
            }
            done.await();
            assertEquals(0, failures.get(), "并发读结果应一致");
            assertTrue(ioThreads.get() < threads,
                    "并发 " + threads + " 次读盘应合并，实际 " + ioThreads.get() + " 个线程读盘");
            assertTrue(totalBytes.get() <= sequential / 2,
                    "并发读总字节应不超过串行 " + threads + " 份的一半，串行 " + sequential
                            + "，并发 " + totalBytes.get());
        } finally {
            QueryIoStats.setEnabled(previous);
            p.close();
        }
    }

    /**
     * 列切片缓存：逐出段的同一段同一列集重复读命中缓存（IO 归零、返回同一向量实例）。
     */
    @Test
    void sliceCacheServesRepeatReadsOfEvictedSegment() throws IOException {
        boolean previousStats = QueryIoStats.isEnabled();
        QueryIoStats.setEnabled(true);
        OmapPersistence p = null;
        try {
            p = newPersistence(dir.resolve("t-slice-cache-hit"));
            ColumnarStore store = newColumnarStore("segmentSize=4", "compactionMinRows=1",
                    "residentBytes=1");
            assertTrue(store.sliceCache.isEnabled(), "默认 32 MiB 预算下缓存应开启");
            store.setPersistence(p);
            for (int i = 0; i < 8; i++) {
                store.append(row(i * 10, "v" + i));
            }
            store.checkpoint();
            ColumnarStore.SegmentRef ref = store.segments.values().iterator().next();
            assertNull(ref.loaded, "residentBytes=1 应把段全部逐出");
            int[] columns = { 0, 1 };

            QueryIoStats.begin();
            ColumnSegment first = store.readSliceTransient(ref, columns);
            QueryIoStats.end();
            QueryIoStats firstStats = QueryIoStats.pollLast();
            assertTrue(firstStats != null && firstStats.getTotalBytes() > 0, "首次读应读盘");

            QueryIoStats.begin();
            ColumnSegment second = store.readSliceTransient(ref, columns);
            QueryIoStats.end();
            QueryIoStats secondStats = QueryIoStats.pollLast();
            assertTrue(secondStats == null || secondStats.isEmpty(), "第二次读应命中切片缓存");
            assertSame(first.getColumn(0), second.getColumn(0), "命中缓存应复用同一列向量");
            assertEquals("v0", second.getColumn(1).get(0).getString());
            assertEquals(1, store.sliceCache.size());
            assertTrue(store.sliceCache.getUsedBytes() > 0);
            p.close();
            p = null;
        } finally {
            QueryIoStats.setEnabled(previousStats);
            if (p != null) {
                p.close();
            }
        }
    }

    /**
     * 列切片缓存的失效：合并换 generation 后旧条目立即清除，下一次读重新读盘并读到新代数。
     */
    @Test
    void sliceCacheInvalidatesWhenSegmentIsCompacted() throws IOException {
        boolean previousStats = QueryIoStats.isEnabled();
        QueryIoStats.setEnabled(true);
        OmapPersistence p = null;
        try {
            p = newPersistence(dir.resolve("t-slice-cache-invalid"));
            ColumnarStore store = newColumnarStore("segmentSize=4", "compactionMinRows=1");
            store.setPersistence(p);
            long[] ids = new long[8];
            for (int i = 0; i < 8; i++) {
                ids[i] = store.append(row(i * 10, "v" + i));
            }
            store.checkpoint();
            // 预算只够留一段驻留，另一段被逐出，制造真实的逐出段读路径。
            long segBytes = store.segments().get(0).getEstimatedMemory();
            store.setResidentBudget(segBytes);
            ColumnarStore.SegmentRef ref = null;
            for (ColumnarStore.SegmentRef r : store.segments.values()) {
                if (r.loaded == null) {
                    ref = r;
                    break;
                }
            }
            assertNotNull(ref, "预算只够一段，另一段应被逐出");
            int[] columns = { 0, 1 };

            QueryIoStats.begin();
            store.readSliceTransient(ref, columns);
            QueryIoStats.end();
            assertEquals(1, store.sliceCache.size());

            store.update(ids[0], row(999, "z"));
            store.compact();
            int newGeneration = ref.generation;
            assertTrue(newGeneration > 0, "合并应递增 generation");
            assertEquals(0, store.sliceCache.size(), "合并后该段的旧缓存条目应被清除");
            assertNull(store.sliceCache.get(ref.segmentId, 0,
                    SliceCache.maskOf(columns)), "旧代条目应不再命中");

            // 重新逐出后读新代：应读盘并把新代切片放进缓存。
            // 先 checkpoint 把新代段文件写盘，否则无文件可读回、不会逐出。
            store.checkpoint();
            store.setResidentBudget(1);
            assertNull(ref.loaded, "预算调小后段应再次被逐出");
            QueryIoStats.begin();
            store.readSliceTransient(ref, columns);
            QueryIoStats.end();
            QueryIoStats stats = QueryIoStats.pollLast();
            assertTrue(stats != null && !stats.isEmpty(), "旧条目失效后应重新读盘");
            assertEquals(newGeneration, stats.get(0).generation(), "重新读盘应读合并后的代数");
            assertEquals(1, store.sliceCache.size(), "新代切片应重新入缓存");
            p.close();
            p = null;
        } finally {
            QueryIoStats.setEnabled(previousStats);
            if (p != null) {
                p.close();
            }
        }
    }

    /**
     * 预算调小立即按 LRU 淘汰到新一档：缓存里唯一的条目超出新预算时立即被消掉，
     * 下一次读重新读盘并重新入缓存。
     */
    @Test
    void sliceCacheBudgetShrinkEvictsImmediately() throws IOException {
        boolean previousStats = QueryIoStats.isEnabled();
        QueryIoStats.setEnabled(true);
        OmapPersistence p = null;
        try {
            p = newPersistence(dir.resolve("t-slice-cache-budget"));
            ColumnarStore store = newColumnarStore("segmentSize=4", "compactionMinRows=1",
                    "residentBytes=1");
            store.setPersistence(p);
            for (int i = 0; i < 8; i++) {
                store.append(row(i * 10, "v" + i));
            }
            store.checkpoint();
            ColumnarStore.SegmentRef ref = store.segments.values().iterator().next();
            int[] columns = { 0, 1 };

            QueryIoStats.begin();
            store.readSliceTransient(ref, columns);
            QueryIoStats.end();
            assertEquals(1, store.sliceCache.size());
            long used = store.sliceCache.getUsedBytes();
            assertTrue(used > 0);

            // 调小到容不下已有条目：立即淘汰，不需要等下一次访问。
            store.setSliceCacheBudget(used - 1);
            assertEquals(0, store.sliceCache.size(), "调小预算应立即按 LRU 淘汰");

            // 恢复预算后重读：重新读盘并重新入缓存。
            store.setSliceCacheBudget(com.hyd.h2omap.engine.OmapOptions.DEFAULT_SLICE_CACHE_BYTES);
            QueryIoStats.begin();
            store.readSliceTransient(ref, columns);
            QueryIoStats.end();
            QueryIoStats stats = QueryIoStats.pollLast();
            assertTrue(stats != null && !stats.isEmpty(), "条目被淘汰后应重新读盘");
            assertEquals(1, store.sliceCache.size());
            p.close();
            p = null;
        } finally {
            QueryIoStats.setEnabled(previousStats);
            if (p != null) {
                p.close();
            }
        }
    }

    /**
     * 快照与并发驻留的竞态：懒段在快照后已被并发读回/预载驻留（loaded 挂上、
     * skeleton 被清）时，首次读列应直接复用驻留内容，不得回填全占位向量后抛异常。
     */
    @Test
    void snapshotSegmentRacingWithResidentReadback() throws IOException {
        boolean previousStats = QueryIoStats.isEnabled();
        QueryIoStats.setEnabled(true);
        OmapPersistence p = null;
        try {
            p = newPersistence(dir.resolve("t-race-resident"));
            ColumnarStore store = newColumnarStore("segmentSize=4", "compactionMinRows=1",
                    "residentBytes=1");
            store.setPersistence(p);
            for (int i = 0; i < 8; i++) {
                store.append(row(i * 10, "v" + i));
            }
            store.checkpoint();
            ColumnarStore.SegmentRef ref = store.segments.values().iterator().next();
            assertNull(ref.loaded, "residentBytes=1 应把段全部逐出");

            ColumnSegment resident = store.readSegmentTransient(ref);
            assertEquals("v0", resident.getColumn(1).get(0).getString());

            // 拍快照：逐出段被包装成懒段，首次读列才按部分列读回
            VectorizedSnapshot snap = SnapshotQuery.take(store, null, null, new int[] { 0, 1 });
            ColumnSegment lazy = snap.segments[0];

            // 模拟快照后另一线程经 ensureResident 把段驻留：loaded 挂上、skeleton 被清。
            // 修复前：懒段首次读列判 skeleton==null 不可读回，回填全占位向量，
            // 触碰请求列抛 UnsupportedOperationException。
            ref.loaded = resident;
            ref.skeleton = null;

            assertEquals("v0", lazy.getColumn(1).get(0).getString());
            assertEquals("v3", lazy.getColumn(1).get(3).getString());
            assertSame(resident, store.readSegmentTransient(ref),
                    "已驻留段应直接返回驻留内容，不再走临时读");

            // 驻留后懒段读列不应再触发磁盘读
            QueryIoStats.begin();
            assertEquals("v1", lazy.getColumn(1).get(1).getString());
            QueryIoStats.end();
            QueryIoStats stats = QueryIoStats.pollLast();
            assertTrue(stats == null || stats.isEmpty(), "驻留后懒段读列不应再读盘");
            p.close();
            p = null;
        } finally {
            QueryIoStats.setEnabled(previousStats);
            if (p != null) {
                p.close();
            }
        }
    }

    private static ColumnarStore newColumnarStore(String... params) {
        return new ColumnarStore(
                new org.h2.table.Column[] {
                        new org.h2.table.Column("A", TypeInfo.getTypeInfo(Value.INTEGER)),
                        new org.h2.table.Column("B", TypeInfo.getTypeInfo(Value.VARCHAR)) },
                OmapOptions.from(List.of(params)),
                CompareMode.getInstance(null, 0));
    }
}

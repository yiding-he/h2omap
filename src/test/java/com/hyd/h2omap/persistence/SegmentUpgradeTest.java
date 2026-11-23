/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;

import org.h2.engine.Database;
import org.h2.jdbc.JdbcConnection;
import org.h2.message.DbException;
import org.h2.table.Column;
import org.h2.value.CompareMode;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueBigint;
import org.h2.value.ValueInteger;
import org.h2.value.ValueNull;
import org.h2.value.ValueVarchar;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.hyd.h2omap.engine.OmapOptions;
import com.hyd.h2omap.stats.ZoneMap;
import com.hyd.h2omap.storage.ColumnSegment;
import com.hyd.h2omap.storage.ColumnarStore;
import com.hyd.h2omap.storage.ResidentBudget;
import com.hyd.h2omap.vector.NullBitmap;

/**
 * 启动自动升级：v2 段文件（OSG2）与 v2 manifest（OMAPQ）在开库时逐段转换为 v3
 * 并原子重写 manifest；重入幂等；失败拒绝开库。
 */
class SegmentUpgradeTest {

    private static final int V2_SEGMENT_MAGIC = 0x4F534732;   // OSG2
    private static final int V2_MANIFEST_MAGIC = 0x4F4D4151;  // OMAPQ

    @TempDir
    Path dir;

    private static Database database;
    private static ValueCodec codec;

    @BeforeAll
    static void setUp() throws Exception {
        Connection c = DriverManager.getConnection(
                "jdbc:h2:mem:segmentUpgradeTest;DB_CLOSE_DELAY=-1", "sa", "");
        database = ((org.h2.engine.SessionLocal) ((JdbcConnection) c).getSession()).getDatabase();
        codec = new ValueCodec(database);
    }

    @AfterAll
    static void tearDown() throws Exception {
        DriverManager.getConnection("jdbc:h2:mem:segmentUpgradeTest;DB_CLOSE_DELAY=-1", "sa", "")
                .createStatement().execute("SHUTDOWN");
    }

    private static Column[] columns() {
        return new Column[] {
                new Column("A", TypeInfo.getTypeInfo(Value.INTEGER)),
                new Column("B", TypeInfo.getTypeInfo(Value.VARCHAR)) };
    }

    private static ColumnarStore newStore() {
        return new ColumnarStore(columns(),
                OmapOptions.from(List.of("segmentSize=4", "compactionMinRows=100000")),
                CompareMode.getInstance(null, 0), new ResidentBudget(0));
    }

    /** 行首元素是 rowId，其余按列。b 为 null 时 B 列含 NULL。 */
    private static Value[] row(long rowId, int a, String b) {
        return new Value[] {
                ValueInteger.get((int) rowId),
                ValueInteger.get(a),
                b == null ? ValueNull.INSTANCE : ValueVarchar.get(b) };
    }

    private static ColumnSegment buildSegment(int segId, List<Value[]> rows) {
        return ColumnSegment.build(segId, rows,
                new TypeInfo[] { TypeInfo.getTypeInfo(Value.INTEGER),
                        TypeInfo.getTypeInfo(Value.VARCHAR) },
                CompareMode.getInstance(null, 0));
    }

    /** 手工构造 v2 段文件：头部无版本字段，尾部目录每项不含 hasNull。 */
    private static void writeV2Segment(Path file, ColumnSegment seg) throws IOException {
        int cols = seg.getColumnCount();
        long[] offsets = new long[cols];
        int[] lengths = new int[cols];
        ByteArrayOutputStream buf = new ByteArrayOutputStream(64 * 1024);
        long footerOffset;
        byte[] bytes;
        try (DataOutputStream out = new DataOutputStream(buf)) {
            out.writeInt(V2_SEGMENT_MAGIC);
            out.writeInt(seg.getSegmentId());
            out.writeInt(seg.getGeneration());
            out.writeInt(seg.getRowCount());
            out.writeInt(seg.getDeletedCount());
            out.writeLong(0L);
            codec.writeNullBitmap(out, seg.getDeleted());
            out.writeInt(cols);
            for (int c = 0; c < cols; c++) {
                offsets[c] = out.size();
                codec.writeVector(out, seg.getColumn(c));
                lengths[c] = out.size() - (int) offsets[c];
            }
            footerOffset = out.size();
            for (int c = 0; c < cols; c++) {
                out.writeLong(offsets[c]);
                out.writeInt(lengths[c]);
                writeV2ZoneEntry(out, seg.getZoneMap(c));
            }
        }
        bytes = buf.toByteArray();
        patchLong(bytes, 20, footerOffset);
        Files.write(file, bytes);
    }

    private static void writeV2ZoneEntry(DataOutputStream out, ZoneMap zone) throws IOException {
        Value min = zone.getMin();
        out.writeBoolean(min != null);
        if (min != null) {
            codec.writeValue(out, min);
        }
        Value max = zone.getMax();
        out.writeBoolean(max != null);
        if (max != null) {
            codec.writeValue(out, max);
        }
    }

    /** 手工构造 v2 manifest：骨架不含每列 hasNull。 */
    private static void writeV2Manifest(Path file, ColumnSegment[] segs) throws IOException {
        try (DataOutputStream out = new DataOutputStream(
                Files.newOutputStream(file))) {
            out.writeInt(V2_MANIFEST_MAGIC);
            out.writeInt(segs.length);          // nextSegmentId
            out.writeLong(1L);                   // manifestWalSeq
            out.writeInt(segs.length);
            for (ColumnSegment seg : segs) {
                out.writeInt(seg.getSegmentId());
                out.writeInt(seg.getGeneration());
                out.writeBoolean(true);          // hasSkeleton
                out.writeInt(seg.getRowCount());
                out.writeInt(seg.getDeletedCount());
                codec.writeNullBitmap(out, seg.getDeleted());
                out.writeInt(seg.getColumnCount());
                for (int c = 0; c < seg.getColumnCount(); c++) {
                    writeV2ZoneEntry(out, seg.getZoneMap(c));
                }
            }
        }
    }

    private static void patchLong(byte[] bytes, int offset, long value) {
        for (int i = 0; i < 8; i++) {
            bytes[offset + i] = (byte) (value >>> (8 * (7 - i)));
        }
    }

    private static int readInt(Path file, int position) throws IOException {
        byte[] head = Files.readAllBytes(file);
        ByteBuffer buf = ByteBuffer.wrap(head, position, 4);
        return buf.getInt();
    }

    /** 段 0 的 B 列含 NULL，段 1 两列都无 NULL；两段 zone 区间不相交。 */
    private static ColumnSegment[] createV2Directory(Path storeDir) throws IOException {
        Files.createDirectories(storeDir);
        List<Value[]> rows0 = List.of(
                row(0, 1, "a"), row(1, 2, null), row(2, 3, "c"), row(3, 4, "d"));
        List<Value[]> rows1 = List.of(
                row(4, 21, "e"), row(5, 22, "f"), row(6, 23, "g"), row(7, 24, "h"));
        ColumnSegment seg0 = buildSegment(0, rows0);
        ColumnSegment seg1 = buildSegment(1, rows1);
        writeV2Segment(storeDir.resolve("seg_0_0.omap"), seg0);
        writeV2Segment(storeDir.resolve("seg_1_0.omap"), seg1);
        writeV2Manifest(storeDir.resolve("manifest.dat"), new ColumnSegment[] { seg0, seg1 });
        return new ColumnSegment[] { seg0, seg1 };
    }

    /**
     * v2 目录开库：全部段文件升级为 v3（魔数 + 版本字段），尾部目录含 hasNull，
     * manifest 重写为 OMAPR，数据不变，段 0 的 B 列 hasNull 为 true。
     */
    @Test
    void recoverUpgradesV2DirectoryToV3() throws IOException {
        Path storeDir = dir.resolve("upgrade");
        createV2Directory(storeDir);

        OmapPersistence p = new OmapPersistence(storeDir, database,
                CompareMode.getInstance(null, 0), 60_000);
        try {
            ColumnarStore store = newStore();
            p.recover(store);
            assertEquals(8, store.getRowCount());

            Path seg0 = storeDir.resolve("seg_0_0.omap");
            Path seg1 = storeDir.resolve("seg_1_0.omap");
            assertEquals(0x4F534733, readInt(seg0, 0), "升级后段文件应为 OSG3 魔数");
            assertEquals(3, readInt(seg0, 4), "升级后段文件版本号应为 3");
            assertEquals(0x4F534733, readInt(seg1, 0));
            assertEquals(0x4F4D4152, readInt(storeDir.resolve("manifest.dat"), 0),
                    "升级后 manifest 应为 OMAPR 魔数");

            ColumnSegment restored0 = p.loadSegment(0, 0);
            assertTrue(restored0.hasNull(1), "段 0 的 B 列含 NULL");
            assertFalse(restored0.hasNull(0), "段 0 的 A 列无 NULL");
            assertEquals(ValueNull.INSTANCE, restored0.getColumn(1).get(1));
            assertEquals("a", restored0.getColumn(1).get(0).getString());

            ColumnSegment restored1 = p.loadSegment(1, 0);
            assertFalse(restored1.hasNull(0), "段 1 两列都无 NULL");
            assertFalse(restored1.hasNull(1));
            assertEquals("h", restored1.getColumn(1).get(3).getString());
        } finally {
            p.close();
        }
    }

    /** 惰性恢复：升级回填的 hasNull 落在骨架里，未加载段零 I/O 即可判定。 */
    @Test
    void upgradeBackfillsHasNullIntoLazySkeleton() throws IOException {
        Path storeDir = dir.resolve("upgrade-lazy");
        createV2Directory(storeDir);

        OmapPersistence p = new OmapPersistence(storeDir, database,
                CompareMode.getInstance(null, 0), 60_000);
        try {
            ColumnarStore store = newStore();
            store.setLazyLoad(true);
            p.recover(store);
            ColumnarStore.SkeletonData sk0 = store.segmentSkeletonData(0);
            assertNotNull(sk0);
            assertTrue(sk0.hasNull[1], "段 0 的 B 列含 NULL");
            assertFalse(sk0.hasNull[0]);
            ColumnarStore.SkeletonData sk1 = store.segmentSkeletonData(1);
            assertNotNull(sk1);
            assertFalse(sk1.hasNull[0], "段 1 两列都无 NULL");
            assertFalse(sk1.hasNull[1]);
            assertEquals(8, store.getRowCount(), "骨架行数恢复后立即可见");
        } finally {
            p.close();
        }
    }

    /** 升级幂等：重开库走 v3 快速路径，段文件与 manifest 一个字节都不重写。 */
    @Test
    void upgradeIsIdempotentAcrossRestarts() throws IOException {
        Path storeDir = dir.resolve("upgrade-idempotent");
        createV2Directory(storeDir);

        OmapPersistence p = new OmapPersistence(storeDir, database,
                CompareMode.getInstance(null, 0), 60_000);
        ColumnarStore store = newStore();
        p.recover(store);
        p.close();
        byte[] seg0Bytes = Files.readAllBytes(storeDir.resolve("seg_0_0.omap"));
        byte[] seg1Bytes = Files.readAllBytes(storeDir.resolve("seg_1_0.omap"));
        byte[] manifestBytes = Files.readAllBytes(storeDir.resolve("manifest.dat"));

        OmapPersistence p2 = new OmapPersistence(storeDir, database,
                CompareMode.getInstance(null, 0), 60_000);
        try {
            ColumnarStore again = newStore();
            p2.recover(again);
            assertEquals(8, again.getRowCount());
            assertArrayEquals(seg0Bytes, Files.readAllBytes(storeDir.resolve("seg_0_0.omap")),
                    "v3 段重开库不应重写");
            assertArrayEquals(seg1Bytes, Files.readAllBytes(storeDir.resolve("seg_1_0.omap")),
                    "v3 段重开库不应重写");
            assertArrayEquals(manifestBytes, Files.readAllBytes(storeDir.resolve("manifest.dat")),
                    "v3 manifest 重开库不应重写");
        } finally {
            p2.close();
        }
    }

    /**
     * 升级中途崩溃重入：段文件已是 v3、manifest 仍是 v2（模拟段转换完成后、
     * manifest 重写前崩溃）。重入走 v3 快速分支：只读尾部目录回填骨架 hasNull、
     * 不重写段文件，随后重写 manifest。
     */
    @Test
    void upgradeResumesFromPartiallyUpgradedState() throws IOException {
        Path storeDir = dir.resolve("upgrade-resume");
        ColumnSegment[] segs = createV2Directory(storeDir);
        OmapPersistence p = new OmapPersistence(storeDir, database,
                CompareMode.getInstance(null, 0), 60_000);
        ColumnarStore store = newStore();
        p.recover(store);
        p.close();
        byte[] seg0V3 = Files.readAllBytes(storeDir.resolve("seg_0_0.omap"));
        byte[] seg1V3 = Files.readAllBytes(storeDir.resolve("seg_1_0.omap"));
        // 人工把 manifest 拨回 v2：段文件停留在升级后状态
        writeV2Manifest(storeDir.resolve("manifest.dat"), segs);

        OmapPersistence p2 = new OmapPersistence(storeDir, database,
                CompareMode.getInstance(null, 0), 60_000);
        try {
            ColumnarStore again = newStore();
            again.setLazyLoad(true);
            p2.recover(again);
            assertEquals(8, again.getRowCount());
            assertArrayEquals(seg0V3, Files.readAllBytes(storeDir.resolve("seg_0_0.omap")),
                    "已是 v3 的段重入升级不应重写");
            assertArrayEquals(seg1V3, Files.readAllBytes(storeDir.resolve("seg_1_0.omap")),
                    "已是 v3 的段重入升级不应重写");
            assertEquals(0x4F4D4152, readInt(storeDir.resolve("manifest.dat"), 0),
                    "重入升级后 manifest 应重写为 OMAPR");
            ColumnarStore.SkeletonData sk0 = again.segmentSkeletonData(0);
            assertNotNull(sk0);
            assertTrue(sk0.hasNull[1], "重入时 hasNull 应从 v3 尾部目录读回");
            assertFalse(sk0.hasNull[0]);
        } finally {
            p2.close();
        }
    }

    /**
     * 重入升级时 v3 段文件尾部目录损坏：恢复必须整体失败，不得静默成功。
     */
    @Test
    void resumeWithCorruptV3FooterFailsRecovery() throws IOException {
        Path storeDir = dir.resolve("upgrade-corrupt-footer");
        ColumnSegment[] segs = createV2Directory(storeDir);
        OmapPersistence p = new OmapPersistence(storeDir, database,
                CompareMode.getInstance(null, 0), 60_000);
        try {
            ColumnarStore store = newStore();
            p.recover(store);
            writeV2Manifest(storeDir.resolve("manifest.dat"), segs);
        } finally {
            p.close();
        }
        Path seg1 = storeDir.resolve("seg_1_0.omap");
        byte[] raw = Files.readAllBytes(seg1);
        Files.write(seg1, java.util.Arrays.copyOf(raw, raw.length - 4));

        OmapPersistence p2 = new OmapPersistence(storeDir, database,
                CompareMode.getInstance(null, 0), 60_000);
        try {
            ColumnarStore again = newStore();
            Exception ex = assertThrows(Exception.class, () -> p2.recover(again));
            Throwable cause = ex;
            boolean ioFailure = false;
            while (cause != null) {
                if (cause instanceof IOException) {
                    ioFailure = true;
                    break;
                }
                cause = cause.getCause();
            }
            assertTrue(ioFailure, "损坏的 v3 尾部目录必须让恢复失败，实际: " + ex);
        } finally {
            p2.close();
        }
    }

    /**
     * 升级中任一段文件魔数非法：恢复整体失败，manifest 不重写（仍 OMAPQ），
     * 下次启动可重入升级。
     */
    @Test
    void upgradeFailureKeepsV2ManifestForRetry() throws IOException {
        Path storeDir = dir.resolve("upgrade-failure");
        createV2Directory(storeDir);
        Path victim = storeDir.resolve("seg_1_0.omap");
        byte[] head = Files.readAllBytes(victim);
        head[0] = 'X';
        Files.write(victim, head);

        OmapPersistence p = new OmapPersistence(storeDir, database,
                CompareMode.getInstance(null, 0), 60_000);
        try {
            assertThrows(DbException.class, () -> p.recover(newStore()));
            assertEquals(V2_MANIFEST_MAGIC, readInt(storeDir.resolve("manifest.dat"), 0),
                    "升级失败后 manifest 不应重写");
        } finally {
            p.close();
        }
    }
}

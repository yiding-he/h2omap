/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
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
}

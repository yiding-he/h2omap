/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.storage;

import java.util.List;
import java.util.Map;

import org.h2.value.CompareMode;
import org.h2.value.Value;

import com.hyd.h2omap.stats.ZoneMap;
import com.hyd.h2omap.vector.ColumnVector;
import com.hyd.h2omap.vector.DictVarcharVector;
import com.hyd.h2omap.vector.NullBitmap;

/**
 * 列段。写满就换新段，段号与段内偏移在行的整个生命周期里都不变，
 * 所以 H2 已经拿到的 rowId 永远有效。
 *
 * <p>段不是严格不可变的：合并会把 delta 的新值写回本段，并给已删除
 * 的行打标记。这两件事都不改变 rowId，因此不需要快照。zone map 在
 * 值或删除标记变化后重算。
 */
public final class ColumnSegment {

    private final int segmentId;

    private final int rowCount;

    private final ColumnVector[] columns;

    private final ZoneMap[] zoneMaps;

    /** 删除标记。被标记的行对读路径不可见，rowId 保留。 */
    private final NullBitmap deleted;

    private final CompareMode compareMode;

    private ColumnSegment(int segmentId, int rowCount, ColumnVector[] columns, ZoneMap[] zoneMaps,
            NullBitmap deleted, CompareMode compareMode) {
        this.segmentId = segmentId;
        this.rowCount = rowCount;
        this.columns = columns;
        this.zoneMaps = zoneMaps;
        this.deleted = deleted;
        this.compareMode = compareMode;
    }

    private ColumnSegment(int segmentId, int rowCount, ColumnVector[] columns, ZoneMap[] zoneMaps,
            CompareMode compareMode) {
        this(segmentId, rowCount, columns, zoneMaps, new NullBitmap(rowCount), compareMode);
    }

    /**
     * 由一批行构建一个段。调用方保证 {@code rows} 不为空。
     *
     * @param segmentId   段号
     * @param rows        行数据，每行长度等于列数
     * @param valueTypes  每列的 H2 类型码，见 {@code org.h2.value.Value}
     * @param compareMode 数据库的比较模式
     * @return 新段
     */
    public static ColumnSegment build(int segmentId, List<Value[]> rows, int[] valueTypes,
            CompareMode compareMode) {
        int count = rows.size();
        int maxDict = Math.min(4096, Math.max(64, count / 4));
        ColumnVector[] vectors = new ColumnVector[valueTypes.length];
        boolean[] prebuilt = new boolean[valueTypes.length];
        for (int c = 0; c < valueTypes.length; c++) {
            int type = valueTypes[c];
            if (type == Value.CHAR || type == Value.VARCHAR) {
                DictVarcharVector dictVec = DictVarcharVector.tryBuild(rows, c + 1, maxDict);
                if (dictVec != null) {
                    vectors[c] = dictVec;
                    prebuilt[c] = true;
                    continue;
                }
            }
            vectors[c] = ColumnVector.create(type, count);
        }
        for (int r = 0; r < count; r++) {
            Value[] row = rows.get(r);
            for (int c = 0; c < valueTypes.length; c++) {
                if (!prebuilt[c]) {
                    vectors[c].set(r, row[c + 1]);
                }
            }
        }
        ZoneMap[] zones = new ZoneMap[valueTypes.length];
        for (int c = 0; c < valueTypes.length; c++) {
            zones[c] = ZoneMap.of(vectors[c], compareMode);
        }
        return new ColumnSegment(segmentId, count, vectors, zones, compareMode);
    }

    public int getSegmentId() {
        return segmentId;
    }

    public int getRowCount() {
        return rowCount;
    }

    public int getColumnCount() {
        return columns.length;
    }

    public Value get(int offset, int column) {
        return columns[column].get(offset);
    }

    public Value[] getRow(int offset) {
        Value[] values = new Value[columns.length];
        for (int c = 0; c < columns.length; c++) {
            values[c] = columns[c].get(offset);
        }
        return values;
    }

    public ColumnVector getColumn(int column) {
        return columns[column];
    }

    public ZoneMap getZoneMap(int column) {
        return zoneMaps[column];
    }

    public boolean isDeleted(int offset) {
        return deleted.isNull(offset);
    }

    /** 合并时标记某行已删除。 */
    public void markDeleted(int offset) {
        deleted.setNull(offset);
    }

    /** 合并时把 delta 里的新值写回本段的某一行。 */
    public void setRow(int offset, Value[] values) {
        for (int c = 0; c < columns.length; c++) {
            columns[c].set(offset, values[c]);
        }
    }

    /** 值或删除标记变化后重算某一列的 zone map。 */
    public void rebuildZoneMap(int column) {
        zoneMaps[column] = ZoneMap.of(columns[column], compareMode);
    }

    /**
     * Copy-On-Compact：基于当前段生成应用了 delta 更新与删除的新段实例，
     * 保证旧段在并发读线程的 {@link LazyOmapRow} 持有期间不被原地改写。
     */
    public ColumnSegment applyDelta(List<Integer> deletedOffsets, List<Map.Entry<Integer, Value[]>> updates) {
        NullBitmap newDeleted = this.deleted.copy();
        if (deletedOffsets != null) {
            for (int off : deletedOffsets) {
                newDeleted.setNull(off);
            }
        }
        if (updates == null || updates.isEmpty()) {
            return new ColumnSegment(segmentId, rowCount, columns, zoneMaps, newDeleted, compareMode);
        }
        ColumnVector[] newColumns = new ColumnVector[columns.length];
        for (int c = 0; c < columns.length; c++) {
            newColumns[c] = columns[c].copy();
        }
        for (Map.Entry<Integer, Value[]> entry : updates) {
            int off = entry.getKey();
            Value[] values = entry.getValue();
            for (int c = 0; c < newColumns.length; c++) {
                newColumns[c].set(off, values[c]);
            }
        }
        ZoneMap[] newZones = new ZoneMap[newColumns.length];
        for (int c = 0; c < newColumns.length; c++) {
            newZones[c] = ZoneMap.of(newColumns[c], compareMode);
        }
        return new ColumnSegment(segmentId, rowCount, newColumns, newZones, newDeleted, compareMode);
    }

    public long getEstimatedMemory() {
        long total = 64;
        for (ColumnVector vector : columns) {
            total += vector.getEstimatedMemory();
        }
        for (ZoneMap zoneMap : zoneMaps) {
            total += zoneMap.getEstimatedMemory();
        }
        return total + deleted.getEstimatedMemory();
    }
}

/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.storage;

import java.util.List;
import java.util.Map;

import org.h2.value.CompareMode;
import org.h2.value.ExtTypeInfo;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueNull;

import com.hyd.h2omap.stats.ZoneMap;
import com.hyd.h2omap.vector.ArrayVector;
import com.hyd.h2omap.vector.ColumnVector;
import com.hyd.h2omap.vector.DictValueVector;
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

    private final int generation;

    private final int rowCount;

    private final ColumnVector[] columns;

    private final ZoneMap[] zoneMaps;

    /** 删除标记。被标记的行对读路径不可见，rowId 保留。 */
    private final NullBitmap deleted;

    private final CompareMode compareMode;

    private ColumnSegment(int segmentId, int generation, int rowCount, ColumnVector[] columns,
            ZoneMap[] zoneMaps, NullBitmap deleted, CompareMode compareMode) {
        this.segmentId = segmentId;
        this.generation = generation;
        this.rowCount = rowCount;
        this.columns = columns;
        this.zoneMaps = zoneMaps;
        this.deleted = deleted;
        this.compareMode = compareMode;
    }

    private ColumnSegment(int segmentId, int rowCount, ColumnVector[] columns, ZoneMap[] zoneMaps,
            CompareMode compareMode) {
        this(segmentId, 0, rowCount, columns, zoneMaps, new NullBitmap(rowCount), compareMode);
    }

    public static ColumnSegment restore(int segmentId, int generation, int rowCount,
            ColumnVector[] columns, NullBitmap deleted, CompareMode compareMode) {
        ZoneMap[] zones = new ZoneMap[columns.length];
        for (int c = 0; c < columns.length; c++) {
            zones[c] = ZoneMap.of(columns[c], compareMode);
        }
        return new ColumnSegment(segmentId, generation, rowCount, columns, zones, deleted, compareMode);
    }

    /**
     * 由一批行构建一个段。调用方保证 {@code rows} 不为空。
     *
     * @param segmentId   段号
     * @param rows        行数据，每行长度等于列数，0 号元素是 rowId
     * @param columnTypes 每列的 H2 类型，见 {@code org.h2.value.TypeInfo}
     * @param compareMode 数据库的比较模式
     * @return 新段
     */
    public static ColumnSegment build(int segmentId, List<Value[]> rows, TypeInfo[] columnTypes,
            CompareMode compareMode) {
        int count = rows.size();
        int maxDict = Math.min(4096, Math.max(64, count / 4));
        ColumnVector[] vectors = new ColumnVector[columnTypes.length];
        boolean[] prebuilt = new boolean[columnTypes.length];
        for (int c = 0; c < columnTypes.length; c++) {
            TypeInfo columnType = columnTypes[c];
            int type = columnType.getValueType();
            if (type == Value.ARRAY) {
                ArrayVector arrayVec = ArrayVector.tryBuild(rows, c + 1, elementType(columnType));
                if (arrayVec != null) {
                    vectors[c] = arrayVec;
                    prebuilt[c] = true;
                    continue;
                }
            } else if (type == Value.CHAR || type == Value.VARCHAR) {
                DictVarcharVector dictVec = DictVarcharVector.tryBuild(rows, c + 1, maxDict);
                if (dictVec != null) {
                    vectors[c] = dictVec;
                    prebuilt[c] = true;
                    continue;
                }
            } else if (isNumericDictCandidate(type)) {
                DictValueVector dictVec = DictValueVector.tryBuild(rows, c + 1, maxDict);
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
            for (int c = 0; c < columnTypes.length; c++) {
                if (!prebuilt[c]) {
                    vectors[c].set(r, row[c + 1]);
                }
            }
        }
        ZoneMap[] zones = new ZoneMap[columnTypes.length];
        for (int c = 0; c < columnTypes.length; c++) {
            zones[c] = ZoneMap.of(vectors[c], compareMode);
        }
        return new ColumnSegment(segmentId, count, vectors, zones, compareMode);
    }

    /** 数组列的元素类型。不是 TypeInfo 或缺失时返回 null，让调用方退回 ValueVector。 */
    private static TypeInfo elementType(TypeInfo arrayType) {
        ExtTypeInfo ext = arrayType.getExtTypeInfo();
        return ext instanceof TypeInfo ? (TypeInfo) ext : null;
    }

    private static boolean isNumericDictCandidate(int type) {
        return type == Value.TINYINT
                || type == Value.SMALLINT
                || type == Value.INTEGER
                || type == Value.BIGINT
                || type == Value.REAL
                || type == Value.DOUBLE;
    }

    public int getSegmentId() {
        return segmentId;
    }

    public int getGeneration() {
        return generation;
    }

    public NullBitmap getDeleted() {
        return deleted;
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
        NullBitmap newDeleted;
        if (deletedOffsets == null || deletedOffsets.isEmpty()) {
            newDeleted = this.deleted;
        } else {
            newDeleted = this.deleted.copy();
            for (int off : deletedOffsets) {
                newDeleted.setNull(off);
            }
        }
        if (updates == null || updates.isEmpty()) {
            return new ColumnSegment(
                    segmentId, generation + 1, rowCount, columns, zoneMaps, newDeleted, compareMode);
        }

        ColumnVector[] newColumns = null;
        ZoneMap[] newZones = null;
        boolean[] needsRebuildZone = new boolean[columns.length];

        for (Map.Entry<Integer, Value[]> entry : updates) {
            int off = entry.getKey();
            Value[] values = entry.getValue();
            for (int c = 0; c < columns.length; c++) {
                ColumnVector currentVec = newColumns != null && newColumns[c] != null
                        ? newColumns[c]
                        : columns[c];
                Value oldVal = currentVec.get(off);
                Value newVal = values[c];
                if (oldVal == newVal
                        || (oldVal != ValueNull.INSTANCE
                                && newVal != ValueNull.INSTANCE
                                && oldVal.equals(newVal))) {
                    continue;
                }
                if (newColumns == null) {
                    newColumns = columns.clone();
                }
                if (newColumns[c] == columns[c]) {
                    newColumns[c] = columns[c].copy();
                }
                newColumns[c].set(off, newVal);

                if (!needsRebuildZone[c]) {
                    ZoneMap baseZm = newZones != null ? newZones[c] : zoneMaps[c];
                    ZoneMap updatedZm = baseZm.tryUpdate(oldVal, newVal);
                    if (updatedZm == null) {
                        needsRebuildZone[c] = true;
                    } else if (updatedZm != baseZm) {
                        if (newZones == null) {
                            newZones = zoneMaps.clone();
                        }
                        newZones[c] = updatedZm;
                    }
                }
            }
        }

        if (newColumns == null) {
            return new ColumnSegment(segmentId, generation + 1, rowCount, columns, zoneMaps,
                    newDeleted, compareMode);
        }
        for (int c = 0; c < columns.length; c++) {
            if (needsRebuildZone[c]) {
                if (newZones == null) {
                    newZones = zoneMaps.clone();
                }
                newZones[c] = ZoneMap.of(newColumns[c], compareMode);
            }
        }
        return new ColumnSegment(
                segmentId,
                generation + 1,
                rowCount,
                newColumns,
                newZones != null ? newZones : zoneMaps,
                newDeleted,
                compareMode);
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

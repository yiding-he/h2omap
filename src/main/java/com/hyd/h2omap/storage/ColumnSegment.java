/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.storage;

import java.util.List;

import org.h2.value.Value;

import com.hyd.h2omap.stats.ZoneMap;
import com.hyd.h2omap.vector.ColumnVector;

/**
 * 不可变列段。写满就换新段，合并时整段重写，旧的直接丢掉。
 *
 * <p>不变性是整个设计的前提：只有段不可变，列向量才能用紧凑的
 * primitive 数组，zone map 也才不会失效。
 */
public final class ColumnSegment {

    private final int segmentId;

    private final int rowCount;

    private final ColumnVector[] columns;

    private final ZoneMap[] zoneMaps;

    private ColumnSegment(int segmentId, int rowCount, ColumnVector[] columns, ZoneMap[] zoneMaps) {
        this.segmentId = segmentId;
        this.rowCount = rowCount;
        this.columns = columns;
        this.zoneMaps = zoneMaps;
    }

    /**
     * 由一批行构建一个段。调用方保证 {@code rows} 不为空。
     *
     * @param rows       行数据，每行长度等于列数
     * @param valueTypes 每列的 H2 类型码，见 {@code org.h2.value.Value}
     * @return 新段，段号取本批数据第一行的段号
     */
    public static ColumnSegment build(int segmentId, List<Value[]> rows, int[] valueTypes) {
        int count = rows.size();
        ColumnVector[] vectors = new ColumnVector[valueTypes.length];
        for (int c = 0; c < valueTypes.length; c++) {
            vectors[c] = ColumnVector.create(valueTypes[c], count);
        }
        for (int r = 0; r < count; r++) {
            Value[] row = rows.get(r);
            for (int c = 0; c < valueTypes.length; c++) {
                vectors[c].set(r, row[c + 1]);
            }
        }
        ZoneMap[] zones = new ZoneMap[valueTypes.length];
        for (int c = 0; c < valueTypes.length; c++) {
            zones[c] = ZoneMap.of(vectors[c]);
        }
        return new ColumnSegment(segmentId, count, vectors, zones);
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

    public long getEstimatedMemory() {
        long total = 64;
        for (ColumnVector vector : columns) {
            total += vector.getEstimatedMemory();
        }
        for (ZoneMap zoneMap : zoneMaps) {
            total += zoneMap.getEstimatedMemory();
        }
        return total;
    }
}

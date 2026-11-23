/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.storage;

import com.hyd.h2omap.stats.ZoneMap;
import com.hyd.h2omap.vector.ColumnVector;
import com.hyd.h2omap.vector.NullBitmap;

/**
 * 段文件的部分列读结果：只含请求列的向量与 zone map，外加行数与删除位图。
 *
 * <p>切片不进入驻留模型：不注册段引用、不计入驻留预算（SET OMAP_RESIDENT_BYTES）、不触发 LRU 触碰，
 * 只服务按需读路径。预加载与逐出读回都读整段，不经此类。
 */
public final class ColumnSlice {

    private final int segmentId;
    private final int generation;
    private final int rowCount;
    private final int deletedCount;
    private final NullBitmap deleted;
    private final int[] columnIds;
    private final ColumnVector[] vectors;
    private final ZoneMap[] zoneMaps;

    public ColumnSlice(int segmentId, int generation, int rowCount, int deletedCount,
            NullBitmap deleted, int[] columnIds, ColumnVector[] vectors, ZoneMap[] zoneMaps) {
        this.segmentId = segmentId;
        this.generation = generation;
        this.rowCount = rowCount;
        this.deletedCount = deletedCount;
        this.deleted = deleted;
        this.columnIds = columnIds.clone();
        this.vectors = vectors;
        this.zoneMaps = zoneMaps;
    }

    public int getSegmentId() {
        return segmentId;
    }

    public int getGeneration() {
        return generation;
    }

    public int getRowCount() {
        return rowCount;
    }

    public int getDeletedCount() {
        return deletedCount;
    }

    /** 切片驻留堆的估算字节数，供列切片缓存做预算管理。 */
    public long getEstimatedMemory() {
        long bytes = 64L;
        for (int i = 0; i < vectors.length; i++) {
            bytes += vectors[i].getEstimatedMemory() + zoneMaps[i].getEstimatedMemory();
        }
        return bytes + deleted.getEstimatedMemory();
    }

    public NullBitmap getDeletedBitmap() {
        return deleted;
    }

    public boolean isDeleted(int row) {
        return deleted.isNull(row);
    }

    /** 请求列集的副本，顺序与构造时一致。 */
    public int[] getColumnIds() {
        return columnIds.clone();
    }

    private int indexOf(int columnId) {
        for (int i = 0; i < columnIds.length; i++) {
            if (columnIds[i] == columnId) {
                return i;
            }
        }
        return -1;
    }

    public ColumnVector getVector(int columnId) {
        int i = indexOf(columnId);
        if (i < 0) {
            throw new IllegalArgumentException("Column not in slice: " + columnId);
        }
        return vectors[i];
    }

    public ZoneMap getZoneMap(int columnId) {
        int i = indexOf(columnId);
        if (i < 0) {
            throw new IllegalArgumentException("Column not in slice: " + columnId);
        }
        return zoneMaps[i];
    }
}

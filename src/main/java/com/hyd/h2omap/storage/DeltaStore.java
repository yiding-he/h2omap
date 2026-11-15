/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.storage;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.h2.value.Value;

/**
 * 行式写缓冲。列段不可变，所有点改先落在这里。
 *
 * <p>两块内容：
 * <ul>
 * <li>{@code updated}：已冻结段内的行被更新后的新像；
 * <li>{@code segmentDeletes}：已冻结段内的行被删除的 rowId。
 * </ul>
 *
 * <p>开放段内的点改直接改段缓冲，不经过这里。
 *
 * <p>不是线程安全的，调用方（{@link ColumnarStore}）负责加锁。
 */
public final class DeltaStore {

    private final Map<Long, Value[]> updated = new HashMap<>();

    private final Set<Long> segmentDeletes = new HashSet<>();

    private final Set<Integer> dirtySegments = new HashSet<>();

    /** 标记删除。 */
    public void markDeleted(long rowId) {
        updated.remove(rowId);
        segmentDeletes.add(rowId);
        dirtySegments.add(RowId.segmentId(rowId));
    }

    /** 记录已冻结段内行的新像。 */
    public void markUpdated(long rowId, Value[] values) {
        updated.put(rowId, values);
        dirtySegments.add(RowId.segmentId(rowId));
    }

    public boolean isEmpty() {
        return updated.isEmpty() && segmentDeletes.isEmpty();
    }

    public boolean hasSegmentDelta(int segmentId) {
        return !isEmpty() && dirtySegments.contains(segmentId);
    }

    public Value[] getUpdated(long rowId) {
        return updated.get(rowId);
    }

    public boolean isSegmentRowDeleted(long rowId) {
        return segmentDeletes.contains(rowId);
    }

    public int updatedSize() {
        return updated.size();
    }

    public int deletedSize() {
        return segmentDeletes.size();
    }

    public Set<Map.Entry<Long, Value[]>> updatedEntries() {
        return updated.entrySet();
    }

    public Map<Long, Value[]> rawUpdated() {
        return updated;
    }

    public Set<Long> deletedRowIds() {
        return segmentDeletes;
    }

    public void clear() {
        updated.clear();
        segmentDeletes.clear();
        dirtySegments.clear();
    }

    /** 仅当当前值引用与快照一致时移除（防止误删锁外构建期间的新更新）。 */
    public void removeUpdatedIfSame(long rowId, Value[] expected) {
        if (updated.get(rowId) == expected) {
            updated.remove(rowId);
        }
    }

    public void removeDeleted(long rowId) {
        segmentDeletes.remove(rowId);
    }

    /** 精确清理部分条目后重算脏段集合。 */
    public void rebuildDirtySegments() {
        dirtySegments.clear();
        for (long rowId : updated.keySet()) {
            dirtySegments.add(RowId.segmentId(rowId));
        }
        for (long rowId : segmentDeletes) {
            dirtySegments.add(RowId.segmentId(rowId));
        }
    }
}

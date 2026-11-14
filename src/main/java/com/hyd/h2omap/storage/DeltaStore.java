/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.storage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.h2.value.Value;

/**
 * 行式写缓冲。列段不可变，所有点改先落在这里。
 *
 * <p>三块内容：
 * <ul>
 * <li>{@code inserted}：新增行，追加写，槽位号就是 rowId 的低位；
 * <li>{@code updated}：段内行被更新后的新像；
 * <li>{@code segmentDeletes}：段内行被删除的 rowId。
 * </ul>
 *
 * <p>不是线程安全的，调用方（{@link ColumnarStore}）负责加锁。
 */
public final class DeltaStore {

    private final List<DeltaRow> inserted = new ArrayList<>();

    private final Map<Long, DeltaRow> updated = new HashMap<>();

    private final Set<Long> segmentDeletes = new HashSet<>();

    /**
     * 追加一行。
     *
     * @param values 整行的值
     * @return 新行分配的 rowId
     */
    public long append(Value[] values) {
        int slot = inserted.size();
        long rowId = RowId.ofDelta(slot);
        inserted.add(new DeltaRow(rowId, values));
        return rowId;
    }

    /**
     * 标记删除。delta 行直接置标志位，段内行记进集合。
     */
    public void markDeleted(long rowId) {
        if (RowId.isDelta(rowId)) {
            DeltaRow row = getInserted(RowId.slot(rowId));
            if (row != null) {
                row.setDeleted(true);
            }
            return;
        }
        updated.remove(rowId);
        segmentDeletes.add(rowId);
    }

    /**
     * 记录段内行的新像。行仍然由原来的段提供位置，值以这里为准。
     */
    public void markUpdated(long rowId, Value[] values) {
        updated.put(rowId, new DeltaRow(rowId, values));
    }

    public DeltaRow getInserted(int slot) {
        return slot >= 0 && slot < inserted.size() ? inserted.get(slot) : null;
    }

    public DeltaRow getUpdated(long rowId) {
        return updated.get(rowId);
    }

    public boolean isSegmentRowDeleted(long rowId) {
        return segmentDeletes.contains(rowId);
    }

    public Collection<DeltaRow> insertedRows() {
        return inserted;
    }

    /** 新增行数，不扣除已删除的。用于判断是否该合并。 */
    public int insertedSize() {
        return inserted.size();
    }

    public int updateSize() {
        return updated.size();
    }

    public void clear() {
        inserted.clear();
        updated.clear();
        segmentDeletes.clear();
    }
}

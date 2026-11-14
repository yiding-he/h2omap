/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.storage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.h2.result.Row;
import org.h2.table.Column;
import org.h2.value.Value;
import org.h2.value.ValueBigint;

import com.hyd.h2omap.engine.OmapOptions;

/**
 * 列式存储的核心容器：不可变列段加行式 delta。
 *
 * <p>新行写进当前开放段，写满即冻结成 {@link ColumnSegment}，之后开一个新段。
 * rowId 在分配时就是最终值：段号预先分配，段内偏移就是写入顺序，
 * 冻结不改变任何已有 rowId。
 *
 * <p>线程安全策略：单写多读。H2 的 {@code addRow} 和 {@code removeRow}
 * 没有锁契约，多个连接会并发进入，所以写锁是必须的。
 */
public final class ColumnarStore {

    private final int[] valueTypes;

    private final OmapOptions options;

    /** 已冻结的段，按段号升序。 */
    private final List<ColumnSegment> segments = new ArrayList<>();

    private final DeltaStore delta = new DeltaStore();

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private final AtomicLong dataModificationId = new AtomicLong();

    /** 当前开放段的行缓冲，每行 0 号元素是 rowId。 */
    private final SegmentBuilder openSegment;

    /** 下一个要分配的段号，单调递增，不回收。 */
    private int nextSegmentId;

    private final Set<Long> openDeletes = new HashSet<>();

    private long rowCount;

    public ColumnarStore(Column[] columns, OmapOptions options) {
        this.options = options;
        this.valueTypes = new int[columns.length];
        for (int i = 0; i < columns.length; i++) {
            valueTypes[i] = columns[i].getType().getValueType();
        }
        this.openSegment = new SegmentBuilder();
    }

    // ------------------------------------------------------------------
    // 写
    // ------------------------------------------------------------------

    public long append(Value[] values) {
        lock.writeLock().lock();
        try {
            long rowId = RowId.ofSegment(nextSegmentId, openSegment.rowCount());
            openSegment.append(withRowId(rowId, values));
            rowCount++;
            bump();
            if (openSegment.rowCount() >= options.getSegmentSize()) {
                freeze();
            }
            return rowId;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void remove(long rowId) {
        lock.writeLock().lock();
        try {
            if (isVisible(rowId)) {
                if (isFrozen(rowId)) {
                    delta.markDeleted(rowId);
                } else {
                    openDeletes.add(rowId);
                }
                rowCount--;
                bump();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 更新。行位置不变，值以 delta 里的新像为准。
     */
    public void update(long rowId, Value[] values) {
        lock.writeLock().lock();
        try {
            if (isFrozen(rowId)) {
                // 整体替换值数组，不就地改写：已交给 H2 的 Row 可能还引用旧数组。
                delta.markUpdated(rowId, values);
            } else {
                openSegment.setRow(RowId.offset(rowId), withRowId(rowId, values));
            }
            bump();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 清空所有数据。
     *
     * @return 清空前的行数
     */
    public long truncate() {
        lock.writeLock().lock();
        try {
            long old = rowCount;
            segments.clear();
            delta.clear();
            openSegment.clear();
            openDeletes.clear();
            rowCount = 0;
            // 段号不回收，清空后再分配的 rowId 不会和之前撞车。
            bump();
            return old;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 把 delta 合并进新的列段。
     *
     * <p>TODO M3 实现：合并会改变 rowId（段号与段内偏移都会变），
     * 必须配合读游标持有的段集合快照，旧快照引用计数归零后才能回收。
     */
    public long compact() {
        lock.writeLock().lock();
        try {
            return 0;
        } finally {
            lock.writeLock().unlock();
        }
    }

    // ------------------------------------------------------------------
    // 读
    // ------------------------------------------------------------------

    /**
     * 物化一行。返回的 {@code Row} 已经带上 rowId，
     * H2 后续的 UPDATE 和 DELETE 靠这个 key 找回原行。
     *
     * @param rowId 行标识
     * @return 行，不存在或已删除时返回 null
     */
    public Row materialize(long rowId) {
        lock.readLock().lock();
        try {
            Value[] values;
            if (isFrozen(rowId)) {
                if (delta.isSegmentRowDeleted(rowId)) {
                    return null;
                }
                Value[] updated = delta.getUpdated(rowId);
                if (updated != null) {
                    values = updated;
                } else {
                    ColumnSegment segment = segment(RowId.segmentId(rowId));
                    if (segment == null || RowId.offset(rowId) >= segment.getRowCount()) {
                        return null;
                    }
                    values = segment.getRow(RowId.offset(rowId));
                }
            } else {
                int offset = RowId.offset(rowId);
                if (offset >= openSegment.rowCount() || openDeletes.contains(rowId)) {
                    return null;
                }
                values = openSegment.rowValues(offset);
            }
            Row row = Row.get(values, 0);
            row.setKey(rowId);
            return row;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 收集所有可见行的 rowId。
     *
     * @param reverse 是否倒序
     * @return rowId 数组
     */
    public long[] collectVisibleRowIds(boolean reverse) {
        lock.readLock().lock();
        try {
            long[] result = new long[(int) Math.min(rowCount, Integer.MAX_VALUE)];
            int p = 0;
            if (!reverse) {
                for (ColumnSegment segment : segments) {
                    for (int offset = 0; offset < segment.getRowCount(); offset++) {
                        long rowId = RowId.ofSegment(segment.getSegmentId(), offset);
                        if (!delta.isSegmentRowDeleted(rowId)) {
                            result[p++] = rowId;
                        }
                    }
                }
                for (int offset = 0; offset < openSegment.rowCount(); offset++) {
                    long rowId = RowId.ofSegment(nextSegmentId, offset);
                    if (!openDeletes.contains(rowId)) {
                        result[p++] = rowId;
                    }
                }
            } else {
                for (int offset = openSegment.rowCount() - 1; offset >= 0; offset--) {
                    long rowId = RowId.ofSegment(nextSegmentId, offset);
                    if (!openDeletes.contains(rowId)) {
                        result[p++] = rowId;
                    }
                }
                for (int s = segments.size() - 1; s >= 0; s--) {
                    ColumnSegment segment = segments.get(s);
                    for (int offset = segment.getRowCount() - 1; offset >= 0; offset--) {
                        long rowId = RowId.ofSegment(segment.getSegmentId(), offset);
                        if (!delta.isSegmentRowDeleted(rowId)) {
                            result[p++] = rowId;
                        }
                    }
                }
            }
            return Arrays.copyOf(result, p);
        } finally {
            lock.readLock().unlock();
        }
    }

    public long getRowCount() {
        lock.readLock().lock();
        try {
            return rowCount;
        } finally {
            lock.readLock().unlock();
        }
    }

    public long getDataModificationId() {
        return dataModificationId.get();
    }

    public OmapOptions getOptions() {
        return options;
    }

    /** 供索引构建时读取列类型。 */
    public int[] getValueTypes() {
        return valueTypes;
    }

    /** 供测试与合并使用。 */
    List<ColumnSegment> segments() {
        return segments;
    }

    // ------------------------------------------------------------------
    // 内部
    // ------------------------------------------------------------------

    /** 把值数组前面加一个 rowId 元素，作为 builder 里的一行。 */
    private static Value[] withRowId(long rowId, Value[] values) {
        Value[] row = new Value[values.length + 1];
        row[0] = ValueBigint.get(rowId);
        System.arraycopy(values, 0, row, 1, values.length);
        return row;
    }

    /** 当前开放段的段号，也就是它冻结后拿到的段号。 */
    private void freeze() {
        if (openSegment.rowCount() == 0) {
            return;
        }
        ColumnSegment segment = ColumnSegment.build(
                nextSegmentId, openSegment.drain(), valueTypes);
        segments.add(segment);
        // 段冻结后，开放段里的删除记录必须移交给 delta，否则那些行会重新可见。
        for (long rowId : openDeletes) {
            delta.markDeleted(rowId);
        }
        openDeletes.clear();
        nextSegmentId++;
    }

    private boolean isVisible(long rowId) {
        return !isFrozen(rowId) || !delta.isSegmentRowDeleted(rowId);
    }

    private boolean isFrozen(long rowId) {
        return RowId.segmentId(rowId) < nextSegmentId;
    }

    private ColumnSegment segment(int segmentId) {
        int index = segmentId - firstSegmentId();
        return index >= 0 && index < segments.size() ? segments.get(index) : null;
    }

    private int firstSegmentId() {
        return segments.isEmpty() ? nextSegmentId : segments.get(0).getSegmentId();
    }

    private void bump() {
        dataModificationId.incrementAndGet();
    }
}

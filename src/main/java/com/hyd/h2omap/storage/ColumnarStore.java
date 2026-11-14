/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.storage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.h2.result.Row;
import org.h2.table.Column;
import org.h2.value.Value;

import com.hyd.h2omap.engine.OmapOptions;

/**
 * 列式存储的核心容器：不可变列段加行式 delta。
 *
 * <p>写入全部落到 {@link DeltaStore}，代价与列数无关。
 * 读取按段顺序拼 result，物化推迟到 {@link #materialize(long)}。
 *
 * <p>线程安全策略：单写多读。H2 的 {@code addRow} 和 {@code removeRow}
 * 没有锁契约，多个连接会并发进入，所以写锁是必须的。
 */
public final class ColumnarStore {

    private final int[] valueTypes;

    private final OmapOptions options;

    private final List<ColumnSegment> segments = new ArrayList<>();

    private final DeltaStore delta = new DeltaStore();

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private final AtomicLong dataModificationId = new AtomicLong();

    private long rowCount;

    private int nextSegmentId;

    public ColumnarStore(Column[] columns, OmapOptions options) {
        this.options = options;
        this.valueTypes = new int[columns.length];
        for (int i = 0; i < columns.length; i++) {
            valueTypes[i] = columns[i].getType().getValueType();
        }
    }

    // ------------------------------------------------------------------
    // 写
    // ------------------------------------------------------------------

    public long append(Value[] values) {
        lock.writeLock().lock();
        try {
            long rowId = delta.append(values);
            rowCount++;
            bump();
            maybeCompact();
            return rowId;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void remove(long rowId) {
        lock.writeLock().lock();
        try {
            if (isVisible(rowId)) {
                delta.markDeleted(rowId);
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
            if (RowId.isDelta(rowId)) {
                // 整体替换 DeltaRow，而不是就地改写值数组：
                // 游标交给 H2 的 Row 直接引用那个数组，就地改写会篡改正在使用的行。
                delta.replaceInserted(RowId.slot(rowId), values);
            } else {
                delta.markUpdated(rowId, values);
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
            rowCount = 0;
            bump();
            return old;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 把 delta 合并进新的列段。
     *
     * <p>TODO 实现步骤：
     * <ol>
     * <li>把受影响的旧段与 delta 的可见行按 rowId 顺序摊平；
     * <li>按 {@code options.getSegmentSize()} 切成新段；
     * <li>旧段整段丢弃，delta 清空，重建索引。
     * </ol>
     */
    public long compact() {
        lock.writeLock().lock();
        try {
            // TODO 未实现，目前是空操作。
            return 0;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void maybeCompact() {
        if (delta.insertedSize() >= options.getCompactionMinRows()) {
            // TODO 合并放到后台线程，避免阻塞写入。
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
            if (RowId.isDelta(rowId)) {
                DeltaRow row = delta.getInserted(RowId.slot(rowId));
                if (row == null || row.isDeleted()) {
                    return null;
                }
                values = row.getValues();
            } else {
                if (delta.isSegmentRowDeleted(rowId)) {
                    return null;
                }
                DeltaRow updated = delta.getUpdated(rowId);
                if (updated != null) {
                    values = updated.getValues();
                } else {
                    ColumnSegment segment = segment(RowId.segmentId(rowId));
                    if (segment == null) {
                        return null;
                    }
                    values = segment.getRow(RowId.offset(rowId));
                }
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
     * <p>第一版把 rowId 全物化成数组，实现最直接。
     * 后续要换成按段流式推进，避免大表扫描时多一次 long 数组。
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
                for (DeltaRow row : delta.insertedRows()) {
                    if (!row.isDeleted()) {
                        result[p++] = row.getRowId();
                    }
                }
            } else {
                List<DeltaRow> inserted = new ArrayList<>(delta.insertedRows());
                for (int i = inserted.size() - 1; i >= 0; i--) {
                    DeltaRow row = inserted.get(i);
                    if (!row.isDeleted()) {
                        result[p++] = row.getRowId();
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

    public DeltaStore getDelta() {
        return delta;
    }

    public OmapOptions getOptions() {
        return options;
    }

    /** 供索引构建时读取列类型。 */
    public int[] getValueTypes() {
        return valueTypes;
    }

    // ------------------------------------------------------------------
    // 内部
    // ------------------------------------------------------------------

    private boolean isVisible(long rowId) {
        if (RowId.isDelta(rowId)) {
            DeltaRow row = delta.getInserted(RowId.slot(rowId));
            return row != null && !row.isDeleted();
        }
        return !delta.isSegmentRowDeleted(rowId);
    }

    private ColumnSegment segment(int segmentId) {
        return segmentId >= 0 && segmentId < segments.size() ? segments.get(segmentId) : null;
    }

    private void bump() {
        dataModificationId.incrementAndGet();
    }

    /** 供合并与测试使用。 */
    List<ColumnSegment> segments() {
        return segments;
    }

    int nextSegmentId() {
        return nextSegmentId++;
    }
}

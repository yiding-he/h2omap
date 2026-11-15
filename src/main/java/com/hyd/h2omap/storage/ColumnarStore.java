/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.storage;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.h2.result.Row;
import org.h2.table.Column;
import org.h2.value.CompareMode;
import org.h2.value.Value;
import org.h2.value.ValueBigint;
import org.h2.value.ValueNull;

import com.hyd.h2omap.engine.OmapOptions;

/**
 * 列式存储的核心容器：列段加行式 delta。
 *
 * <p>新行写进当前开放段，写满即冻结成 {@link ColumnSegment}，之后开一个新段。
 * rowId 在分配时就是最终值：段号预先分配，段内偏移就是写入顺序，
 * 冻结与合并都不改变任何已有 rowId。
 *
 * <p>delta 保存已冻结段的更新与删除。delta 攒到 {@code compactionMinRows}
 * 就在写锁内合并回段：值写回列向量，删除写进段的删除位图，然后清空 delta。
 * 因为 rowId 不变，读路径不需要快照。
 *
 * <p>线程安全策略：单写多读。H2 的 {@code addRow} 和 {@code removeRow}
 * 没有锁契约，多个连接会并发进入，所以写锁是必须的。
 */
public final class ColumnarStore {

    private final int[] valueTypes;

    private final OmapOptions options;

    private final CompareMode compareMode;

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

    public ColumnarStore(Column[] columns, OmapOptions options, CompareMode compareMode) {
        this.options = options;
        this.compareMode = compareMode;
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
                if (isFrozen(rowId)) {
                    delta.markDeleted(rowId);
                } else {
                    openDeletes.add(rowId);
                }
                rowCount--;
                bump();
                maybeCompact();
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
            maybeCompact();
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
     * 把 delta 的改动写回已冻结的段，然后清空 delta。
     *
     * <p>段号与段内偏移都不变，已交给 H2 的 rowId 因此仍然有效。
     * 合并后重算被触及段的 zone map。
     *
     * @return 合并的改动行数
     */
    public long compact() {
        lock.writeLock().lock();
        try {
            if (delta.updatedSize() == 0 && delta.deletedSize() == 0) {
                return 0;
            }
            Map<Integer, List<Map.Entry<Integer, Value[]>>> updatesBySeg = new HashMap<>();
            Map<Integer, List<Integer>> deletesBySeg = new HashMap<>();
            Set<Integer> touchedIds = new HashSet<>();

            for (Map.Entry<Long, Value[]> entry : delta.updatedEntries()) {
                int segId = RowId.segmentId(entry.getKey());
                if (segment(segId) != null) {
                    updatesBySeg.computeIfAbsent(segId, k -> new ArrayList<>())
                            .add(new AbstractMap.SimpleImmutableEntry<>(
                                    RowId.offset(entry.getKey()), entry.getValue()));
                    touchedIds.add(segId);
                }
            }
            for (long rowId : delta.deletedRowIds()) {
                int segId = RowId.segmentId(rowId);
                if (segment(segId) != null) {
                    deletesBySeg.computeIfAbsent(segId, k -> new ArrayList<>())
                            .add(RowId.offset(rowId));
                    touchedIds.add(segId);
                }
            }

            long merged = delta.updatedSize() + delta.deletedSize();
            delta.clear();

            int firstId = firstSegmentId();
            for (int segId : touchedIds) {
                int index = segId - firstId;
                ColumnSegment oldSeg = segments.get(index);
                ColumnSegment newSeg = oldSeg.applyDelta(
                        deletesBySeg.get(segId), updatesBySeg.get(segId));
                segments.set(index, newSeg);
            }
            bump();
            return merged;
        } finally {
            lock.writeLock().unlock();
        }
    }

    // ------------------------------------------------------------------
    // 读
    // ------------------------------------------------------------------

    /**
     * 按段推进的可见行迭代器。每次 {@code next()} 自己取读锁，
     * 所以游标可以在锁外长期存活。
     *
     * <p>迭代器是创建时刻的快照：构造时固定最后一个段号与开放段行数。
     * 开放段在迭代期间被冻结时，迭代器改从段列表读它，rowId 不变；
     * 冻结之后新开的段不在本次快照内。
     */
    @FunctionalInterface
    public interface RowIterator {

        /** 下一个可见行的 rowId，结束时返回 -1。 */
        long next();

        /**
         * 批量读取并物化最多 {@code buffer.length} 行。
         *
         * @param store  存储实例
         * @param buffer 接收物化行的缓冲区
         * @return 实际写入的行数，0 表示迭代结束
         */
        default int nextBatch(ColumnarStore store, Row[] buffer) {
            int filled = 0;
            while (filled < buffer.length) {
                long rowId = next();
                if (rowId < 0) {
                    break;
                }
                Row row = store.materialize(rowId);
                if (row != null) {
                    buffer[filled++] = row;
                }
            }
            return filled;
        }
    }

    /**
     * 创建一个行迭代器。
     *
     * @param lower   每列的下界，null 元素表示无下界；null 表示不做剪枝
     * @param upper   每列的上界，null 元素表示无上界；null 表示不做剪枝
     * @param reverse 是否倒序
     * @return 迭代器
     */
    public RowIterator rowIterator(Value[] lower, Value[] upper, boolean reverse) {
        return new SegmentRowIterator(lower, upper, reverse);
    }

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
                int segId = RowId.segmentId(rowId);
                ColumnSegment segment = segment(segId);
                int offset = RowId.offset(rowId);
                if (segment == null || offset >= segment.getRowCount()
                        || segment.isDeleted(offset)) {
                    return null;
                }
                if (delta.hasSegmentDelta(segId)) {
                    if (delta.isSegmentRowDeleted(rowId)) {
                        return null;
                    }
                    Value[] updated = delta.getUpdated(rowId);
                    if (updated != null) {
                        Row row = Row.get(updated, 0);
                        row.setKey(rowId);
                        return row;
                    }
                }
                return new LazyOmapRow(segment, offset, rowId);
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
        return collectVisibleRowIds(null, null, reverse);
    }

    /**
     * 收集可见行的 rowId，用 zone map 跳过不可能命中边界的段。
     *
     * @param lower   每列的下界，null 元素表示无下界；null 表示不做剪枝
     * @param upper   每列的上界，null 元素表示无上界；null 表示不做剪枝
     * @param reverse 是否倒序
     * @return rowId 数组
     */
    public long[] collectVisibleRowIds(Value[] lower, Value[] upper, boolean reverse) {
        lock.readLock().lock();
        try {
            long[] result = new long[(int) Math.min(rowCount, Integer.MAX_VALUE)];
            int p = 0;
            RowIterator iterator = rowIterator(lower, upper, reverse);
            for (long rowId; (rowId = iterator.next()) >= 0; ) {
                result[p++] = rowId;
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

    private void maybeCompact() {
        if (delta.updatedSize() + delta.deletedSize() >= options.getCompactionMinRows()) {
            compact();
        }
    }

    /** 当前开放段的段号，也就是它冻结后拿到的段号。 */
    private void freeze() {
        if (openSegment.rowCount() == 0) {
            return;
        }
        ColumnSegment segment = ColumnSegment.build(
                nextSegmentId, openSegment.drain(), valueTypes, compareMode);
        segments.add(segment);
        // 段冻结后，开放段里的删除记录必须移交给 delta，否则那些行会重新可见。
        for (long rowId : openDeletes) {
            delta.markDeleted(rowId);
        }
        openDeletes.clear();
        nextSegmentId++;
    }

    private boolean isVisible(long rowId) {
        if (!isFrozen(rowId)) {
            return !openDeletes.contains(rowId);
        }
        if (delta.isSegmentRowDeleted(rowId)) {
            return false;
        }
        ColumnSegment segment = segment(RowId.segmentId(rowId));
        return segment != null && !segment.isDeleted(RowId.offset(rowId));
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

    private final class SegmentRowIterator implements RowIterator {

        private final Value[] lower;

        private final Value[] upper;

        private final boolean reverse;

        /** 创建时的开放段段号，迭代期间不变。 */
        private final int lastSegmentId;

        /** 创建时开放段的行数，用来把快照限制在创建时刻已存在的行。 */
        private final int openRowCount;

        private int segmentId;

        private int offset;

        /** 当前段是否已经定位到起始偏移，倒序时用于区分新段与已走完的段。 */
        private boolean started;

        SegmentRowIterator(Value[] lower, Value[] upper, boolean reverse) {
            this.lower = lower;
            this.upper = upper;
            this.reverse = reverse;
            this.lastSegmentId = nextSegmentId;
            this.openRowCount = openSegment.rowCount();
            this.segmentId = reverse ? lastSegmentId : firstSegmentId();
            this.offset = reverse ? -1 : 0;
        }

        @Override
        public long next() {
            lock.readLock().lock();
            try {
                return reverse ? nextReverse() : nextForward();
            } finally {
                lock.readLock().unlock();
            }
        }

        @Override
        public int nextBatch(ColumnarStore store, Row[] buffer) {
            lock.readLock().lock();
            try {
                return reverse ? nextBatchReverse(buffer) : nextBatchForward(buffer);
            } finally {
                lock.readLock().unlock();
            }
        }

        private int nextBatchForward(Row[] buffer) {
            int filled = 0;
            while (filled < buffer.length && segmentId <= lastSegmentId) {
                boolean open = isOpen();
                ColumnSegment segment = open ? null : segment(segmentId);
                if (segment == null && !open) {
                    nextSegmentForward();
                    continue;
                }
                if (segment != null && !intersects(segment)) {
                    nextSegmentForward();
                    continue;
                }
                int count = countOf(open, segment);
                if (open) {
                    boolean noDeletes = openDeletes.isEmpty();
                    while (offset < count && filled < buffer.length) {
                        int currentOffset = offset++;
                        long rowId = RowId.ofSegment(segmentId, currentOffset);
                        if (noDeletes || !openDeletes.contains(rowId)) {
                            Row row = Row.get(openSegment.rowValues(currentOffset), 0);
                            row.setKey(rowId);
                            buffer[filled++] = row;
                        }
                    }
                } else {
                    boolean hasDelta = delta.hasSegmentDelta(segmentId);
                    while (offset < count && filled < buffer.length) {
                        int currentOffset = offset++;
                        if (segment.isDeleted(currentOffset)) {
                            continue;
                        }
                        long rowId = RowId.ofSegment(segmentId, currentOffset);
                        if (hasDelta) {
                            if (delta.isSegmentRowDeleted(rowId)) {
                                continue;
                            }
                            Value[] updated = delta.getUpdated(rowId);
                            if (updated != null) {
                                Row row = Row.get(updated, 0);
                                row.setKey(rowId);
                                buffer[filled++] = row;
                                continue;
                            }
                        }
                        buffer[filled++] = new LazyOmapRow(segment, currentOffset, rowId);
                    }
                }
                if (offset >= count) {
                    nextSegmentForward();
                }
            }
            return filled;
        }

        private int nextBatchReverse(Row[] buffer) {
            int filled = 0;
            int minSegmentId = firstSegmentId();
            while (filled < buffer.length && segmentId >= minSegmentId) {
                boolean open = isOpen();
                ColumnSegment segment = open ? null : segment(segmentId);
                if (segment == null && !open) {
                    nextSegmentReverse();
                    continue;
                }
                if (segment != null && !intersects(segment)) {
                    nextSegmentReverse();
                    continue;
                }
                int count = countOf(open, segment);
                if (!started) {
                    offset = count - 1;
                    started = true;
                }
                if (open) {
                    boolean noDeletes = openDeletes.isEmpty();
                    while (offset >= 0 && filled < buffer.length) {
                        int currentOffset = offset--;
                        long rowId = RowId.ofSegment(segmentId, currentOffset);
                        if (noDeletes || !openDeletes.contains(rowId)) {
                            Row row = Row.get(openSegment.rowValues(currentOffset), 0);
                            row.setKey(rowId);
                            buffer[filled++] = row;
                        }
                    }
                } else {
                    boolean hasDelta = delta.hasSegmentDelta(segmentId);
                    while (offset >= 0 && filled < buffer.length) {
                        int currentOffset = offset--;
                        if (segment.isDeleted(currentOffset)) {
                            continue;
                        }
                        long rowId = RowId.ofSegment(segmentId, currentOffset);
                        if (hasDelta) {
                            if (delta.isSegmentRowDeleted(rowId)) {
                                continue;
                            }
                            Value[] updated = delta.getUpdated(rowId);
                            if (updated != null) {
                                Row row = Row.get(updated, 0);
                                row.setKey(rowId);
                                buffer[filled++] = row;
                                continue;
                            }
                        }
                        buffer[filled++] = new LazyOmapRow(segment, currentOffset, rowId);
                    }
                }
                if (offset < 0) {
                    nextSegmentReverse();
                }
            }
            return filled;
        }

        private long nextForward() {
            while (segmentId <= lastSegmentId) {
                boolean open = isOpen();
                ColumnSegment segment = open ? null : segment(segmentId);
                if (segment == null && !open) {
                    nextSegmentForward();
                    continue;
                }
                if (segment != null && !intersects(segment)) {
                    nextSegmentForward();
                    continue;
                }
                int count = countOf(open, segment);
                while (offset < count) {
                    long rowId = RowId.ofSegment(segmentId, offset);
                    boolean deleted = open ? openDeletes.contains(rowId) : segment.isDeleted(offset);
                    offset++;
                    if (!deleted) {
                        return rowId;
                    }
                }
                nextSegmentForward();
            }
            return -1;
        }

        private long nextReverse() {
            while (segmentId >= firstSegmentId()) {
                boolean open = isOpen();
                ColumnSegment segment = open ? null : segment(segmentId);
                if (segment == null && !open) {
                    nextSegmentReverse();
                    continue;
                }
                if (segment != null && !intersects(segment)) {
                    nextSegmentReverse();
                    continue;
                }
                int count = countOf(open, segment);
                if (!started) {
                    offset = count - 1;
                    started = true;
                }
                while (offset >= 0) {
                    long rowId = RowId.ofSegment(segmentId, offset);
                    boolean deleted = open ? openDeletes.contains(rowId) : segment.isDeleted(offset);
                    offset--;
                    if (!deleted) {
                        return rowId;
                    }
                }
                nextSegmentReverse();
            }
            return -1;
        }

        private void nextSegmentForward() {
            segmentId++;
            offset = 0;
        }

        private void nextSegmentReverse() {
            segmentId--;
            offset = -1;
            started = false;
        }

        /** 当前段号是否仍是未冻结的开放段。 */
        private boolean isOpen() {
            return segmentId == lastSegmentId && segmentId == nextSegmentId;
        }

        /**
         * 当前段要遍历的行数。创建时的开放段可能被写满并冻结，
         * 新追加的行不属于本次快照。
         */
        private int countOf(boolean open, ColumnSegment segment) {
            int available = open ? openSegment.rowCount() : segment.getRowCount();
            return segmentId == lastSegmentId ? Math.min(available, openRowCount) : available;
        }

        private boolean intersects(ColumnSegment segment) {
            for (int c = 0; c < valueTypes.length; c++) {
                Value low = bound(lower, c);
                Value high = bound(upper, c);
                if ((low != null || high != null) && !segment.getZoneMap(c).mayIntersect(low, high)) {
                    return false;
                }
            }
            return true;
        }

        private Value bound(Value[] bounds, int column) {
            if (bounds == null || column >= bounds.length) {
                return null;
            }
            Value v = bounds[column];
            return v == null || v == ValueNull.INSTANCE ? null : v;
        }
    }
}

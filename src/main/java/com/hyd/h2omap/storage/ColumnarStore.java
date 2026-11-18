/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.storage;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
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
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueBigint;
import org.h2.value.ValueNull;

import com.hyd.h2omap.engine.OmapOptions;
import com.hyd.h2omap.persistence.OmapPersistence;

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

    /** 每列的完整类型，建段时用它识别数组列的元素类型。 */
    private final TypeInfo[] columnTypes;

    private final OmapOptions options;

    private final CompareMode compareMode;

    /** 已冻结的段，按段号升序。 */
    private final List<ColumnSegment> segments = new ArrayList<>();

    private final DeltaStore delta = new DeltaStore();

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private final Object compactMonitor = new Object();

    private final AtomicLong dataModificationId = new AtomicLong();

    /** 当前开放段的行缓冲，每行 0 号元素是 rowId。 */
    private final SegmentBuilder openSegment;

    /** 下一个要分配的段号，单调递增，不回收。 */
    private int nextSegmentId;

    private final Set<Long> openDeletes = new HashSet<>();

    private long rowCount;

    private final AtomicLong prunedSegments = new AtomicLong();

    private final AtomicLong orderedSkippedSegments = new AtomicLong();

    private volatile OmapPersistence persistence;

    public ColumnarStore(Column[] columns, OmapOptions options, CompareMode compareMode) {
        this.options = options;
        this.compareMode = compareMode;
        this.valueTypes = new int[columns.length];
        this.columnTypes = new TypeInfo[columns.length];
        for (int i = 0; i < columns.length; i++) {
            columnTypes[i] = columns[i].getType();
            valueTypes[i] = columnTypes[i].getValueType();
        }
        this.openSegment = new SegmentBuilder();
    }

    public void setPersistence(OmapPersistence persistence) {
        this.persistence = persistence;
    }

    // ------------------------------------------------------------------
    // 写
    // ------------------------------------------------------------------

    public long append(Value[] values) {
        long rowId;
        boolean needCompact;
        boolean froze = false;
        lock.writeLock().lock();
        try {
            rowId = RowId.ofSegment(nextSegmentId, openSegment.rowCount());
            openSegment.append(withRowId(rowId, values));
            rowCount++;
            bump();
            OmapPersistence p = persistence;
            if (p != null) {
                p.logInsert(rowId, values);
            }
            if (openSegment.rowCount() >= options.getSegmentSize()) {
                freeze();
                froze = true;
            }
            needCompact = shouldCompact();
        } finally {
            lock.writeLock().unlock();
        }
        if (needCompact) {
            compact();
        } else if (froze && persistence != null) {
            checkpoint();
        }
        return rowId;
    }

    public void remove(long rowId) {
        boolean needCompact = false;
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
                OmapPersistence p = persistence;
                if (p != null) {
                    p.logDelete(rowId);
                }
                needCompact = shouldCompact();
            }
        } finally {
            lock.writeLock().unlock();
        }
        if (needCompact) {
            compact();
        }
    }

    /**
     * 更新。行位置不变，值以 delta 里的新像为准。
     */
    public void update(long rowId, Value[] values) {
        boolean needCompact = false;
        lock.writeLock().lock();
        try {
            if (isFrozen(rowId)) {
                // 整体替换值数组，不就地改写：已交给 H2 的 Row 可能还引用旧数组。
                delta.markUpdated(rowId, values);
            } else {
                openSegment.setRow(RowId.offset(rowId), withRowId(rowId, values));
            }
            bump();
            OmapPersistence p = persistence;
            if (p != null) {
                p.logUpdate(rowId, values);
            }
            needCompact = shouldCompact();
        } finally {
            lock.writeLock().unlock();
        }
        if (needCompact) {
            compact();
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
            OmapPersistence p = persistence;
            if (p != null) {
                p.logTruncate(nextSegmentId);
            }
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
        long compactedCount;
        synchronized (compactMonitor) {
            Map<Integer, ColumnSegment> baseSegments = new HashMap<>();
            Map<Integer, List<Map.Entry<Integer, Value[]>>> updatesBySeg = new HashMap<>();
            Map<Integer, List<Integer>> deletesBySeg = new HashMap<>();
            List<Map.Entry<Long, Value[]>> updateSnap = new ArrayList<>();
            List<Long> deleteSnap = new ArrayList<>();

            lock.writeLock().lock();
            try {
                if (delta.updatedSize() == 0 && delta.deletedSize() == 0) {
                    return 0;
                }
                for (Map.Entry<Long, Value[]> entry : delta.updatedEntries()) {
                    long rowId = entry.getKey();
                    int segId = RowId.segmentId(rowId);
                    ColumnSegment seg = segment(segId);
                    if (seg != null) {
                        baseSegments.putIfAbsent(segId, seg);
                        updatesBySeg.computeIfAbsent(segId, k -> new ArrayList<>())
                                .add(new AbstractMap.SimpleImmutableEntry<>(
                                        RowId.offset(rowId), entry.getValue()));
                        updateSnap.add(new AbstractMap.SimpleImmutableEntry<>(
                                rowId, entry.getValue()));
                    }
                }
                for (long rowId : delta.deletedRowIds()) {
                    int segId = RowId.segmentId(rowId);
                    ColumnSegment seg = segment(segId);
                    if (seg != null) {
                        baseSegments.putIfAbsent(segId, seg);
                        deletesBySeg.computeIfAbsent(segId, k -> new ArrayList<>())
                                .add(RowId.offset(rowId));
                        deleteSnap.add(rowId);
                    }
                }
            } finally {
                lock.writeLock().unlock();
            }

            if (baseSegments.isEmpty()) {
                return 0;
            }

            Map<Integer, ColumnSegment> builtSegments = new HashMap<>(baseSegments.size());
            for (Map.Entry<Integer, ColumnSegment> entry : baseSegments.entrySet()) {
                int segId = entry.getKey();
                ColumnSegment newSeg = entry.getValue().applyDelta(
                        deletesBySeg.get(segId), updatesBySeg.get(segId));
                builtSegments.put(segId, newSeg);
            }

            lock.writeLock().lock();
            try {
                int firstId = firstSegmentId();
                for (Map.Entry<Integer, ColumnSegment> entry : builtSegments.entrySet()) {
                    int segId = entry.getKey();
                    int index = segId - firstId;
                    if (index >= 0 && index < segments.size()
                            && segments.get(index) == baseSegments.get(segId)) {
                        segments.set(index, entry.getValue());
                    } else {
                        return 0;
                    }
                }
                for (Map.Entry<Long, Value[]> entry : updateSnap) {
                    delta.removeUpdatedIfSame(entry.getKey(), entry.getValue());
                }
                for (long rowId : deleteSnap) {
                    delta.removeDeleted(rowId);
                }
                delta.rebuildDirtySegments();
                bump();
                compactedCount = (long) updateSnap.size() + deleteSnap.size();
            } finally {
                lock.writeLock().unlock();
            }
        }
        if (persistence != null && compactedCount >= Math.max(1024, options.getCompactionMinRows())) {
            checkpoint();
        }
        return compactedCount;
    }

    /**
     * 执行一次增量段快照 Checkpoint（将未写盘的新/改段及当前开放段/delta 写盘，并截断旧 WAL）。
     */
    public void checkpoint() {
        OmapPersistence p = persistence;
        if (p == null) {
            return;
        }
        List<ColumnSegment> segsSnap;
        int nextSegIdSnap;
        long newWalSeq;
        List<Value[]> openRowsSnap;
        Set<Long> openDeletesSnap;
        Map<Long, Value[]> deltaUpdatedSnap;
        Set<Long> deltaDeletesSnap;
        lock.writeLock().lock();
        try {
            segsSnap = new ArrayList<>(segments);
            nextSegIdSnap = nextSegmentId;
            openRowsSnap = new ArrayList<>(openSegment.rawRows());
            openDeletesSnap = new HashSet<>(openDeletes);
            deltaUpdatedSnap = new HashMap<>(delta.rawUpdated());
            deltaDeletesSnap = new HashSet<>(delta.deletedRowIds());
            newWalSeq = p.rotateWal();
        } finally {
            lock.writeLock().unlock();
        }
        p.writeCheckpoint(segsSnap, nextSegIdSnap, newWalSeq,
                openRowsSnap, openDeletesSnap, deltaUpdatedSnap, deltaDeletesSnap);
    }

    // ------------------------------------------------------------------
    // 持久化恢复（仅在开库阶段单线程调用）
    // ------------------------------------------------------------------

    public void restoreFromPersistence(List<ColumnSegment> restoredSegments, int restoredNextSegId,
            List<Value[]> openRows, Set<Long> restoredOpenDeletes,
            Map<Long, Value[]> restoredDeltaUpdated, Set<Long> restoredDeltaDeletes) {
        lock.writeLock().lock();
        try {
            segments.clear();
            segments.addAll(restoredSegments);
            nextSegmentId = restoredNextSegId;
            openSegment.clear();
            for (Value[] row : openRows) {
                openSegment.append(row);
            }
            openDeletes.clear();
            openDeletes.addAll(restoredOpenDeletes);
            delta.clear();
            for (Map.Entry<Long, Value[]> e : restoredDeltaUpdated.entrySet()) {
                delta.markUpdated(e.getKey(), e.getValue());
            }
            for (long rowId : restoredDeltaDeletes) {
                delta.markDeleted(rowId);
            }
            recomputeRowCount();
            bump();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void replayInsert(long rowId, Value[] values) {
        lock.writeLock().lock();
        try {
            int segId = RowId.segmentId(rowId);
            while (nextSegmentId < segId) {
                nextSegmentId = segId;
            }
            openSegment.append(withRowId(rowId, values));
            rowCount++;
            bump();
            if (openSegment.rowCount() >= options.getSegmentSize()) {
                freeze();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void replayUpdate(long rowId, Value[] values) {
        lock.writeLock().lock();
        try {
            if (isFrozen(rowId)) {
                delta.markUpdated(rowId, values);
            } else {
                openSegment.setRow(RowId.offset(rowId), withRowId(rowId, values));
            }
            bump();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void replayDelete(long rowId) {
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

    public void replayTruncate(int restoredNextSegId) {
        lock.writeLock().lock();
        try {
            segments.clear();
            delta.clear();
            openSegment.clear();
            openDeletes.clear();
            nextSegmentId = restoredNextSegId;
            rowCount = 0;
            bump();
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void recomputeRowCount() {
        long count = 0;
        for (ColumnSegment seg : segments) {
            int rc = seg.getRowCount();
            if (seg.getDeleted().hasNull()) {
                for (int i = 0; i < rc; i++) {
                    if (!seg.isDeleted(i)) {
                        count++;
                    }
                }
            } else {
                count += rc;
            }
        }
        for (long rowId : delta.deletedRowIds()) {
            ColumnSegment seg = segment(RowId.segmentId(rowId));
            if (seg != null && !seg.isDeleted(RowId.offset(rowId))) {
                count--;
            }
        }
        count += openSegment.rowCount() - openDeletes.size();
        this.rowCount = Math.max(0, count);
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
            // rowCount 是估计值，恢复后的 delta 可能让它与迭代器可见行数不一致，
            // 所以按估计值起步并在超出时扩容，不能假定两者相等。
            int initial = (int) Math.min(Math.max(rowCount, 16), 1 << 24);
            long[] result = new long[initial];
            int p = 0;
            RowIterator iterator = rowIterator(lower, upper, reverse);
            for (long rowId; (rowId = iterator.next()) >= 0; ) {
                if (p == result.length) {
                    result = Arrays.copyOf(result, (int) Math.min((long) result.length * 2, Integer.MAX_VALUE));
                }
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

    private boolean shouldCompact() {
        return delta.updatedSize() + delta.deletedSize() >= options.getCompactionMinRows();
    }

    /** 当前开放段的段号，也就是它冻结后拿到的段号。 */
    private void freeze() {
        if (openSegment.rowCount() == 0) {
            return;
        }
        ColumnSegment segment = ColumnSegment.build(
                nextSegmentId, openSegment.drain(), columnTypes, compareMode);
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
                boolean hasDelta = !open && delta.hasSegmentDelta(segmentId);
                while (offset < count) {
                    long rowId = RowId.ofSegment(segmentId, offset);
                    boolean deleted = open ? openDeletes.contains(rowId)
                            : segment.isDeleted(offset) || (hasDelta && delta.isSegmentRowDeleted(rowId));
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
                    boolean deleted = open ? openDeletes.contains(rowId)
                            : segment.isDeleted(offset)
                                    || (delta.hasSegmentDelta(segmentId) && delta.isSegmentRowDeleted(rowId));
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
            return ColumnarStore.intersects(segment, lower, upper, valueTypes.length);
        }
    }

    /**
     * 向量化查询的一次性一致性快照：在极短读锁内截取不可变段数组、
     * 被 delta 屏蔽的段内偏移以及 delta/开放段活跃行，之后锁外无锁计算。
     */
    public static final class VectorizedSnapshot {

        public final ColumnSegment[] segments;
        public final Map<Integer, BitSet> maskedBySegment;
        public final List<Value[]> extraRows;

        VectorizedSnapshot(ColumnSegment[] segments,
                Map<Integer, BitSet> maskedBySegment, List<Value[]> extraRows) {
            this.segments = segments;
            this.maskedBySegment = maskedBySegment;
            this.extraRows = extraRows;
        }
    }

    public VectorizedSnapshot takeVectorizedSnapshot() {
        return takeVectorizedSnapshot(null, null);
    }

    /**
     * 向量化查询的一次性一致性快照，并按列值区间跳过不可能命中的段。
     *
     * @param lower 每列下界，null 元素表示该列无下界；null 表示不剪枝
     * @param upper 每列上界，null 元素表示该列无上界；null 表示不剪枝
     * @return 快照，{@code segments} 只含可能与区间相交的段
     */
    public VectorizedSnapshot takeVectorizedSnapshot(Value[] lower, Value[] upper) {
        lock.readLock().lock();
        try {
            ColumnSegment[] segs = segments.toArray(new ColumnSegment[0]);
            if (lower != null || upper != null) {
                segs = prune(segs, lower, upper);
            }
            Map<Integer, BitSet> masked = null;
            for (ColumnSegment seg : segs) {
                if (seg.getDeleted().hasNull()) {
                    if (masked == null) {
                        masked = new HashMap<>();
                    }
                    BitSet bs = masked.computeIfAbsent(seg.getSegmentId(), k -> new BitSet());
                    int rc = seg.getRowCount();
                    for (int r = 0; r < rc; r++) {
                        if (seg.isDeleted(r)) {
                            bs.set(r);
                        }
                    }
                }
            }
            List<Value[]> extra = null;
            if (!delta.isEmpty()) {
                if (masked == null) {
                    masked = new HashMap<>();
                }
                for (long rowId : delta.deletedRowIds()) {
                    masked.computeIfAbsent(RowId.segmentId(rowId), k -> new BitSet())
                            .set(RowId.offset(rowId));
                }
                extra = new ArrayList<>(delta.updatedSize() + openSegment.rowCount());
                for (Map.Entry<Long, Value[]> entry : delta.updatedEntries()) {
                    long rowId = entry.getKey();
                    masked.computeIfAbsent(RowId.segmentId(rowId), k -> new BitSet())
                            .set(RowId.offset(rowId));
                    extra.add(entry.getValue());
                }
            }
            int openCount = openSegment.rowCount();
            if (openCount > 0) {
                if (extra == null) {
                    extra = new ArrayList<>(openCount);
                }
                if (openDeletes.isEmpty()) {
                    for (int r = 0; r < openCount; r++) {
                        extra.add(openSegment.rowValues(r));
                    }
                } else {
                    for (int r = 0; r < openCount; r++) {
                        if (!openDeletes.contains(RowId.ofSegment(nextSegmentId, r))) {
                            extra.add(openSegment.rowValues(r));
                        }
                    }
                }
            }
            return new VectorizedSnapshot(
                    segs, masked != null ? masked : Map.of(), extra != null ? extra : List.of());
        } finally {
            lock.readLock().unlock();
        }
    }

    private ColumnSegment[] prune(ColumnSegment[] segs, Value[] lower, Value[] upper) {
        int kept = 0;
        ColumnSegment[] result = new ColumnSegment[segs.length];
        for (ColumnSegment segment : segs) {
            if (intersects(segment, lower, upper, valueTypes.length)) {
                result[kept++] = segment;
            }
        }
        prunedSegments.addAndGet(segs.length - kept);
        return kept == segs.length ? segs : Arrays.copyOf(result, kept);
    }

    /**
     * 段是否可能与给定区间相交。区间为 null 的一侧视为无界。
     *
     * <p>行迭代器与向量化快照共用这一份判定，避免两套实现漂移。
     */
    private static boolean intersects(ColumnSegment segment, Value[] lower, Value[] upper,
            int columnCount) {
        for (int c = 0; c < columnCount; c++) {
            Value low = bound(lower, c);
            Value high = bound(upper, c);
            if ((low != null || high != null) && !segment.getZoneMap(c).mayIntersect(low, high)) {
                return false;
            }
        }
        return true;
    }

    private static Value bound(Value[] bounds, int column) {
        if (bounds == null || column >= bounds.length) {
            return null;
        }
        Value v = bounds[column];
        return v == null || v == ValueNull.INSTANCE ? null : v;
    }

    /** 被段级 zone map 剪掉的段数累计值，供测试断言剪枝确实生效。 */
    public long getPrunedSegmentCount() {
        return prunedSegments.get();
    }

    /** 有序扫描中因段内最好一行不优于第 N 名而整段跳过的段数，供测试断言早停生效。 */
    public long getOrderedSkippedSegmentCount() {
        return orderedSkippedSegments.get();
    }

    /** 记一次有序扫描的整段跳过。 */
    public void recordOrderedSkip() {
        orderedSkippedSegments.incrementAndGet();
    }
}

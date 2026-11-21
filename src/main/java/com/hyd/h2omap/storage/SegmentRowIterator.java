/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.storage;

import org.h2.result.Row;
import org.h2.value.Value;

/**
 * 按段推进的可见行迭代器。每次 {@code next()} 自己取读锁，
 * 所以游标可以在锁外长期存活。
 *
 * <p>迭代器是创建时刻的快照：构造时固定最后一个段号与开放段行数。
 * 开放段在迭代期间被冻结时，迭代器改从段列表读它，rowId 不变；
 * 冻结之后新开的段不在本次快照内。
 *
 * <p>本类读取存储的包级状态（段表、开放段、delta、锁）：{@code next()} 自行取读锁，
 * 批量方法在调用方的读锁内推进，锁语义与拆出前一致。
 */
final class SegmentRowIterator implements ColumnarStore.RowIterator {

    private final ColumnarStore store;


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

    private ColumnSegment currentLoadedSegment;

    private ColumnarStore.SegmentRef pendingLoadRef;

    SegmentRowIterator(ColumnarStore store, Value[] lower, Value[] upper, boolean reverse) {
            this.store = store;
        this.lower = lower;
        this.upper = upper;
        this.reverse = reverse;
        this.lastSegmentId = store.nextSegmentId;
        this.openRowCount = store.openSegment.rowCount();
        this.segmentId = reverse ? lastSegmentId : store.firstSegmentId();
        this.offset = reverse ? -1 : 0;
    }

    @Override
    public long next() {
        store.lock.readLock().lock();
        try {
            return reverse ? nextReverse() : nextForward();
        } finally {
            store.lock.readLock().unlock();
        }
    }

    @Override
    public int nextBatch(ColumnarStore store, Row[] buffer) {
        int filled = 0;
        while (filled < buffer.length) {
            pendingLoadRef = null;
            store.lock.readLock().lock();
            try {
                filled = reverse
                        ? nextBatchReverse(buffer, filled)
                        : nextBatchForward(buffer, filled);
            } finally {
                store.lock.readLock().unlock();
            }
            if (pendingLoadRef == null) {
                break;
            }
            // 按段按需读回当前相交的逐出段；预算连单段都容不下时复用返回的段实例直至本段扫完。
            currentLoadedSegment = store.ensureResident(pendingLoadRef);
            if (currentLoadedSegment == null) {
                currentLoadedSegment = store.readSegmentTransient(pendingLoadRef);
            }
            if (currentLoadedSegment == null) {
                throw new IllegalStateException(
                        "逐出段读回失败: seg_" + pendingLoadRef.segmentId
                                + "_" + pendingLoadRef.generation);
            }
        }
        return filled;
    }

    private int nextBatchForward(Row[] buffer, int filled) {
        while (filled < buffer.length && segmentId <= lastSegmentId) {
            boolean open = isOpen();
            ColumnarStore.SegmentRef ref = open ? null : store.refOf(segmentId);
            if (!open && (ref == null || (ref.loaded == null && ref.skeleton == null))) {
                nextSegmentForward();
                continue;
            }
            if (!open && !intersects(ref)) {
                nextSegmentForward();
                continue;
            }
            ColumnSegment segment = null;
            if (!open) {
                segment = ref.loaded;
                if (segment == null) {
                    if (currentLoadedSegment != null
                            && currentLoadedSegment.getSegmentId() == segmentId) {
                        segment = currentLoadedSegment;
                    } else {
                        pendingLoadRef = ref;
                        return filled;
                    }
                }
            }
            int count = countOf(open, ref);
            if (!open) {
                store.touchRef(ref);
            }
            if (open) {
                boolean noDeletes = store.openDeletes.isEmpty();
                while (offset < count && filled < buffer.length) {
                    int currentOffset = offset++;
                    long rowId = RowId.ofSegment(segmentId, currentOffset);
                    if (noDeletes || !store.openDeletes.contains(rowId)) {
                        Row row = Row.get(store.openSegment.rowValues(currentOffset), 0);
                        row.setKey(rowId);
                        buffer[filled++] = row;
                    }
                }
            } else {
                boolean hasDelta = store.delta.hasSegmentDelta(segmentId);
                while (offset < count && filled < buffer.length) {
                    int currentOffset = offset++;
                    if (segment.isDeleted(currentOffset)) {
                        continue;
                    }
                    long rowId = RowId.ofSegment(segmentId, currentOffset);
                    if (hasDelta) {
                        if (store.delta.isSegmentRowDeleted(rowId)) {
                            continue;
                        }
                        Value[] updated = store.delta.getUpdated(rowId);
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

    private int nextBatchReverse(Row[] buffer, int filled) {
        int minSegmentId = store.firstSegmentId();
        while (filled < buffer.length && segmentId >= minSegmentId) {
            boolean open = isOpen();
            ColumnarStore.SegmentRef ref = open ? null : store.refOf(segmentId);
            if (!open && (ref == null || (ref.loaded == null && ref.skeleton == null))) {
                nextSegmentReverse();
                continue;
            }
            if (!open && !intersects(ref)) {
                nextSegmentReverse();
                continue;
            }
            ColumnSegment segment = null;
            if (!open) {
                segment = ref.loaded;
                if (segment == null) {
                    if (currentLoadedSegment != null
                            && currentLoadedSegment.getSegmentId() == segmentId) {
                        segment = currentLoadedSegment;
                    } else {
                        pendingLoadRef = ref;
                        return filled;
                    }
                }
            }
            int count = countOf(open, ref);
            if (!open) {
                store.touchRef(ref);
            }
            if (!started) {
                offset = count - 1;
                started = true;
            }
            if (open) {
                boolean noDeletes = store.openDeletes.isEmpty();
                while (offset >= 0 && filled < buffer.length) {
                    int currentOffset = offset--;
                    long rowId = RowId.ofSegment(segmentId, currentOffset);
                    if (noDeletes || !store.openDeletes.contains(rowId)) {
                        Row row = Row.get(store.openSegment.rowValues(currentOffset), 0);
                        row.setKey(rowId);
                        buffer[filled++] = row;
                    }
                }
            } else {
                boolean hasDelta = store.delta.hasSegmentDelta(segmentId);
                while (offset >= 0 && filled < buffer.length) {
                    int currentOffset = offset--;
                    if (segment.isDeleted(currentOffset)) {
                        continue;
                    }
                    long rowId = RowId.ofSegment(segmentId, currentOffset);
                    if (hasDelta) {
                        if (store.delta.isSegmentRowDeleted(rowId)) {
                            continue;
                        }
                        Value[] updated = store.delta.getUpdated(rowId);
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
            ColumnarStore.SegmentRef ref = open ? null : store.refOf(segmentId);
            if (ref == null && !open) {
                nextSegmentForward();
                continue;
            }
            if (!open && !intersects(ref)) {
                nextSegmentForward();
                continue;
            }
            int count = countOf(open, ref);
            if (!open) {
                store.touchRef(ref);
            }
            boolean hasDelta = !open && store.delta.hasSegmentDelta(segmentId);
            while (offset < count) {
                long rowId = RowId.ofSegment(segmentId, offset);
                boolean deleted = open ? store.openDeletes.contains(rowId)
                        : store.refDeleted(ref, offset)
                                || (hasDelta && store.delta.isSegmentRowDeleted(rowId));
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
        while (segmentId >= store.firstSegmentId()) {
            boolean open = isOpen();
            ColumnarStore.SegmentRef ref = open ? null : store.refOf(segmentId);
            if (ref == null && !open) {
                nextSegmentReverse();
                continue;
            }
            if (!open && !intersects(ref)) {
                nextSegmentReverse();
                continue;
            }
            int count = countOf(open, ref);
            if (!open) {
                store.touchRef(ref);
            }
            if (!started) {
                offset = count - 1;
                started = true;
            }
            while (offset >= 0) {
                long rowId = RowId.ofSegment(segmentId, offset);
                boolean deleted = open ? store.openDeletes.contains(rowId)
                        : store.refDeleted(ref, offset)
                                || (store.delta.hasSegmentDelta(segmentId) && store.delta.isSegmentRowDeleted(rowId));
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
        currentLoadedSegment = null;
    }

    private void nextSegmentReverse() {
        segmentId--;
        offset = -1;
        started = false;
        currentLoadedSegment = null;
    }

    /** 当前段号是否仍是未冻结的开放段。 */
    private boolean isOpen() {
        return segmentId == lastSegmentId && segmentId == store.nextSegmentId;
    }

    /**
     * 当前段要遍历的行数。创建时的开放段可能被写满并冻结，
     * 新追加的行不属于本次快照。
     */
    private int countOf(boolean open, ColumnarStore.SegmentRef ref) {
        int available;
        if (open) {
            available = store.openSegment.rowCount();
        } else if (ref.loaded != null) {
            available = ref.loaded.getRowCount();
        } else {
            available = ref.skeleton.rowCount;
        }
        return segmentId == lastSegmentId ? Math.min(available, openRowCount) : available;
    }

    /** 段是否可能与给定区间相交；已逐出的段用骨架里的 zone map。 */
    private boolean intersects(ColumnarStore.SegmentRef ref) {
        if (lower == null && upper == null) {
            return true;
        }
        // 只有未回写的更新新像会让段的 zone map 失真（重开后 WAL 重放把更新放回 store.delta，
        // 段文件与其中的 zone map 仍是合并前的；未重开时合并也会先改段后清 store.delta）。
        // 删除只会收窄值域，不改变 zone map 的有效性，纯删除段仍参与边界剪枝。
        if (store.delta.hasSegmentUpdates(ref.segmentId)) {
            return true;
        }
        ColumnSegment seg = ref.loaded;
        if (seg != null) {
            return ColumnarStore.intersects(seg, lower, upper, store.valueTypes.length);
        }
        return store.skeletonIntersects(ref.skeleton, lower, upper);
    }
}

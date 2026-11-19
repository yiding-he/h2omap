/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.storage;

import java.util.BitSet;
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
 * <li>{@code updated}：已冻结段内的行被更新后的新像；</li>
 * <li>{@code segmentDeletes}：已冻结段内未写回段位图的删除标记，
 *     按段组织成位图（每行 1 bit），供读路径按位判定与空段判定。</li>
 * </ul>
 *
 * <p>开放段内的点改直接改段缓冲，不经过这里。
 *
 * <p>不是线程安全的，调用方（{@link ColumnarStore}）负责加锁。
 */
public final class DeltaStore {

    /** 单段的删除标记位图，附带已置位数计数，空段判定因此是 O(1)。 */
    static final class SegmentDeleteBitmap {

        final BitSet bits = new BitSet();

        int count;
    }

    private final Map<Long, Value[]> updated = new HashMap<>();

    private final Map<Integer, SegmentDeleteBitmap> segmentDeletes = new HashMap<>();

    private final Set<Integer> dirtySegments = new HashSet<>();

    /** 有未回写更新新像的段。纯删除段的 zone map 不会失真，仍可参与边界剪枝。 */
    private final Set<Integer> updatedSegments = new HashSet<>();

    /** 未回写删除标记总数，供合并触发判断。 */
    private int deletedTotal;

    /** 标记删除。 */
    public void markDeleted(long rowId) {
        int segmentId = RowId.segmentId(rowId);
        updated.remove(rowId);
        SegmentDeleteBitmap bitmap = segmentDeletes.computeIfAbsent(segmentId, k -> new SegmentDeleteBitmap());
        int offset = RowId.offset(rowId);
        if (!bitmap.bits.get(offset)) {
            bitmap.bits.set(offset);
            bitmap.count++;
            deletedTotal++;
        }
        dirtySegments.add(segmentId);
    }

    /** 记录已冻结段内行的新像。 */
    public void markUpdated(long rowId, Value[] values) {
        updated.put(rowId, values);
        int segmentId = RowId.segmentId(rowId);
        dirtySegments.add(segmentId);
        updatedSegments.add(segmentId);
    }

    public boolean isEmpty() {
        return updated.isEmpty() && segmentDeletes.isEmpty();
    }

    public boolean hasSegmentDelta(int segmentId) {
        return !isEmpty() && dirtySegments.contains(segmentId);
    }

    /** 该段是否有未回写的更新新像：有则段的 zone map 可能落后，边界剪枝必须跳过。 */
    public boolean hasSegmentUpdates(int segmentId) {
        return updatedSegments.contains(segmentId);
    }

    public Value[] getUpdated(long rowId) {
        return updated.get(rowId);
    }

    public boolean isSegmentRowDeleted(long rowId) {
        SegmentDeleteBitmap bitmap = segmentDeletes.get(RowId.segmentId(rowId));
        return bitmap != null && bitmap.bits.get(RowId.offset(rowId));
    }

    public int updatedSize() {
        return updated.size();
    }

    public int deletedSize() {
        return deletedTotal;
    }

    public Set<Map.Entry<Long, Value[]>> updatedEntries() {
        return updated.entrySet();
    }

    public Map<Long, Value[]> rawUpdated() {
        return updated;
    }

    /** 未回写删除标记按段展开，调用方持有存储锁。 */
    public Set<Map.Entry<Integer, SegmentDeleteBitmap>> segmentDeleteEntries() {
        return segmentDeletes.entrySet();
    }

    /** 某段未回写的删除位图，无则 null，调用方持有存储锁。 */
    BitSet segmentDeleteBits(int segmentId) {
        SegmentDeleteBitmap bitmap = segmentDeletes.get(segmentId);
        return bitmap == null ? null : bitmap.bits;
    }

    /** 某段未回写的删除标记数。 */
    public int segmentDeleteCount(int segmentId) {
        SegmentDeleteBitmap bitmap = segmentDeletes.get(segmentId);
        return bitmap == null ? 0 : bitmap.count;
    }

    /** 合并把某段的删除标记写回段位图后调用；脏段集合由 {@link #rebuildDirtySegments()} 重算。 */
    void clearSegmentDeletes(int segmentId) {
        SegmentDeleteBitmap bitmap = segmentDeletes.remove(segmentId);
        if (bitmap != null) {
            deletedTotal -= bitmap.count;
        }
    }

    /** 展开成 rowId 集合，供 checkpoint 写 tail 使用。 */
    public Set<Long> deletedRowIds() {
        Set<Long> rowIds = new HashSet<>(deletedTotal * 2);
        for (Map.Entry<Integer, SegmentDeleteBitmap> entry : segmentDeletes.entrySet()) {
            int segmentId = entry.getKey();
            for (int off = entry.getValue().bits.nextSetBit(0); off >= 0;
                    off = entry.getValue().bits.nextSetBit(off + 1)) {
                rowIds.add(RowId.ofSegment(segmentId, off));
            }
        }
        return rowIds;
    }

    public void clear() {
        updated.clear();
        segmentDeletes.clear();
        dirtySegments.clear();
        updatedSegments.clear();
        deletedTotal = 0;
    }

    /** 仅当当前值引用与快照一致时移除（防止误删锁外构建期间的新更新）。 */
    public void removeUpdatedIfSame(long rowId, Value[] expected) {
        if (updated.get(rowId) == expected) {
            updated.remove(rowId);
        }
    }

    /** 精确清理部分条目后重算脏段与更新段集合。 */
    public void rebuildDirtySegments() {
        dirtySegments.clear();
        updatedSegments.clear();
        for (long rowId : updated.keySet()) {
            int segmentId = RowId.segmentId(rowId);
            dirtySegments.add(segmentId);
            updatedSegments.add(segmentId);
        }
        for (int segmentId : segmentDeletes.keySet()) {
            dirtySegments.add(segmentId);
        }
    }
}

/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.storage;

import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.h2.value.CompareMode;
import org.h2.value.TypeInfo;
import org.h2.value.Value;

import com.hyd.h2omap.stats.ZoneMap;

/**
 * 行式写缓冲。列段不可变，所有点改先落在这里。
 *
 * <p>两块内容：
 * <ul>
 * <li>{@code updated}：已冻结段内的行被更新后的新像，按列存进
 *     {@link DeltaRowset}（位置不可变、每列带 zone map）；</li>
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

    private final DeltaRowset updatedRowset;

    private final Map<Integer, SegmentDeleteBitmap> segmentDeletes = new HashMap<>();

    private final Set<Integer> dirtySegments = new HashSet<>();

    /** 有未回写更新新像的段。纯删除段的 zone map 不会失真，仍可参与边界剪枝。 */
    private final Set<Integer> updatedSegments = new HashSet<>();

    /** 每段的 delta 屏蔽位版本：未回写删除或更新新像变化时递增，供部分聚合缓存判定失效。 */
    private final Map<Integer, Long> segmentVersions = new ConcurrentHashMap<>();

    /** 未回写删除标记总数，供合并触发判断。 */
    private int deletedTotal;

    public DeltaStore(TypeInfo[] columnTypes, CompareMode compareMode) {
        this.updatedRowset = new DeltaRowset(columnTypes, compareMode);
    }

    /** 标记删除。 */
    public void markDeleted(long rowId) {
        int segmentId = RowId.segmentId(rowId);
        updatedRowset.remove(rowId);
        SegmentDeleteBitmap bitmap = segmentDeletes.computeIfAbsent(segmentId, k -> new SegmentDeleteBitmap());
        int offset = RowId.offset(rowId);
        if (!bitmap.bits.get(offset)) {
            bitmap.bits.set(offset);
            bitmap.count++;
            deletedTotal++;
        }
        dirtySegments.add(segmentId);
        segmentVersions.merge(segmentId, 1L, Long::sum);
    }

    /** 记录已冻结段内行的新像。 */
    public void markUpdated(long rowId, Value[] values) {
        updatedRowset.upsert(rowId, values);
        int segmentId = RowId.segmentId(rowId);
        dirtySegments.add(segmentId);
        updatedSegments.add(segmentId);
        segmentVersions.merge(segmentId, 1L, Long::sum);
    }

    public boolean isEmpty() {
        return updatedRowset.isEmpty() && segmentDeletes.isEmpty();
    }

    public boolean hasSegmentDelta(int segmentId) {
        return !isEmpty() && dirtySegments.contains(segmentId);
    }

    /** 该段是否有未回写的更新新像：有则段的 zone map 可能落后，边界剪枝必须跳过。 */
    public boolean hasSegmentUpdates(int segmentId) {
        return updatedSegments.contains(segmentId);
    }

    public Value[] getUpdated(long rowId) {
        return updatedRowset.get(rowId);
    }

    public int updatedSize() {
        return updatedRowset.liveCount();
    }

    public int deletedSize() {
        return deletedTotal;
    }

    /** 合并第一阶段取活位置快照；位置不可变，锁外读取安全。 */
    public int[] updatedPositions() {
        return updatedRowset.livePositions();
    }

    public long updatedRowIdAt(int pos) {
        return updatedRowset.rowIdAt(pos);
    }

    /** 按不可变位置物化一行，供合并的锁外构建阶段使用。 */
    public Value[] updatedValueAt(int pos) {
        return updatedRowset.materializeAt(pos);
    }

    /** 全部活像物化成映射，供 checkpoint 写 tail 使用。 */
    public Map<Long, Value[]> snapshotUpdated() {
        return updatedRowset.snapshotEntries();
    }

    /** delta rowset 某列的 zone map，供向量化快照跳过不可能命中的新像。 */
    public ZoneMap updatedZoneMap(int column) {
        return updatedRowset.zoneMap(column);
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

    /** 某段未回写删除位图里的行是否被标记删除。 */
    public boolean isSegmentRowDeleted(long rowId) {
        SegmentDeleteBitmap bitmap = segmentDeletes.get(RowId.segmentId(rowId));
        return bitmap != null && bitmap.bits.get(RowId.offset(rowId));
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

    /** 某段的 delta 屏蔽位版本，随未回写删除与更新新像的每次变化递增。 */
    public long segmentVersion(int segmentId) {
        return segmentVersions.getOrDefault(segmentId, 0L);
    }

    public void clear() {
        updatedRowset.clear();
        segmentDeletes.clear();
        dirtySegments.clear();
        updatedSegments.clear();
        segmentVersions.clear();
        deletedTotal = 0;
    }

    /** 仅当该行仍指向快照时的位置才撤掉，防止误删合并锁外期间写入的新更新。 */
    public void removeUpdatedIfSamePosition(long rowId, int position) {
        updatedRowset.removeIfSamePosition(rowId, position);
    }

    /** 死位置超过半数时重排回收。调用方持有写锁。 */
    void compactRowsetIfWasteful() {
        updatedRowset.rebuildIfWasteful();
    }

    /** 精确清理部分条目后重算脏段与更新段集合。 */
    public void rebuildDirtySegments() {
        dirtySegments.clear();
        updatedSegments.clear();
        for (int pos : updatedRowset.livePositions()) {
            int segmentId = RowId.segmentId(updatedRowset.rowIdAt(pos));
            dirtySegments.add(segmentId);
            updatedSegments.add(segmentId);
        }
        for (int segmentId : segmentDeletes.keySet()) {
            dirtySegments.add(segmentId);
        }
    }
}

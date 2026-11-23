/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.storage;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.hyd.h2omap.persistence.OmapPersistence;
import com.hyd.h2omap.storage.ColumnarStore.SegmentMeta;
import com.hyd.h2omap.storage.ColumnarStore.SegmentRef;
import com.hyd.h2omap.storage.ColumnarStore.SegmentSkeleton;
import com.hyd.h2omap.storage.ColumnarStore.SkeletonData;
import com.hyd.h2omap.storage.DeltaStore.SegmentDeleteBitmap;
import com.hyd.h2omap.vector.NullBitmap;

import org.h2.value.Value;

/**
 * 检查点与开库恢复：写检查点、从持久化状态重建段映射与 delta，以及 WAL 重放。
 *
 * <p>调用前提：这些方法自行获取存储的写锁；仅在开库阶段单线程调用恢复与重放，
 * 检查点由写路径在写锁释放前后调用。
 */
final class StorePersistence {

    private final ColumnarStore store;

    StorePersistence(ColumnarStore store) {
        this.store = store;
    }

    void checkpoint() {
        OmapPersistence p = store.persistence;
        if (p == null) {
            return;
        }
        List<SegmentMeta> segsSnap;
        int nextSegIdSnap;
        long newWalSeq;
        List<Value[]> openRowsSnap;
        Set<Long> openDeletesSnap;
        Map<Long, Value[]> deltaUpdatedSnap;
        Set<Long> deltaDeletesSnap;
        store.lock.writeLock().lock();
        try {
            segsSnap = new ArrayList<>(store.segments.size());
            for (SegmentRef ref : store.segments.values()) {
                segsSnap.add(new SegmentMeta(ref.segmentId, ref.generation));
            }
            nextSegIdSnap = store.nextSegmentId;
            openRowsSnap = new ArrayList<>(store.openSegment.rawRows());
            openDeletesSnap = new HashSet<>(store.openDeletes);
            deltaUpdatedSnap = store.delta.snapshotUpdated();
            deltaDeletesSnap = new HashSet<>(store.delta.deletedRowIds());
            newWalSeq = p.rotateWal();
        } finally {
            store.lock.writeLock().unlock();
        }
        p.writeCheckpoint(store, segsSnap, nextSegIdSnap, newWalSeq,
                openRowsSnap, openDeletesSnap, deltaUpdatedSnap, deltaDeletesSnap);
    }

    // ------------------------------------------------------------------
    // 持久化恢复（仅在开库阶段单线程调用）
    // ------------------------------------------------------------------

    void restoreFromPersistence(int[] segmentIds, int[] generations,
            ColumnSegment[] preloaded, SkeletonData[] skeletons, int restoredNextSegId,
            List<Value[]> openRows, Set<Long> restoredOpenDeletes,
            Map<Long, Value[]> restoredDeltaUpdated, Set<Long> restoredDeltaDeletes) {
        store.lock.writeLock().lock();
        try {
            store.segments.clear();
            for (int i = 0; i < segmentIds.length; i++) {
                SegmentRef ref = new SegmentRef(segmentIds[i], generations[i], preloaded[i]);
                if (preloaded[i] != null) {
                    ref.residentBytes = preloaded[i].getEstimatedMemory();
                    store.residentAdd(ref.residentBytes);
                } else if (skeletons != null && skeletons[i] != null) {
                    // 骨架在恢复时登记：未加载内容的段对全部读路径可见且可剪枝。
                    ref.skeleton = new SegmentSkeleton(skeletons[i]);
                }
                store.segments.put(segmentIds[i], ref);
            }
            store.nextSegmentId = restoredNextSegId;
            store.openSegment.clear();
            for (Value[] row : openRows) {
                store.openSegment.append(row);
            }
            store.openDeletes.clear();
            store.openDeletes.addAll(restoredOpenDeletes);
            store.delta.clear();
            for (Map.Entry<Long, Value[]> e : restoredDeltaUpdated.entrySet()) {
                store.delta.markUpdated(e.getKey(), e.getValue());
            }
            for (long rowId : restoredDeltaDeletes) {
                store.delta.markDeleted(rowId);
            }
            recomputeRowCount();
            store.bump();
            // 恢复后同样遵守驻留预算：非惰性模式读入了全部段，这里裁到预算内。
            store.evictLocked();
        } finally {
            store.lock.writeLock().unlock();
        }
    }

    void replayInsert(long rowId, Value[] values) {
        store.lock.writeLock().lock();
        try {
            int segId = RowId.segmentId(rowId);
            while (store.nextSegmentId < segId) {
                store.nextSegmentId = segId;
            }
            store.openSegment.append(store.withRowId(rowId, values));
            store.rowCount++;
            store.bump();
            if (store.openSegment.rowCount() >= store.options.getSegmentSize()) {
                store.freeze();
            }
        } finally {
            store.lock.writeLock().unlock();
        }
    }

    void replayUpdate(long rowId, Value[] values) {
        store.lock.writeLock().lock();
        try {
            if (store.isFrozen(rowId)) {
                store.delta.markUpdated(rowId, values);
            } else {
                store.openSegment.setRow(RowId.offset(rowId), store.withRowId(rowId, values));
            }
            store.bump();
        } finally {
            store.lock.writeLock().unlock();
        }
    }

    void replayDelete(long rowId) {
        store.lock.writeLock().lock();
        try {
            if (store.isVisible(rowId)) {
                int segId = RowId.segmentId(rowId);
                if (store.isFrozen(rowId)) {
                    store.delta.markDeleted(rowId);
                } else {
                    store.openDeletes.add(rowId);
                }
                if (store.countedInRowCount(rowId)) {
                    store.rowCount--;
                }
                store.dropSegmentIfEmpty(segId);
                store.bump();
            }
        } finally {
            store.lock.writeLock().unlock();
        }
    }

    void replayTruncate(int restoredNextSegId) {
        store.lock.writeLock().lock();
        try {
            store.segments.clear();
            store.delta.clear();
            store.aggCache.clear();
            store.openSegment.clear();
            store.openDeletes.clear();
            store.nextSegmentId = restoredNextSegId;
            store.rowCount = 0;
            store.bump();
        } finally {
            store.lock.writeLock().unlock();
        }
    }

    private void recomputeRowCount() {
        long count = 0;
        for (SegmentRef ref : store.segments.values()) {
            // 未加载且无骨架的段（旧格式 manifest）不计入，与读路径的可见性判定保持一致；
            // 有骨架的段按骨架计入，保证可见性与剪枝从开库就完整。
            ColumnSegment seg = ref.loaded;
            if (seg != null) {
                count += SegmentLoader.visibleRowsOf(seg);
            } else if (ref.skeleton != null) {
                count += ref.skeleton.rowCount - ref.skeleton.deletedCount;
            }
        }
        for (Map.Entry<Integer, SegmentDeleteBitmap> entry : store.delta.segmentDeleteEntries()) {
            SegmentRef ref = store.segments.get(entry.getKey());
            ColumnSegment seg = ref != null ? ref.loaded : null;
            NullBitmap deleted = seg != null ? seg.getDeleted()
                    : ref != null && ref.skeleton != null ? ref.skeleton.deleted : null;
            if (deleted == null) {
                continue;
            }
            for (int off = entry.getValue().bits.nextSetBit(0); off >= 0;
                    off = entry.getValue().bits.nextSetBit(off + 1)) {
                if (!deleted.isNull(off)) {
                    count--;
                }
            }
        }
        count += store.openSegment.rowCount() - store.openDeletes.size();
        store.rowCount = Math.max(0, count);
    }
}

/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.storage;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
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
import com.hyd.h2omap.stats.ZoneMap;
import com.hyd.h2omap.vector.NullBitmap;

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

    /**
     * 已冻结的段，按段号升序。惰性加载时元素的 {@code loaded} 可能为 null。
     * 按段号映射而不是按下标：全删段会从中间被整体丢弃，段号不再连续。
     */
    private final TreeMap<Integer, SegmentRef> segments = new TreeMap<>();

    private final DeltaStore delta;

    /** 段级部分聚合缓存（MOR 批次 3）：签名 × 段的部分聚合结果。 */
    private final com.hyd.h2omap.vectorized.SegmentAggCache aggCache =
            new com.hyd.h2omap.vectorized.SegmentAggCache();

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

    /** 空位置数组：rowset 被 zone map 跳过时用它替代。 */
    private static final int[] EMPTY_POSITIONS = new int[0];

    private volatile OmapPersistence persistence;

    /** 只读 manifest 而不读段文件；由服务端系统属性 {@code h2.omapLazyLoad} 决定。 */
    private volatile boolean lazyLoad;

    /** 后台段加载线程，仅惰性模式下存在。 */
    private volatile Thread lazyLoader;

    private volatile boolean lazyLoaderStopped;

    /** 后台加载中读段失败时保留的最后一个异常，供测试与诊断读取。 */
    private volatile Throwable lastLazyLoadError;

    /** 驻留段访问序号，值域从 2^40 起，保证大于任何未访问初始键（段号）。 */
    private static final long TOUCH_BASE = 1L << 40;

    /** 驻留段访问序号，正值单调递增，供 LRU 逐出排序。 */
    private final AtomicLong touchCounter = new AtomicLong();

    /** 全部驻留段的估算字节数，写锁内维护。 */
    private long residentTotal;

    /** 累计逐出段数，供测试断言。 */
    private final AtomicLong evictedSegments = new AtomicLong();

    /** 段加载完成后的通知，用来让 H2 的查询结果缓存失效。 */
    private volatile Runnable visibilityListener;

    /** 段的持久化元数据：checkpoint 与 manifest 只需要这两个值。 */
    public record SegmentMeta(int segmentId, int generation) {
    }

    /**
     * 段骨架元数据的载体：检查点写出与恢复登记共用这一形态。
     * 骨架让未加载内容的段保持可见、可剪枝，是驻留预算与惰性加载同时开启的前提。
     */
    public static final class SkeletonData {

        public final int rowCount;

        public final int deletedCount;

        public final NullBitmap deleted;

        public final ZoneMap[] zoneMaps;

        public SkeletonData(int rowCount, int deletedCount, NullBitmap deleted, ZoneMap[] zoneMaps) {
            this.rowCount = rowCount;
            this.deletedCount = deletedCount;
            this.deleted = deleted;
            this.zoneMaps = zoneMaps;
        }
    }

    /** 段引用：{@code loaded} 为 null 且 {@code skeleton} 非 null 表示已逐出；两者都为 null 表示惰性加载中。 */
    private static final class SegmentRef {

        final int segmentId;

        int generation;

        volatile ColumnSegment loaded;

        /** 逐出后的常驻骨架；驻留时为 null。 */
        volatile SegmentSkeleton skeleton;

        /** 驻留内容的估算字节数，写锁内维护；逐出后归零。 */
        long residentBytes;

        /**
         * LRU 键：小于 2^40 表示从未被查询访问（取段号，段号越老越优先逐出），
         * 否则为访问序号。逐出取最小者。
         */
        volatile long lastTouch;

        SegmentRef(int segmentId, int generation, ColumnSegment loaded) {
            this.segmentId = segmentId;
            this.generation = generation;
            this.loaded = loaded;
            this.lastTouch = segmentId;
        }
    }

    /** 逐出时留在堆上的段级元数据：行数、删除位图与每列 zone map。 */
    private static final class SegmentSkeleton {

        final int rowCount;

        final int deletedCount;

        /** 与被逐出段共享同一对象：段内删除位图冻结后不再变。 */
        final NullBitmap deleted;

        final ZoneMap[] zoneMaps;

        SegmentSkeleton(ColumnSegment segment) {
            this.rowCount = segment.getRowCount();
            this.deletedCount = segment.getDeletedCount();
            this.deleted = segment.getDeleted();
            this.zoneMaps = new ZoneMap[segment.getColumnCount()];
            for (int c = 0; c < zoneMaps.length; c++) {
                this.zoneMaps[c] = segment.getZoneMap(c);
            }
        }

        SegmentSkeleton(SkeletonData data) {
            this.rowCount = data.rowCount;
            this.deletedCount = data.deletedCount;
            this.deleted = data.deleted;
            this.zoneMaps = data.zoneMaps;
        }
    }

    public ColumnarStore(Column[] columns, OmapOptions options, CompareMode compareMode) {
        this.options = options;
        this.compareMode = compareMode;
        this.valueTypes = new int[columns.length];
        this.columnTypes = new TypeInfo[columns.length];
        for (int i = 0; i < columns.length; i++) {
            columnTypes[i] = columns[i].getType();
            valueTypes[i] = columnTypes[i].getValueType();
        }
        this.delta = new DeltaStore(columnTypes, compareMode);
        this.openSegment = new SegmentBuilder();
    }

    /** 段级部分聚合缓存，按「签名 × 段」失效，跨段写只使被改动的段失效。 */
    public com.hyd.h2omap.vectorized.SegmentAggCache getAggCache() {
        return aggCache;
    }

    public void setPersistence(OmapPersistence persistence) {
        this.persistence = persistence;
    }

    /** 只登记 manifest、不读段文件。必须在 {@code recover()} 之前设置。 */
    public void setLazyLoad(boolean lazyLoad) {
        this.lazyLoad = lazyLoad;
    }

    public boolean isLazyLoad() {
        return lazyLoad;
    }

    /** 段加载完成后的回调，用来让 H2 的查询结果缓存失效。 */
    public void setVisibilityListener(Runnable listener) {
        this.visibilityListener = listener;
    }

    /**
     * 启动后台段加载。按段号倒序进行：段号与写入顺序同向，
     * 所以最近写入的数据最先对查询可见。
     */
    public void startLazyLoading() {
        if (!lazyLoad || lazyLoader != null) {
            return;
        }
        Thread thread = new Thread(this::loadAllSegments, "omap-lazy-loader");
        thread.setDaemon(true);
        lazyLoaderStopped = false;
        lazyLoader = thread;
        thread.start();
    }

    /** 停止后台加载并等待其退出。 */
    public void stopLazyLoading() {
        Thread thread = lazyLoader;
        lazyLoaderStopped = true;
        if (thread != null) {
            thread.interrupt();
            try {
                thread.join(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            lazyLoader = null;
        }
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
            evictIfNeeded();
        }
        return rowId;
    }

    /** 驻留字节超出预算时逐出段，写路径与合并收尾调用。 */
    private void evictIfNeeded() {
        lock.writeLock().lock();
        try {
            evictLocked();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 运行时修改驻留预算：调小立即逐出超预算的段；调大不重启预载，由后续查询自然填驻留。
     * evictLocked 自带预算判断：预算 0（不逐出）或未超预算时空转。
     */
    public void setResidentBudget(long bytes) {
        lock.writeLock().lock();
        try {
            options.setResidentBytes(bytes);
            evictLocked();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** 当前驻留预算，0 表示不逐出。 */
    public long getResidentBudget() {
        return options.getResidentBytes();
    }

    public void remove(long rowId) {
        boolean needCompact = false;
        lock.writeLock().lock();
        try {
            if (isVisible(rowId)) {
                int segId = RowId.segmentId(rowId);
                if (isFrozen(rowId)) {
                    delta.markDeleted(rowId);
                } else {
                    openDeletes.add(rowId);
                }
                if (countedInRowCount(rowId)) {
                    rowCount--;
                }
                // 全删段立即整体丢弃：rowId 空洞保留，行数已在上面逐行扣过。
                dropSegmentIfEmpty(segId);
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
            residentTotal = 0;
            delta.clear();
            aggCache.clear();
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
            int[] snapPositions;
            long[] snapRowIds;
            int snapCount = 0;
            int foldedDeletes = 0;

            lock.writeLock().lock();
            try {
                if (delta.updatedSize() == 0 && delta.deletedSize() == 0) {
                    return 0;
                }
                // 列向量只在锁内安全，第一阶段就把活像物化成不可变快照；
                // 第三阶段按不可变位置双重比对，锁外期间的新更新不会被误删。
                int[] positions = delta.updatedPositions();
                snapPositions = new int[positions.length];
                snapRowIds = new long[positions.length];
                for (int pos : positions) {
                    long rowId = delta.updatedRowIdAt(pos);
                    int segId = RowId.segmentId(rowId);
                    ColumnSegment seg = segment(segId);
                    if (seg == null) {
                        // 受影响的段已被逐出：合并需要重写段内容，先同步读回。
                        seg = ensureResident(segments.get(segId));
                    }
                    if (seg != null) {
                        baseSegments.putIfAbsent(segId, seg);
                        updatesBySeg.computeIfAbsent(segId, k -> new ArrayList<>())
                                .add(new AbstractMap.SimpleImmutableEntry<>(
                                        RowId.offset(rowId), delta.updatedValueAt(pos)));
                        snapPositions[snapCount] = pos;
                        snapRowIds[snapCount] = rowId;
                        snapCount++;
                    }
                }
                for (Map.Entry<Integer, DeltaStore.SegmentDeleteBitmap> entry : delta.segmentDeleteEntries()) {
                    int segId = entry.getKey();
                    ColumnSegment seg = segment(segId);
                    if (seg == null) {
                        seg = ensureResident(segments.get(segId));
                    }
                    if (seg != null) {
                        baseSegments.putIfAbsent(segId, seg);
                        List<Integer> offsets = deletesBySeg.computeIfAbsent(segId, k -> new ArrayList<>());
                        for (int off = entry.getValue().bits.nextSetBit(0); off >= 0;
                                off = entry.getValue().bits.nextSetBit(off + 1)) {
                            if (off < seg.getRowCount()) {
                                offsets.add(off);
                                foldedDeletes++;
                            }
                        }
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
                for (Map.Entry<Integer, ColumnSegment> entry : builtSegments.entrySet()) {
                    int segId = entry.getKey();
                    SegmentRef ref = segments.get(segId);
                    if (ref == null || ref.loaded != baseSegments.get(segId)) {
                        return 0;
                    }
                    ColumnSegment newSeg = entry.getValue();
                    if (newSeg.getDeletedCount() >= newSeg.getRowCount()) {
                        // 回写后整段全删：直接丢弃，不再挂回段列表。
                        segments.remove(segId);
                        residentTotal -= ref.residentBytes;
                    } else {
                        ref.loaded = newSeg;
                        ref.generation = newSeg.getGeneration();
                        residentTotal += newSeg.getEstimatedMemory() - ref.residentBytes;
                        ref.residentBytes = newSeg.getEstimatedMemory();
                    }
                    delta.clearSegmentDeletes(segId);
                }
                for (int i = 0; i < snapCount; i++) {
                    delta.removeUpdatedIfSamePosition(snapRowIds[i], snapPositions[i]);
                }
                delta.rebuildDirtySegments();
                delta.compactRowsetIfWasteful();
                bump();
                compactedCount = (long) snapCount + foldedDeletes;
            } finally {
                lock.writeLock().unlock();
            }
        }
        if (persistence != null && compactedCount >= Math.max(1024, options.getCompactionMinRows())) {
            checkpoint();
        }
        evictIfNeeded();
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
        List<SegmentMeta> segsSnap;
        int nextSegIdSnap;
        long newWalSeq;
        List<Value[]> openRowsSnap;
        Set<Long> openDeletesSnap;
        Map<Long, Value[]> deltaUpdatedSnap;
        Set<Long> deltaDeletesSnap;
        lock.writeLock().lock();
        try {
            segsSnap = new ArrayList<>(segments.size());
            for (SegmentRef ref : segments.values()) {
                segsSnap.add(new SegmentMeta(ref.segmentId, ref.generation));
            }
            nextSegIdSnap = nextSegmentId;
            openRowsSnap = new ArrayList<>(openSegment.rawRows());
            openDeletesSnap = new HashSet<>(openDeletes);
            deltaUpdatedSnap = delta.snapshotUpdated();
            deltaDeletesSnap = new HashSet<>(delta.deletedRowIds());
            newWalSeq = p.rotateWal();
        } finally {
            lock.writeLock().unlock();
        }
        p.writeCheckpoint(this, segsSnap, nextSegIdSnap, newWalSeq,
                openRowsSnap, openDeletesSnap, deltaUpdatedSnap, deltaDeletesSnap);
    }

    // ------------------------------------------------------------------
    // 持久化恢复（仅在开库阶段单线程调用）
    // ------------------------------------------------------------------

    public void restoreFromPersistence(int[] segmentIds, int[] generations,
            ColumnSegment[] preloaded, SkeletonData[] skeletons, int restoredNextSegId,
            List<Value[]> openRows, Set<Long> restoredOpenDeletes,
            Map<Long, Value[]> restoredDeltaUpdated, Set<Long> restoredDeltaDeletes) {
        lock.writeLock().lock();
        try {
            segments.clear();
            for (int i = 0; i < segmentIds.length; i++) {
                SegmentRef ref = new SegmentRef(segmentIds[i], generations[i], preloaded[i]);
                if (preloaded[i] != null) {
                    ref.residentBytes = preloaded[i].getEstimatedMemory();
                    residentTotal += ref.residentBytes;
                } else if (skeletons != null && skeletons[i] != null) {
                    // 骨架在恢复时登记：未加载内容的段对全部读路径可见且可剪枝。
                    ref.skeleton = new SegmentSkeleton(skeletons[i]);
                }
                segments.put(segmentIds[i], ref);
            }
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
            // 恢复后同样遵守驻留预算：非惰性模式读入了全部段，这里裁到预算内。
            evictLocked();
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
                int segId = RowId.segmentId(rowId);
                if (isFrozen(rowId)) {
                    delta.markDeleted(rowId);
                } else {
                    openDeletes.add(rowId);
                }
                if (countedInRowCount(rowId)) {
                    rowCount--;
                }
                dropSegmentIfEmpty(segId);
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
            aggCache.clear();
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
        for (SegmentRef ref : segments.values()) {
            // 未加载且无骨架的段（旧格式 manifest）不计入，与读路径的可见性判定保持一致；
            // 有骨架的段按骨架计入，保证可见性与剪枝从开库就完整。
            ColumnSegment seg = ref.loaded;
            if (seg != null) {
                count += visibleRowsOf(seg);
            } else if (ref.skeleton != null) {
                count += ref.skeleton.rowCount - ref.skeleton.deletedCount;
            }
        }
        for (Map.Entry<Integer, DeltaStore.SegmentDeleteBitmap> entry : delta.segmentDeleteEntries()) {
            SegmentRef ref = segments.get(entry.getKey());
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
        ColumnSegment transientSegment = null;
        if (isFrozen(rowId) && ensureFrozenSegmentResident(rowId)) {
            // 预算容不下单个段：读回后立即被逐出，读一份仅供本次物化。
            transientSegment = readSegmentTransient(segments.get(RowId.segmentId(rowId)));
        }
        lock.readLock().lock();
        try {
            Value[] values;
            if (isFrozen(rowId)) {
                int segId = RowId.segmentId(rowId);
                SegmentRef ref = segments.get(segId);
                ColumnSegment segment = transientSegment != null ? transientSegment : ref == null ? null : ref.loaded;
                int offset = RowId.offset(rowId);
                if (segment == null || offset >= segment.getRowCount()
                        || segment.isDeleted(offset)) {
                    return null;
                }
                if (transientSegment == null && ref != null) {
                    touchRef(ref);
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
     * 逐出段的行被点查命中时先把段读回。读回后该段拥有最新的访问序号，
     * 紧接着的预算逐出不会先选它。返回 true 表示读回后段仍未驻留
     * （预算容不下单个段），调用方需要退回临时读。
     */
    private boolean ensureFrozenSegmentResident(long rowId) {
        SegmentRef evicted = null;
        lock.readLock().lock();
        try {
            if (isFrozen(rowId)) {
                SegmentRef ref = segments.get(RowId.segmentId(rowId));
                if (ref != null && ref.loaded == null && ref.skeleton != null) {
                    evicted = ref;
                }
            }
        } finally {
            lock.readLock().unlock();
        }
        if (evicted == null) {
            return false;
        }
        ensureResident(evicted);
        lock.readLock().lock();
        try {
            SegmentRef ref = segments.get(RowId.segmentId(rowId));
            return ref == null || ref.loaded == null;
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
        List<ColumnSegment> loaded = new ArrayList<>(segments.size());
        for (SegmentRef ref : segments.values()) {
            if (ref.loaded != null) {
                loaded.add(ref.loaded);
            }
        }
        return loaded;
    }

    /** 已登记的段数（含尚未加载的段）。 */
    public int segmentCount() {
        lock.readLock().lock();
        try {
            return segments.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    /** 已加载的段数，供惰性加载的测试断言。 */
    public int loadedSegmentCount() {
        lock.readLock().lock();
        try {
            int n = 0;
            for (SegmentRef ref : segments.values()) {
                if (ref.loaded != null) {
                    n++;
                }
            }
            return n;
        } finally {
            lock.readLock().unlock();
        }
    }

    // ------------------------------------------------------------------
    // 后台段加载
    // ------------------------------------------------------------------

    private void loadAllSegments() {
        OmapPersistence p = persistence;
        if (p == null) {
            return;
        }
        SegmentRef[] refs;
        Map<Integer, List<Integer>> deletedOffsetsBySegment = new HashMap<>();
        lock.readLock().lock();
        try {
            refs = segments.values().toArray(new SegmentRef[0]);
            for (Map.Entry<Integer, DeltaStore.SegmentDeleteBitmap> entry : delta.segmentDeleteEntries()) {
                List<Integer> offsets = new ArrayList<>();
                for (int off = entry.getValue().bits.nextSetBit(0); off >= 0;
                        off = entry.getValue().bits.nextSetBit(off + 1)) {
                    offsets.add(off);
                }
                deletedOffsetsBySegment.put(entry.getKey(), offsets);
            }
        } finally {
            lock.readLock().unlock();
        }
        for (int i = refs.length - 1; i >= 0 && !lazyLoaderStopped; i--) {
            if (options.getResidentBytes() > 0 && getResidentBytes() >= options.getResidentBytes()) {
                // 驻留预算已装满：继续预载只会载入即逐出，老段等查询按需读回。
                break;
            }
            SegmentRef ref = refs[i];
            if (ref.loaded != null) {
                continue;
            }
            ColumnSegment segment;
            try {
                segment = p.loadSegment(ref.segmentId, ref.generation);
            } catch (Exception e) {
                // 段文件读不出来时跳过这一段，其余段继续加载；读路径继续把该段当作不可见。
                lastLazyLoadError = e;
                continue;
            }
            boolean published = publishLoaded(ref, segment, deletedOffsetsBySegment.get(ref.segmentId));
            if (published && ref.loaded == null) {
                // 刚发布的段被预算立即逐出：预算装不下更多段，再预载都是无效读；
                // 全删段被丢弃时 ref.loaded 仍非空，不会误判。
                break;
            }
        }
    }

    private boolean publishLoaded(SegmentRef ref, ColumnSegment segment, List<Integer> deletedOffsets) {
        lock.writeLock().lock();
        try {
            if (ref.loaded != null) {
                return false;
            }
            if (ref.skeleton != null) {
                // 骨架已在恢复时计入行数并保证可见性：这里只挂内容，不动可见性与结果缓存。
                ref.loaded = segment;
                ref.residentBytes = segment.getEstimatedMemory();
                residentTotal += ref.residentBytes;
                evictLocked();
            } else {
                ref.loaded = segment;
                ref.residentBytes = segment.getEstimatedMemory();
                residentTotal += ref.residentBytes;
                long added = visibleRowsOf(segment);
                if (deletedOffsets != null) {
                    int rowCountOfSegment = segment.getRowCount();
                    for (int offset : deletedOffsets) {
                        if (offset < rowCountOfSegment && !segment.isDeleted(offset)) {
                            added--;
                        }
                    }
                }
                rowCount += Math.max(0, added);
                // 加载后该段可能整段已删：与写路径同一条丢弃判定。
                dropSegmentIfEmpty(ref.segmentId);
                bump();
                evictLocked();
            }
        } finally {
            lock.writeLock().unlock();
        }
        Runnable listener = visibilityListener;
        if (listener != null) {
            listener.run();
        }
        return true;
    }

    private static long visibleRowsOf(ColumnSegment segment) {
        int rowCount = segment.getRowCount();
        if (!segment.getDeleted().hasNull()) {
            return rowCount;
        }
        long visible = 0;
        for (int i = 0; i < rowCount; i++) {
            if (!segment.isDeleted(i)) {
                visible++;
            }
        }
        return visible;
    }

    // ------------------------------------------------------------------
    // 段级逐出（MOR 批次 4）
    // ------------------------------------------------------------------

    /**
     * 驻留字节超出预算时逐出段。逐出后段仍可见：行数、删除位图与每列 zone map
     * 留在骨架里，命中时从段文件同步读回。
     * 逐出对象优先选从未被查询访问过的段（段号老的先逐出），
     * 其次选访问时间最早的段，与数据的写入顺序无关。
     * 调用方必须已持有写锁。
     */
    private void evictLocked() {
        long budget = options.getResidentBytes();
        if (budget <= 0 || persistence == null || residentTotal <= budget) {
            return;
        }
        while (residentTotal > budget) {
            SegmentRef victim = null;
            for (SegmentRef ref : segments.values()) {
                if (ref.loaded == null || !persistence.hasSegmentFile(ref.segmentId, ref.generation)) {
                    continue;
                }
                if (victim == null || ref.lastTouch < victim.lastTouch) {
                    victim = ref;
                }
            }
            if (victim == null) {
                break;
            }
            victim.skeleton = new SegmentSkeleton(victim.loaded);
            victim.loaded = null;
            residentTotal -= victim.residentBytes;
            victim.residentBytes = 0;
            evictedSegments.incrementAndGet();
        }
    }

    /**
     * 确保段驻留：已逐出时从段文件同步读回。并发下只允许一个读回生效，
     * 读回后视为刚被访问，不会被紧接着的预算逐出立刻逐出。
     */
    private ColumnSegment ensureResident(SegmentRef ref) {
        if (ref == null) {
            return null;
        }
        ColumnSegment loaded = ref.loaded;
        if (loaded != null) {
            return loaded;
        }
        OmapPersistence p = persistence;
        if (p == null || ref.skeleton == null) {
            return null;
        }
        try {
            loaded = p.loadSegment(ref.segmentId, ref.generation);
        } catch (IOException e) {
            throw new IllegalStateException(
                    "逐出段读回失败: seg_" + ref.segmentId + "_" + ref.generation, e);
        }
        lock.writeLock().lock();
        try {
            if (ref.loaded != null) {
                return ref.loaded;
            }
            if (ref.skeleton == null) {
                return null;
            }
            ref.loaded = loaded;
            ref.skeleton = null;
            ref.residentBytes = loaded.getEstimatedMemory();
            residentTotal += ref.residentBytes;
            touchRef(ref);
            evictLocked();
        } finally {
            lock.writeLock().unlock();
        }
        return loaded;
    }

    /** 读路径触碰一段：记下访问序号，供 LRU 逐出排序。须在锁内调用。 */
    private void touchRef(SegmentRef ref) {
        ref.lastTouch = TOUCH_BASE + touchCounter.incrementAndGet();
    }

    /** 该段当前是否驻留堆上。 */
    public boolean isSegmentResident(int segmentId) {
        lock.readLock().lock();
        try {
            SegmentRef ref = segments.get(segmentId);
            return ref != null && ref.loaded != null;
        } finally {
            lock.readLock().unlock();
        }
    }

    /** 全部驻留段的估算字节数。 */
    public long getResidentBytes() {
        lock.readLock().lock();
        try {
            return residentTotal;
        } finally {
            lock.readLock().unlock();
        }
    }

    /** 累计逐出的段数，供测试断言。 */
    public long getEvictedSegmentCount() {
        return evictedSegments.get();
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
        SegmentRef ref = new SegmentRef(nextSegmentId, segment.getGeneration(), segment);
        ref.residentBytes = segment.getEstimatedMemory();
        residentTotal += ref.residentBytes;
        segments.put(nextSegmentId, ref);
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
        int segId = RowId.segmentId(rowId);
        SegmentRef ref = segments.get(segId);
        if (ref == null) {
            // 段尚未加载时读不到段内删除位图；rowId 由 H2 或 WAL 提供，按存在处理，
            // 使 WAL 重放与客户端删除能记进 delta，加载时再统一扣除。
            return true;
        }
        if (ref.loaded != null) {
            return !ref.loaded.isDeleted(RowId.offset(rowId));
        }
        // 已逐出的段读骨架里的删除位图。
        return ref.skeleton == null || !ref.skeleton.deleted.isNull(RowId.offset(rowId));
    }

    private boolean isFrozen(long rowId) {
        return RowId.segmentId(rowId) < nextSegmentId;
    }

    /** 该行是否已计入 {@code rowCount}：未加载段（含逐出段）的行在加载完成时才计入。 */
    private boolean countedInRowCount(long rowId) {
        if (!isFrozen(rowId)) {
            return true;
        }
        SegmentRef ref = segments.get(RowId.segmentId(rowId));
        return ref != null && (ref.loaded != null || ref.skeleton != null);
    }

    private ColumnSegment segment(int segmentId) {
        SegmentRef ref = segments.get(segmentId);
        return ref == null ? null : ref.loaded;
    }

    /** 读路径的段引用：已丢弃段与惰性未加载段返回 null；逐出段照常返回。须在锁内调用。 */
    private SegmentRef refOf(int segmentId) {
        SegmentRef ref = segments.get(segmentId);
        if (ref == null || (ref.loaded == null && ref.skeleton == null)) {
            return null;
        }
        return ref;
    }

    /** 段内删除位：已逐出的段读骨架位图。须在锁内调用。 */
    private boolean refDeleted(SegmentRef ref, int offset) {
        ColumnSegment seg = ref.loaded;
        return seg != null ? seg.isDeleted(offset) : ref.skeleton.deleted.isNull(offset);
    }

    /** 读一份段内容供当次迭代使用，不回写驻留状态；仅批量路径预读后又被并发逐出的窗口使用。 */
    private ColumnSegment readSegmentTransient(SegmentRef ref) {
        OmapPersistence p = persistence;
        if (p == null || ref.skeleton == null) {
            return null;
        }
        try {
            return p.loadSegment(ref.segmentId, ref.generation);
        } catch (IOException e) {
            throw new IllegalStateException(
                    "逐出段读回失败: seg_" + ref.segmentId + "_" + ref.generation, e);
        }
    }

    /** 按段号取已加载的段，供 checkpoint 写盘使用。 */
    /**
     * 检查点要写进 manifest 的骨架元数据；段既无内容也无骨架时返回 null。
     */
    public SkeletonData segmentSkeletonData(int segmentId) {
        lock.readLock().lock();
        try {
            SegmentRef ref = segments.get(segmentId);
            if (ref == null) {
                return null;
            }
            ColumnSegment seg = ref.loaded;
            if (seg != null) {
                ZoneMap[] zones = new ZoneMap[seg.getColumnCount()];
                for (int c = 0; c < zones.length; c++) {
                    zones[c] = seg.getZoneMap(c);
                }
                return new SkeletonData(seg.getRowCount(), seg.getDeletedCount(), seg.getDeleted(), zones);
            }
            SegmentSkeleton sk = ref.skeleton;
            if (sk == null) {
                return null;
            }
            return new SkeletonData(sk.rowCount, sk.deletedCount, sk.deleted, sk.zoneMaps);
        } finally {
            lock.readLock().unlock();
        }
    }

    public ColumnSegment loadedSegment(int segmentId) {
        lock.readLock().lock();
        try {
            return segment(segmentId);
        } finally {
            lock.readLock().unlock();
        }
    }

    private int firstSegmentId() {
        return segments.isEmpty() ? nextSegmentId : segments.firstKey();
    }

    /**
     * 段内已标记删除的行数（段位图加 delta 未回写部分）达到段行数时
     * 把整段从段列表摘除，段文件在下次 checkpoint 随 manifest 一起清理。
     * 调用方必须已持有写锁。rowId 空洞保留，不计入任何行数调整。
     */
    private void dropSegmentIfEmpty(int segmentId) {
        SegmentRef ref = segments.get(segmentId);
        if (ref == null || (ref.loaded == null && ref.skeleton == null)) {
            return;
        }
        // 已逐出的段只有骨架，同样适用同一条丢弃判定。
        ColumnSegment seg = ref.loaded;
        int segRowCount = seg != null ? seg.getRowCount() : ref.skeleton.rowCount;
        long deleted = (long) (seg != null ? seg.getDeletedCount() : ref.skeleton.deletedCount)
                + delta.segmentDeleteCount(segmentId);
        if (deleted < segRowCount) {
            return;
        }
        segments.remove(segmentId);
        residentTotal -= ref.residentBytes;
        delta.clearSegmentDeletes(segmentId);
        aggCache.invalidateSegment(segmentId);
        bump();
    }

    /** 恢复完成后的一次性扫描：把旧版本数据里可能存在的全删段摘除。 */
    public void dropEmptySegments() {
        lock.writeLock().lock();
        try {
            for (int segId : new ArrayList<>(segments.keySet())) {
                dropSegmentIfEmpty(segId);
            }
        } finally {
            lock.writeLock().unlock();
        }
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
            // 产行的批量路径需要段内容：先在读锁外把剩余扫描范围内的逐出段读回。
            ensureRangeResident();
            lock.readLock().lock();
            try {
                return reverse ? nextBatchReverse(buffer) : nextBatchForward(buffer);
            } finally {
                lock.readLock().unlock();
            }
        }

        /** 剩余扫描范围内的逐出段读回驻留；用骨架 zone map 先剪枝，预算容不下时批量路径退回临时读。 */
        private void ensureRangeResident() {
            List<SegmentRef> evicted = null;
            lock.readLock().lock();
            try {
                for (SegmentRef ref : reverse
                        ? segments.headMap(segmentId, true).values()
                        : segments.tailMap(segmentId, true).values()) {
                    if (ref.loaded == null && ref.skeleton != null && intersects(ref)) {
                        if (evicted == null) {
                            evicted = new ArrayList<>();
                        }
                        evicted.add(ref);
                    }
                }
            } finally {
                lock.readLock().unlock();
            }
            if (evicted != null) {
                for (SegmentRef ref : evicted) {
                    ensureResident(ref);
                }
            }
        }

        /** 批量路径的段内容：驻留的取驻留值；预读后又被逐出的读一份仅供本次迭代，不回写驻留状态。 */
        private ColumnSegment residentForBatch(SegmentRef ref) {
            if (ref == null) {
                return null;
            }
            ColumnSegment seg = ref.loaded;
            if (seg != null) {
                return seg;
            }
            return ref.skeleton == null ? null : readSegmentTransient(ref);
        }

        private int nextBatchForward(Row[] buffer) {
            int filled = 0;
            while (filled < buffer.length && segmentId <= lastSegmentId) {
                boolean open = isOpen();
                SegmentRef ref = open ? null : refOf(segmentId);
                ColumnSegment segment = open ? null : residentForBatch(ref);
                if (segment == null && !open) {
                    nextSegmentForward();
                    continue;
                }
                if (!open && !intersects(ref)) {
                    nextSegmentForward();
                    continue;
                }
                int count = countOf(open, ref);
                if (!open) {
                    touchRef(ref);
                }
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
                SegmentRef ref = open ? null : refOf(segmentId);
                ColumnSegment segment = open ? null : residentForBatch(ref);
                if (segment == null && !open) {
                    nextSegmentReverse();
                    continue;
                }
                if (!open && !intersects(ref)) {
                    nextSegmentReverse();
                    continue;
                }
                int count = countOf(open, ref);
                if (!open) {
                    touchRef(ref);
                }
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
                SegmentRef ref = open ? null : refOf(segmentId);
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
                    touchRef(ref);
                }
                boolean hasDelta = !open && delta.hasSegmentDelta(segmentId);
                while (offset < count) {
                    long rowId = RowId.ofSegment(segmentId, offset);
                    boolean deleted = open ? openDeletes.contains(rowId)
                            : refDeleted(ref, offset)
                                    || (hasDelta && delta.isSegmentRowDeleted(rowId));
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
                SegmentRef ref = open ? null : refOf(segmentId);
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
                    touchRef(ref);
                }
                if (!started) {
                    offset = count - 1;
                    started = true;
                }
                while (offset >= 0) {
                    long rowId = RowId.ofSegment(segmentId, offset);
                    boolean deleted = open ? openDeletes.contains(rowId)
                            : refDeleted(ref, offset)
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
        private int countOf(boolean open, SegmentRef ref) {
            int available;
            if (open) {
                available = openSegment.rowCount();
            } else if (ref.loaded != null) {
                available = ref.loaded.getRowCount();
            } else {
                available = ref.skeleton.rowCount;
            }
            return segmentId == lastSegmentId ? Math.min(available, openRowCount) : available;
        }

        /** 段是否可能与给定区间相交；已逐出的段用骨架里的 zone map。 */
        private boolean intersects(SegmentRef ref) {
            if (lower == null && upper == null) {
                return true;
            }
            // 只有未回写的更新新像会让段的 zone map 失真（重开后 WAL 重放把更新放回 delta，
            // 段文件与其中的 zone map 仍是合并前的；未重开时合并也会先改段后清 delta）。
            // 删除只会收窄值域，不改变 zone map 的有效性，纯删除段仍参与边界剪枝。
            if (delta.hasSegmentUpdates(ref.segmentId)) {
                return true;
            }
            ColumnSegment seg = ref.loaded;
            if (seg != null) {
                return ColumnarStore.intersects(seg, lower, upper, valueTypes.length);
            }
            return skeletonIntersects(ref.skeleton, lower, upper);
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

        /** 每段的 delta 屏蔽位版本，供部分聚合缓存判定条目是否仍有效。 */
        public final Map<Integer, Long> deltaVersions;

        VectorizedSnapshot(ColumnSegment[] segments,
                Map<Integer, BitSet> maskedBySegment, List<Value[]> extraRows,
                Map<Integer, Long> deltaVersions) {
            this.segments = segments;
            this.maskedBySegment = maskedBySegment;
            this.extraRows = extraRows;
            this.deltaVersions = deltaVersions;
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
        reloadEvictedForSnapshot(lower, upper);
        lock.readLock().lock();
        try {
            List<ColumnSegment> loadedSegments = new ArrayList<>(segments.size());
            for (SegmentRef ref : segments.values()) {
                ColumnSegment seg = ref.loaded;
                if (seg == null && ref.skeleton != null && refNeeded(ref, lower, upper)) {
                    // 预读后又被预算逐出：读一份仅供本次快照，不回写驻留状态。
                    seg = readSegmentTransient(ref);
                }
                if (seg != null) {
                    loadedSegments.add(seg);
                }
            }
            ColumnSegment[] segs = loadedSegments.toArray(new ColumnSegment[0]);
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
            List<Value[]> openRows = null;
            int openCount = openSegment.rowCount();
            if (openCount > 0) {
                openRows = new ArrayList<>(openCount);
                if (openDeletes.isEmpty()) {
                    for (int r = 0; r < openCount; r++) {
                        openRows.add(openSegment.rowValues(r));
                    }
                } else {
                    for (int r = 0; r < openCount; r++) {
                        if (!openDeletes.contains(RowId.ofSegment(nextSegmentId, r))) {
                            openRows.add(openSegment.rowValues(r));
                        }
                    }
                }
            }
            List<Value[]> extra = null;
            if (!delta.isEmpty()) {
                if (masked == null) {
                    masked = new HashMap<>();
                }
                for (Map.Entry<Integer, DeltaStore.SegmentDeleteBitmap> entry : delta.segmentDeleteEntries()) {
                    masked.computeIfAbsent(entry.getKey(), k -> new BitSet())
                            .or(entry.getValue().bits);
                }
                int[] rowsetPositions = delta.updatedPositions();
                for (int pos : rowsetPositions) {
                    long rowId = delta.updatedRowIdAt(pos);
                    masked.computeIfAbsent(RowId.segmentId(rowId), k -> new BitSet())
                            .set(RowId.offset(rowId));
                }
                // 新像有自己的 zone map：与查询区间不相交时整组跳过，零物化零求值。
                if (rowsetPositions.length > 0 && (lower != null || upper != null)
                        && !rowsetIntersects(lower, upper)) {
                    rowsetPositions = EMPTY_POSITIONS;
                }
                // 列向量只在锁内安全，这里在锁内把活像物化成不可变快照。
                for (int pos : rowsetPositions) {
                    if (extra == null) {
                        extra = new ArrayList<>();
                    }
                    extra.add(delta.updatedValueAt(pos));
                }
            }
            if (openRows != null) {
                if (extra == null) {
                    extra = openRows;
                } else {
                    extra.addAll(openRows);
                }
            }
            Map<Integer, Long> versions = new HashMap<>(segs.length * 2);
            for (ColumnSegment seg : segs) {
                versions.put(seg.getSegmentId(), delta.segmentVersion(seg.getSegmentId()));
            }
            // 剪枝后实际参与扫描的段记一次访问，供 LRU 逐出排序。
            for (ColumnSegment seg : segs) {
                SegmentRef ref = segments.get(seg.getSegmentId());
                if (ref != null) {
                    touchRef(ref);
                }
            }
            return new VectorizedSnapshot(
                    segs, masked != null ? masked : Map.of(), extra != null ? extra : List.of(),
                    versions);
        } finally {
            lock.readLock().unlock();
        }
    }

    /** 快照是否需要该段：无界时全要；有界时用骨架 zone map 判定，未回写更新的段必须保留。 */
    private boolean refNeeded(SegmentRef ref, Value[] lower, Value[] upper) {
        if (lower == null && upper == null) {
            return true;
        }
        return delta.hasSegmentUpdates(ref.segmentId)
                || skeletonIntersects(ref.skeleton, lower, upper);
    }

    /**
     * 逐出段进入向量化快照前先读回：向量化路径需要段内容，骨架只服务行号级读路径。
     * 剪枝判定用骨架里的 zone map，与驻留段同一条规则。
     */
    private void reloadEvictedForSnapshot(Value[] lower, Value[] upper) {
        List<SegmentRef> evicted = null;
        lock.readLock().lock();
        try {
            for (SegmentRef ref : segments.values()) {
                if (ref.loaded != null || ref.skeleton == null) {
                    continue;
                }
                boolean needed = (lower == null && upper == null)
                        || delta.hasSegmentUpdates(ref.segmentId)
                        || skeletonIntersects(ref.skeleton, lower, upper);
                if (needed) {
                    if (evicted == null) {
                        evicted = new ArrayList<>();
                    }
                    evicted.add(ref);
                }
            }
        } finally {
            lock.readLock().unlock();
        }
        if (evicted != null) {
            for (SegmentRef ref : evicted) {
                ensureResident(ref);
            }
        }
    }

    /** 骨架的 zone map 是否可能与给定区间相交。 */
    private boolean skeletonIntersects(SegmentSkeleton skeleton, Value[] lower, Value[] upper) {
        int columns = Math.min(valueTypes.length, skeleton.zoneMaps.length);
        for (int c = 0; c < columns; c++) {
            Value low = bound(lower, c);
            Value high = bound(upper, c);
            if ((low != null || high != null) && !skeleton.zoneMaps[c].mayIntersect(low, high)) {
                return false;
            }
        }
        return true;
    }

    private boolean rowsetIntersects(Value[] lower, Value[] upper) {
        for (int c = 0; c < valueTypes.length; c++) {
            Value low = lower == null ? null : lower[c];
            Value high = upper == null ? null : upper[c];
            if (low != null || high != null) {
                if (!delta.updatedZoneMap(c).mayIntersect(low, high)) {
                    return false;
                }
            }
        }
        return true;
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

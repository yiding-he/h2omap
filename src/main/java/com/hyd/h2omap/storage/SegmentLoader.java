/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.storage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.hyd.h2omap.persistence.OmapPersistence;
import com.hyd.h2omap.storage.ColumnarStore.SegmentRef;
import com.hyd.h2omap.storage.DeltaStore.SegmentDeleteBitmap;

/**
 * 后台段加载与发布：惰性加载的预载线程、按段号倒序的固定并发工人群，以及加载完成后的发布判定。
 *
 * <p>锁契约：发布（{@link #publishLoaded}）自行获取存储的写锁；预算逐出（{@code evictLocked}）
 * 要求调用方已持有写锁。工人群在锁外读段文件，只在发布时进锁。
 */
final class SegmentLoader {

    private final ColumnarStore store;

    private volatile Thread lazyLoader;

    private volatile boolean lazyLoaderStopped;

    private volatile Throwable lastLazyLoadError;

    SegmentLoader(ColumnarStore store) {
        this.store = store;
    }

    boolean isRunning() {
        return lazyLoader != null;
    }

    void start() {
        Thread thread = new Thread(this::loadAllSegments, "omap-lazy-loader");
        thread.setDaemon(true);
        lazyLoaderStopped = false;
        lazyLoader = thread;
        thread.start();
    }

    /** 停止后台加载并等待其退出。 */
    void stop() {
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
    // 后台段加载
    // ------------------------------------------------------------------

    private void loadAllSegments() {
        OmapPersistence p = store.persistence;
        if (p == null) {
            return;
        }
        SegmentRef[] refs;
        Map<Integer, List<Integer>> deletedOffsetsBySegment = new HashMap<>();
        store.lock.readLock().lock();
        try {
            refs = store.segments.values().toArray(new SegmentRef[0]);
            for (Map.Entry<Integer, DeltaStore.SegmentDeleteBitmap> entry : store.delta.segmentDeleteEntries()) {
                List<Integer> offsets = new ArrayList<>();
                for (int off = entry.getValue().bits.nextSetBit(0); off >= 0;
                        off = entry.getValue().bits.nextSetBit(off + 1)) {
                    offsets.add(off);
                }
                deletedOffsetsBySegment.put(entry.getKey(), offsets);
            }
        } finally {
            store.lock.readLock().unlock();
        }
        if (refs.length == 0) {
            return;
        }
        // 固定并发的工人群在加载线程池上按段号从新到老取段；
        // 预算停止条件由发布路径统一判定（见 publishLoaded 的逐出状态）。
        AtomicInteger cursor = new AtomicInteger(refs.length - 1);
        AtomicBoolean budgetBoundary = new AtomicBoolean();
        int workers = Math.min(OmapPersistence.LOAD_CONCURRENCY, refs.length);
        CountDownLatch done = new CountDownLatch(workers);
        for (int w = 0; w < workers; w++) {
            p.executeLoadTask(() -> {
                try {
                    runLazyLoadWorker(p, refs, cursor, budgetBoundary, deletedOffsetsBySegment);
                } finally {
                    done.countDown();
                }
            });
        }
        try {
            done.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void runLazyLoadWorker(OmapPersistence p, SegmentRef[] refs, AtomicInteger cursor,
            AtomicBoolean budgetBoundary, Map<Integer, List<Integer>> deletedOffsetsBySegment) {
        while (!lazyLoaderStopped && !budgetBoundary.get()) {
            long budget = store.getResidentBudget();
            if (budget > 0 && store.getResidentUsed() >= budget) {
                // 全库驻留预算已装满：继续预载只会载入即逐出，老段等查询按需读回。
                return;
            }
            int i = cursor.getAndDecrement();
            if (i < 0) {
                return;
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
            if (publishLoaded(ref, segment, deletedOffsetsBySegment.get(ref.segmentId))
                    == PUBLISH_EVICTED) {
                // 本次发布触发了预算逐出（含发布段自身被逐出）：预算装不下更多段，
                // 再预载都是无效读；并发下边界处最多多读 (并发数 - 1) 个装不下的段。
                // 全删段被丢弃时 ref.loaded 仍非空，不会误判。
                budgetBoundary.set(true);
                return;
            }
        }
    }

    /** publishLoaded 的返回状态：段已被并发读回加载（预载放弃该段）。 */
    private static final int PUBLISH_ALREADY = 0;
    /** publishLoaded 的返回状态：已发布且本次发布未触发预算逐出。 */
    private static final int PUBLISH_OK = 1;
    /** publishLoaded 的返回状态：已发布且本次发布触发了预算逐出（预算边界）。 */
    private static final int PUBLISH_EVICTED = 2;

    private int publishLoaded(SegmentRef ref, ColumnSegment segment, List<Integer> deletedOffsets) {
        int status;
        store.lock.writeLock().lock();
        try {
            if (ref.loaded != null) {
                status = PUBLISH_ALREADY;
            } else if (ref.skeleton != null) {
                // 骨架已在恢复时计入行数并保证可见性：这里只挂内容，不动可见性与结果缓存。
                ref.loaded = segment;
                ref.residentBytes = segment.getEstimatedMemory();
                store.residentAdd(ref.residentBytes);
                status = store.evictLocked() > 0 ? PUBLISH_EVICTED : PUBLISH_OK;
            } else {
                ref.loaded = segment;
                ref.residentBytes = segment.getEstimatedMemory();
                store.residentAdd(ref.residentBytes);
                long added = visibleRowsOf(segment);
                if (deletedOffsets != null) {
                    int rowCountOfSegment = segment.getRowCount();
                    for (int offset : deletedOffsets) {
                        if (offset < rowCountOfSegment && !segment.isDeleted(offset)) {
                            added--;
                        }
                    }
                }
                store.rowCount += Math.max(0, added);
                // 加载后该段可能整段已删：与写路径同一条丢弃判定。
                store.dropSegmentIfEmpty(ref.segmentId);
                store.bump();
                status = store.evictLocked() > 0 ? PUBLISH_EVICTED : PUBLISH_OK;
            }
        } finally {
            store.lock.writeLock().unlock();
        }
        Runnable listener = store.visibilityListener;
        if (listener != null) {
            listener.run();
        }
        return status;
    }

    static long visibleRowsOf(ColumnSegment segment) {
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
}

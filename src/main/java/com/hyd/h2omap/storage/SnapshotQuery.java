/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.h2.engine.SessionLocal;
import org.h2.value.Value;
import org.h2.value.ValueNull;

import com.hyd.h2omap.persistence.OmapPersistence;
import com.hyd.h2omap.stats.ZoneMap;
import com.hyd.h2omap.vector.ColumnVector;

/**
 * 向量化快照与裸 MIN/MAX 定界的构造逻辑，从 {@link ColumnarStore} 拆出。
 *
 * <p>本类读取存储的包级状态（段表、开放段、delta、锁、计数器），
 * 快照在调用方的读锁内构造，定界同样在调用方的读锁内扫描；锁语义与拆出前一致。
 */
final class SnapshotQuery {

    /** 空位置数组：rowset 被 zone map 跳过时用它替代。 */
    private static final int[] EMPTY_POSITIONS = new int[0];

    /** 与 extraRows 对齐的 rowId 数组；空集共用一个零长数组。 */
    private static long[] toIds(List<Long> ids) {
        if (ids == null || ids.isEmpty()) {
            return EMPTY_ROW_IDS;
        }
        long[] out = new long[ids.size()];
        for (int i = 0; i < out.length; i++) {
            out[i] = ids.get(i);
        }
        return out;
    }

    private static final long[] EMPTY_ROW_IDS = new long[0];

    private SnapshotQuery() {
    }

    /**
     * 向量化查询的一次性一致性快照，并按列值区间跳过不可能命中的段。
     *
     * @param store  存储实例
     * @param lower 每列下界，null 元素表示该列无下界；null 表示不剪枝
     * @param upper 每列上界，null 元素表示该列无上界；null 表示不剪枝
     * @param columns 部分列读的请求列集（列号升序），null 表示逐出段整段读回（旧语义）
     * @return 快照，{@code segments} 只含可能与区间相交的段
     */
    static VectorizedSnapshot take(ColumnarStore store, Value[] lower, Value[] upper, int[] columns) {
        store.lock.readLock().lock();
        try {
            List<ColumnSegment> loadedSegments = new ArrayList<>(store.segments.size());
            for (ColumnarStore.SegmentRef ref : store.segments.values()) {
                ColumnSegment seg = ref.loaded;
                if (seg == null && ref.skeleton != null && refNeeded(store, ref, lower, upper)) {
                    // 逐出段在拍快照时不读盘：用堆上骨架构造懒加载段，执行期首次读列时才触发磁盘读取。
                    ColumnarStore.SegmentRef target = ref;
                    ColumnarStore.SegmentSkeleton sk = ref.skeleton;
                    seg = ColumnSegment.lazy(ref.segmentId, ref.generation, sk.rowCount,
                            store.valueTypes.length, sk.zoneMaps, sk.deleted, sk.deletedCount,
                            store.compareMode, () -> loadSnapshotVectors(store, target, columns));
                }
                if (seg != null) {
                    loadedSegments.add(seg);
                }
            }
            ColumnSegment[] segs = loadedSegments.toArray(new ColumnSegment[0]);
            if (lower != null || upper != null) {
                segs = prune(store, segs, lower, upper);
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
            List<Long> openRowIds = null;
            int openCount = store.openSegment.rowCount();
            if (openCount > 0) {
                openRows = new ArrayList<>(openCount);
                openRowIds = new ArrayList<>(openCount);
                if (store.openDeletes.isEmpty()) {
                    for (int r = 0; r < openCount; r++) {
                        openRows.add(store.openSegment.rowValues(r));
                        openRowIds.add(RowId.ofSegment(store.nextSegmentId, r));
                    }
                } else {
                    for (int r = 0; r < openCount; r++) {
                        long rowId = RowId.ofSegment(store.nextSegmentId, r);
                        if (!store.openDeletes.contains(rowId)) {
                            openRows.add(store.openSegment.rowValues(r));
                            openRowIds.add(rowId);
                        }
                    }
                }
            }
            List<Value[]> extra = null;
            List<Long> extraIds = null;
            if (!store.delta.isEmpty()) {
                if (masked == null) {
                    masked = new HashMap<>();
                }
                for (Map.Entry<Integer, DeltaStore.SegmentDeleteBitmap> entry
                        : store.delta.segmentDeleteEntries()) {
                    masked.computeIfAbsent(entry.getKey(), k -> new BitSet())
                            .or(entry.getValue().bits);
                }
                int[] rowsetPositions = store.delta.updatedPositions();
                for (int pos : rowsetPositions) {
                    long rowId = store.delta.updatedRowIdAt(pos);
                    masked.computeIfAbsent(RowId.segmentId(rowId), k -> new BitSet())
                            .set(RowId.offset(rowId));
                }
                // 新像有自己的 zone map：与查询区间不相交时整组跳过，零物化零求值。
                if (rowsetPositions.length > 0 && (lower != null || upper != null)
                        && !rowsetIntersects(store, lower, upper)) {
                    rowsetPositions = EMPTY_POSITIONS;
                }
                // 列向量只在锁内安全，这里在锁内把活像物化成不可变快照。
                for (int pos : rowsetPositions) {
                    if (extra == null) {
                        extra = new ArrayList<>();
                        extraIds = new ArrayList<>();
                    }
                    extra.add(store.delta.updatedValueAt(pos));
                    extraIds.add(store.delta.updatedRowIdAt(pos));
                }
            }
            if (openRows != null) {
                if (extra == null) {
                    extra = openRows;
                    extraIds = openRowIds;
                } else {
                    extra.addAll(openRows);
                    extraIds.addAll(openRowIds);
                }
            }
            Map<Integer, Long> versions = new HashMap<>(segs.length * 2);
            for (ColumnSegment seg : segs) {
                versions.put(seg.getSegmentId(), store.delta.segmentVersion(seg.getSegmentId()));
            }
            // 剪枝后实际参与扫描的段记一次访问，供 LRU 逐出排序。
            // 懒加载快照读不进驻留模型：只触碰本来驻留的段，逐出段保持逐出。
            for (ColumnSegment seg : segs) {
                ColumnarStore.SegmentRef ref = store.segments.get(seg.getSegmentId());
                if (ref != null && ref.loaded != null) {
                    store.touchRef(ref);
                }
            }
            return new VectorizedSnapshot(
                    segs, masked != null ? masked : Map.of(), extra != null ? extra : List.of(),
                    toIds(extraIds), versions);
        } finally {
            store.lock.readLock().unlock();
        }
    }

    /** 快照是否需要该段：无界时全要；有界时用骨架 zone map 判定，未回写更新的段必须保留。 */
    private static boolean refNeeded(ColumnarStore store, ColumnarStore.SegmentRef ref,
            Value[] lower, Value[] upper) {
        if (lower == null && upper == null) {
            return true;
        }
        return store.delta.hasSegmentUpdates(ref.segmentId)
                || store.skeletonIntersects(ref.skeleton, lower, upper);
    }

    /** 懒加载快照段首次访问列向量时触发：按列目录切片读或整段临时读，不进入驻留模型。 */
    private static ColumnVector[] loadSnapshotVectors(ColumnarStore store,
            ColumnarStore.SegmentRef ref, int[] columns) {
        ColumnSegment seg = columns != null
                ? store.readSliceTransient(ref, columns)
                : store.readSegmentTransient(ref);
        if (seg == null) {
            ColumnVector[] empty = new ColumnVector[store.valueTypes.length];
            Arrays.fill(empty, AbsentColumnVector.INSTANCE);
            return empty;
        }
        int colCount = store.valueTypes.length;
        ColumnVector[] vectors = new ColumnVector[colCount];
        for (int c = 0; c < colCount; c++) {
            vectors[c] = seg.getColumn(c);
        }
        return vectors;
    }

    /** 裸 MIN/MAX 只对可比较标量列定界；数组、CLOB、几何等类型交回常规路径。 */
    private static boolean comparableForExtreme(int type) {
        switch (type) {
        case Value.TINYINT:
        case Value.SMALLINT:
        case Value.INTEGER:
        case Value.BIGINT:
        case Value.REAL:
        case Value.DOUBLE:
        case Value.NUMERIC:
        case Value.BOOLEAN:
        case Value.CHAR:
        case Value.VARCHAR:
        case Value.VARCHAR_IGNORECASE:
        case Value.DATE:
        case Value.TIME:
        case Value.TIME_TZ:
        case Value.TIMESTAMP:
        case Value.TIMESTAMP_TZ:
        case Value.UUID:
            return true;
        default:
            return false;
        }
    }

    /** 目标方向的候选段：zone 边界与当前最优值，边界为 null 的段不参与。 */
    private static final class ExtremeCandidate {

        final ColumnarStore.SegmentRef ref;

        final Value bound;

        ExtremeCandidate(ColumnarStore.SegmentRef ref, Value bound) {
            this.ref = ref;
            this.bound = bound;
        }
    }

    /**
     * 裸 MIN/MAX 的定界求值：段按目标方向的 zone 边界排序后依次扫描，
     * 当下一个段的边界已不优于当前最优值时停止——后续段的可见值不可能更优。
     *
     * <p>zone 边界是含已删行的下界（MIN 方向），因此不能一次性只扫全局边界最小的段：
     * 该段的最小行可能已被删，真实最优值会落到边界更大的段里。逐段推进在任何删除
     * 形态下都精确，时间递增数据通常扫一段即停。逐出段只读目标列，不进驻留模型。
     *
     * @return 目标列的最优可见值；全空或无行时为 null（与 H2 裸 MIN/MAX 语义一致）
     */
    static ExtremeValue extreme(ColumnarStore store, int columnId, boolean min,
            SessionLocal session) {
        if (columnId < 0 || columnId >= store.valueTypes.length
                || !comparableForExtreme(store.valueTypes[columnId])) {
            return ExtremeValue.unsupported();
        }
        store.lock.readLock().lock();
        try {
            List<ExtremeCandidate> candidates = new ArrayList<>(store.segments.size());
            for (ColumnarStore.SegmentRef ref : store.segments.values()) {
                ZoneMap zone = ref.loaded != null ? ref.loaded.getZoneMap(columnId)
                        : ref.skeleton != null ? ref.skeleton.zoneMaps[columnId] : null;
                if (zone == null) {
                    // 驻留与骨架都是 null 的瞬态不在既有不变式内，交回常规路径。
                    return ExtremeValue.unsupported();
                }
                Value bound = min ? zone.getMin() : zone.getMax();
                if (bound == null || bound == ValueNull.INSTANCE) {
                    // 全 null 列的段不贡献非空值；类型门控已排除不可比较列。
                    continue;
                }
                candidates.add(new ExtremeCandidate(ref, bound));
            }
            candidates.sort((x, y) -> {
                int cmp = x.bound.compareTo(y.bound, session, store.compareMode);
                return min ? cmp : -cmp;
            });
            Value best = null;
            for (ExtremeCandidate candidate : candidates) {
                if (best != null) {
                    int cmp = candidate.bound.compareTo(best, session, store.compareMode);
                    if (min ? cmp >= 0 : cmp <= 0) {
                        break;
                    }
                }
                best = scanSegmentExtreme(store, candidate.ref, columnId, min, best, session);
            }
            // delta 新像与开放段的行无条件参与，数量以合并阈值为上界。
            for (int pos : store.delta.updatedPositions()) {
                best = pickExtreme(store, store.delta.updatedValueAt(pos)[columnId], min, best, session);
            }
            int openCount = store.openSegment.rowCount();
            for (int r = 0; r < openCount; r++) {
                if (!store.openDeletes.contains(RowId.ofSegment(store.nextSegmentId, r))) {
                    best = pickExtreme(store, store.openSegment.rowValues(r)[columnId], min, best, session);
                }
            }
            return ExtremeValue.of(best);
        } finally {
            store.lock.readLock().unlock();
        }
    }

    private static Value pickExtreme(ColumnarStore store, Value v, boolean min, Value best,
            SessionLocal session) {
        if (v == null || v == ValueNull.INSTANCE) {
            return best;
        }
        if (best == null) {
            return v;
        }
        int cmp = v.compareTo(best, session, store.compareMode);
        return (min ? cmp < 0 : cmp > 0) ? v : best;
    }

    /** 扫描一个候选段的目标列可见行，逐出段只读该列；返回更新后的最优值。 */
    private static Value scanSegmentExtreme(ColumnarStore store, ColumnarStore.SegmentRef ref,
            int columnId, boolean min, Value best, SessionLocal session) {
        store.extremeScanned.incrementAndGet();
        ColumnSegment seg = ref.loaded;
        ColumnVector vec;
        int rowCount;
        ColumnSlice slice = null;
        if (seg != null) {
            vec = seg.getColumn(columnId);
            rowCount = seg.getRowCount();
        } else {
            OmapPersistence p = store.persistence;
            if (p == null || ref.skeleton == null) {
                throw new IllegalStateException(
                        "逐出段无读回通道: seg_" + ref.segmentId + "_" + ref.generation);
            }
            try {
                slice = p.readSegmentColumns(ref.segmentId, ref.generation, new int[] { columnId });
            } catch (IOException e) {
                throw new IllegalStateException(
                        "逐出段读回失败: seg_" + ref.segmentId + "_" + ref.generation, e);
            }
            vec = slice.getVector(columnId);
            rowCount = slice.getRowCount();
        }
        boolean hasDelta = store.delta.hasSegmentDelta(ref.segmentId);
        for (int r = 0; r < rowCount; r++) {
            if (seg != null ? seg.isDeleted(r) : slice.isDeleted(r)) {
                continue;
            }
            if (hasDelta) {
                long rowId = RowId.ofSegment(ref.segmentId, r);
                if (store.delta.isSegmentRowDeleted(rowId) || store.delta.getUpdated(rowId) != null) {
                    continue;
                }
            }
            best = pickExtreme(store, vec.get(r), min, best, session);
        }
        return best;
    }

    private static boolean rowsetIntersects(ColumnarStore store, Value[] lower, Value[] upper) {
        for (int c = 0; c < store.valueTypes.length; c++) {
            Value low = lower == null ? null : lower[c];
            Value high = upper == null ? null : upper[c];
            if (low != null || high != null) {
                if (!store.delta.updatedZoneMap(c).mayIntersect(low, high)) {
                    return false;
                }
            }
        }
        return true;
    }

    private static ColumnSegment[] prune(ColumnarStore store, ColumnSegment[] segs,
            Value[] lower, Value[] upper) {
        int kept = 0;
        ColumnSegment[] result = new ColumnSegment[segs.length];
        for (ColumnSegment segment : segs) {
            if (ColumnarStore.intersects(segment, lower, upper, store.valueTypes.length)) {
                result[kept++] = segment;
            }
        }
        store.prunedSegments.addAndGet(segs.length - kept);
        return kept == segs.length ? segs : Arrays.copyOf(result, kept);
    }
}

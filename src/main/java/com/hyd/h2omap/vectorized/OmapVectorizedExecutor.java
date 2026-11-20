/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.vectorized;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeSet;

import org.h2.command.query.QueryOrderBy;
import org.h2.engine.SessionLocal;
import org.h2.expression.BinaryOperation;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.H2ExpressionAccess;
import org.h2.expression.aggregate.Aggregate;
import org.h2.expression.aggregate.AggregateType;
import org.h2.expression.aggregate.H2AggregateAccess;
import org.h2.expression.condition.Comparison;
import org.h2.expression.condition.ConditionAndOr;
import org.h2.expression.condition.ConditionAndOrN;
import org.h2.expression.condition.H2ConditionAccess;
import org.h2.result.LocalResult;
import org.h2.result.SortOrder;
import org.h2.table.TableFilter;
import org.h2.value.CompareMode;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueBigint;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueDouble;
import org.h2.value.ValueNull;
import org.h2.value.ValueNumeric;

import com.hyd.h2omap.index.OmapScanIndex;
import com.hyd.h2omap.stats.ZoneMap;
import com.hyd.h2omap.storage.ColumnSegment;
import com.hyd.h2omap.storage.ColumnarStore;
import com.hyd.h2omap.storage.RowId;
import com.hyd.h2omap.table.OmapTable;
import com.hyd.h2omap.vector.ColumnVector;
import com.hyd.h2omap.vector.DictValueVector;
import com.hyd.h2omap.vector.DictVarcharVector;
import com.hyd.h2omap.vector.DoubleVector;
import com.hyd.h2omap.vector.IntVector;
import com.hyd.h2omap.vector.LongVector;
import com.hyd.h2omap.vector.NullBitmap;

/**
 * 单表过滤与聚合的向量化执行器。
 *
 * <p>直接读取 {@link ColumnarStore.VectorizedSnapshot} 中的不可变列段原始数组，
 * 在完全无锁的情况下完成 Selection Vector 过滤与无装箱聚合。遇到不支持的表达式时返回
 * {@code false}，交由 H2 原 {@code queryGroup} 无缝兜底。
 */
public final class OmapVectorizedExecutor {

    private OmapVectorizedExecutor() {
    }

    public static boolean tryExecuteFlat(
            SessionLocal session,
            TableFilter tableFilter,
            Expression condition,
            List<Expression> expressions,
            int resultColumnCount,
            long maxRows,
            SortOrder sortOrder,
            LocalResult result) {
        if (!(tableFilter.getTable() instanceof OmapTable)) {
            return false;
        }
        // 主键/唯一索引点查或范围查找走 OmapRowIdIndex 更快，不走全表向量化扫描
        if (tableFilter.getIndex() != null && !(tableFilter.getIndex() instanceof OmapScanIndex)) {
            return false;
        }
        if (expressions == null || expressions.isEmpty()) {
            return false;
        }

        int outLen = Math.max(resultColumnCount, expressions.size());
        int[] projCols = new int[outLen];
        Value[] projConsts = new Value[outLen];
        for (int i = 0; i < outLen; i++) {
            Expression e = i < expressions.size() ? expressions.get(i) : null;
            if (e == null) {
                projCols[i] = -1;
                projConsts[i] = ValueNull.INSTANCE;
                continue;
            }
            if (e instanceof ExpressionColumn) {
                ExpressionColumn ec = (ExpressionColumn) e;
                if (ec.getTableFilter() != tableFilter) {
                    return false;
                }
                projCols[i] = ec.getColumn().getColumnId();
            } else if (e.isConstant()) {
                projCols[i] = -1;
                projConsts[i] = e.getValue(session);
            } else {
                return false;
            }
        }

        VectorizedFilter filter = null;
        if (condition != null) {
            filter = VectorizedFilter.compile(condition, tableFilter, session);
            if (filter == null) {
                return false;
            }
        }

        if (maxRows == 0) {
            return true;
        }

        OmapTable table = (OmapTable) tableFilter.getTable();
        CompareMode compareMode = table.getCompareMode();
        long emitted = 0;

        // 统计被引用到的非重复列，只物化这些列向量
        int[] uniqueProjCols = new int[outLen];
        int uniqueCount = 0;
        int[] projToUnique = new int[outLen];
        for (int i = 0; i < outLen; i++) {
            int col = projCols[i];
            if (col < 0) {
                projToUnique[i] = -1;
                continue;
            }
            int found = -1;
            for (int u = 0; u < uniqueCount; u++) {
                if (uniqueProjCols[u] == col) {
                    found = u;
                    break;
                }
            }
            if (found < 0) {
                found = uniqueCount;
                uniqueProjCols[uniqueCount++] = col;
            }
            projToUnique[i] = found;
        }
        ColumnVector[] colVecs = new ColumnVector[uniqueCount];

        // 有序模式的排序键：预解析进请求列集；形态不合法时 executeOrdered 会自行回退。
        int preSortCol = -1;
        if (sortOrder != null) {
            List<QueryOrderBy> orderList = sortOrder.getOrderList();
            int[] sortTypes = sortOrder.getSortTypesWithNullOrdering();
            if (orderList != null && orderList.size() == 1 && sortTypes != null && sortTypes.length == 1) {
                preSortCol = VectorizedFilter.resolveColumn(orderList.get(0).expression, tableFilter);
            }
        }

        // 部分列读：逐出段只读投影列与谓词列（有序模式另含排序键）。
        int[] snapshotColumns = unionColumns(
                Arrays.copyOf(uniqueProjCols, uniqueCount),
                new int[] { preSortCol },
                filter != null ? filter.referencedColumns() : null);
        ColumnarStore.VectorizedSnapshot snap = takeSnapshot(table, filter, tableFilter, snapshotColumns);

        if (sortOrder != null) {
            return executeOrdered(session, snap, filter, sortOrder, tableFilter, projCols, projConsts,
                    projToUnique, uniqueProjCols, uniqueCount, outLen, maxRows, table.getStore(), result,
                    compareMode);
        }

        // 无 ORDER BY 时返回行序无保证：先扫已驻留段与开放段/新像，LIMIT 不够才按需读回逐出段。
        List<ColumnSegment> residentSegs = new ArrayList<>();
        List<ColumnSegment> lazySegs = new ArrayList<>();
        for (ColumnSegment seg : snap.segments) {
            (seg.isLoaded() ? residentSegs : lazySegs).add(seg);
        }
        emitted += scanFlatSegments(residentSegs, snap, filter, session, compareMode, outLen,
                projConsts, projToUnique, uniqueProjCols, uniqueCount, maxRows, emitted, result);
        if (maxRows > 0 && emitted >= maxRows) {
            return true;
        }

        for (Value[] srcRow : snap.extraRows) {
            if (filter == null || filter.evalRow(srcRow, session, compareMode)) {
                Value[] row = new Value[outLen];
                for (int c = 0; c < outLen; c++) {
                    int col = projCols[c];
                    row[c] = col >= 0 ? srcRow[col] : projConsts[c];
                }
                result.addRow(row);
                emitted++;
                if (maxRows > 0 && emitted >= maxRows) {
                    return true;
                }
            }
        }
        if (maxRows > 0 && emitted >= maxRows) {
            return true;
        }
        scanFlatSegments(lazySegs, snap, filter, session, compareMode, outLen,
                projConsts, projToUnique, uniqueProjCols, uniqueCount, maxRows, emitted, result);
        return true;
    }

    /**
     * 扫描一组段并把命中行写进结果集，写满允许额度即停；返回本次写出的行数。
     * 首次访问懒段的列向量时才触发按需读盘。
     */
    private static long scanFlatSegments(List<ColumnSegment> segs,
            ColumnarStore.VectorizedSnapshot snap, VectorizedFilter filter, SessionLocal session,
            CompareMode compareMode, int outLen, Value[] projConsts,
            int[] projToUnique, int[] uniqueProjCols, int uniqueCount, long maxRows, long emitted,
            LocalResult result) {
        long allowed = maxRows > 0 ? maxRows : Long.MAX_VALUE;
        long written = 0;
        int[] sel = null;
        ColumnVector[] colVecs = new ColumnVector[uniqueCount];
        for (ColumnSegment seg : segs) {
            int rowCount = seg.getRowCount();
            if (rowCount == 0) {
                continue;
            }
            BitSet masked = snap.maskedBySegment.get(seg.getSegmentId());
            if (masked != null && masked.cardinality() >= rowCount) {
                continue;
            }

            if (filter == null) {
                for (int u = 0; u < uniqueCount; u++) {
                    colVecs[u] = seg.getColumn(uniqueProjCols[u]);
                }
                for (int i = 0; i < rowCount; i++) {
                    if (masked != null && masked.get(i)) {
                        continue;
                    }
                    Value[] row = new Value[outLen];
                    for (int c = 0; c < outLen; c++) {
                        int u = projToUnique[c];
                        row[c] = u >= 0 ? colVecs[u].get(i) : projConsts[c];
                    }
                    result.addRow(row);
                    written++;
                    if (emitted + written >= allowed) {
                        return written;
                    }
                }
                continue;
            }

            if (sel == null || sel.length < rowCount) {
                sel = new int[rowCount];
            }
            int selCount = filter.filterSegment(seg, sel, masked, session, compareMode);
            if (selCount == 0) {
                continue;
            }
            for (int u = 0; u < uniqueCount; u++) {
                colVecs[u] = seg.getColumn(uniqueProjCols[u]);
            }
            for (int k = 0; k < selCount; k++) {
                int idx = sel[k];
                Value[] row = new Value[outLen];
                for (int c = 0; c < outLen; c++) {
                    int u = projToUnique[c];
                    row[c] = u >= 0 ? colVecs[u].get(idx) : projConsts[c];
                }
                result.addRow(row);
                written++;
                if (emitted + written >= allowed) {
                    return written;
                }
            }
        }
        return written;
    }

    /** 有序模式下允许的最大 LIMIT（含 OFFSET）；超出时交回 H2，不为巨大 LIMIT 建堆。 */
    private static final int ORDERED_MAX_ROWS = 10_000;

    /** 有序模式下堆里的一行：排序键、rowId（并列时定序）与投影后的行。 */
    private static final class OrderedEntry {

        final Value key;

        final long rowId;

        final Value[] row;

        OrderedEntry(Value key, long rowId, Value[] row) {
            this.key = key;
            this.rowId = rowId;
            this.row = row;
        }
    }

    /**
     * 有序模式：{@code ORDER BY 单列 + LIMIT}。
     *
     * <p>段按排序列在 zone map 上的边界排序后依次扫描，用容量为 {@code limit + offset} 的堆
     * 留下前 N 行。堆满时，若下一个段内最好的一行也不优于堆内第 N 名，则整段跳过。
     * 物理聚簇与排序键同向时只需扫一两段，结果与全排序一致；同向不成立时也能得到
     * 正确结果，只是跳过不了段。
     */
    private static boolean executeOrdered(SessionLocal session, ColumnarStore.VectorizedSnapshot snap,
            VectorizedFilter filter, SortOrder sortOrder, TableFilter tableFilter, int[] projCols,
            Value[] projConsts, int[] projToUnique, int[] uniqueProjCols, int uniqueCount, int outLen,
            long maxRows, ColumnarStore store, LocalResult result, CompareMode compareMode) {
        if (maxRows <= 0 || maxRows > ORDERED_MAX_ROWS) {
            return false;
        }
        List<QueryOrderBy> orderList = sortOrder.getOrderList();
        int[] sortTypes = sortOrder.getSortTypesWithNullOrdering();
        if (orderList == null || orderList.size() != 1 || sortTypes == null || sortTypes.length != 1) {
            return false;
        }
        int sortCol = VectorizedFilter.resolveColumn(orderList.get(0).expression, tableFilter);
        if (sortCol < 0) {
            return false;
        }
        boolean descending = (sortTypes[0] & SortOrder.DESCENDING) != 0;
        boolean nullsFirst = (sortTypes[0] & SortOrder.NULLS_FIRST) != 0;

        Comparator<OrderedEntry> order = (x, y) -> {
            int cmp = compareSort(x.key, y.key, session, compareMode, descending, nullsFirst);
            return cmp != 0 ? cmp : Long.compare(x.rowId, y.rowId);
        };
        int capacity = (int) maxRows;
        PriorityQueue<OrderedEntry> heap = new PriorityQueue<>(Math.min(capacity, 1024), order.reversed());

        ColumnSegment[] ordered = orderedSegments(snap.segments, sortCol, descending, session, compareMode);
        ColumnVector[] colVecs = new ColumnVector[uniqueCount];
        int[] sel = null;
        for (ColumnSegment seg : ordered) {
            int rowCount = seg.getRowCount();
            if (rowCount == 0) {
                continue;
            }
            if (heap.size() >= capacity
                    && canSkipSegment(seg, sortCol, heap.peek().key, descending, nullsFirst,
                            session, compareMode)) {
                store.recordOrderedSkip();
                continue;
            }
            BitSet masked = snap.maskedBySegment.get(seg.getSegmentId());
            if (masked != null && masked.cardinality() >= rowCount) {
                continue;
            }
            if (sel == null || sel.length < rowCount) {
                sel = new int[rowCount];
            }
            int selCount = filter == null ? fillAll(sel, rowCount, masked)
                    : filter.filterSegment(seg, sel, masked, session, compareMode);
            if (selCount == 0) {
                continue;
            }
            for (int u = 0; u < uniqueCount; u++) {
                colVecs[u] = seg.getColumn(uniqueProjCols[u]);
            }
            ColumnVector sortVec = seg.getColumn(sortCol);
            for (int k = 0; k < selCount; k++) {
                int idx = sel[k];
                Value[] row = new Value[outLen];
                for (int c = 0; c < outLen; c++) {
                    int u = projToUnique[c];
                    row[c] = u >= 0 ? colVecs[u].get(idx) : projConsts[c];
                }
                push(heap, capacity, order,
                        new OrderedEntry(sortVec.get(idx), RowId.ofSegment(seg.getSegmentId(), idx), row));
            }
        }

        for (Value[] srcRow : snap.extraRows) {
            if (filter != null && !filter.evalRow(srcRow, session, compareMode)) {
                continue;
            }
            Value[] row = new Value[outLen];
            for (int c = 0; c < outLen; c++) {
                int col = projCols[c];
                row[c] = col >= 0 ? srcRow[col] : projConsts[c];
            }
            push(heap, capacity, order, new OrderedEntry(srcRow[sortCol], srcRow[0].getLong(), row));
        }

        OrderedEntry[] entries = heap.toArray(new OrderedEntry[0]);
        Arrays.sort(entries, order);
        for (OrderedEntry entry : entries) {
            result.addRow(entry.row);
        }
        return true;
    }

    private static void push(PriorityQueue<OrderedEntry> heap, int capacity,
            Comparator<OrderedEntry> order, OrderedEntry entry) {
        if (heap.size() < capacity) {
            heap.add(entry);
        } else if (order.compare(entry, heap.peek()) < 0) {
            heap.poll();
            heap.add(entry);
        }
    }

    private static int fillAll(int[] sel, int rowCount, BitSet masked) {
        if (masked == null) {
            for (int i = 0; i < rowCount; i++) {
                sel[i] = i;
            }
            return rowCount;
        }
        int n = 0;
        for (int i = 0; i < rowCount; i++) {
            if (!masked.get(i)) {
                sel[n++] = i;
            }
        }
        return n;
    }

    /**
     * 堆已满时，本段能否整段跳过。判定取保守侧：只有能确定“段内最好的一行也不优于第 N 名”才跳。
     */
    private static boolean canSkipSegment(ColumnSegment segment, int sortCol, Value kth,
            boolean descending, boolean nullsFirst, SessionLocal session, CompareMode compareMode) {
        if (kth == null) {
            return false;
        }
        boolean kthIsNull = kth == ValueNull.INSTANCE;
        if (segment.getColumn(sortCol).nulls().hasNull() && nullsFirst != kthIsNull) {
            // 段内含 NULL，且 NULL 的排序位置优于当前第 N 名，本段可能贡献更好的行
            return false;
        }
        ZoneMap zoneMap = segment.getZoneMap(sortCol);
        Value bound = descending ? zoneMap.getMax() : zoneMap.getMin();
        if (bound == null || bound == ValueNull.INSTANCE) {
            return false;
        }
        return compareSort(bound, kth, session, compareMode, descending, nullsFirst) > 0;
    }

    /** 按排序列在 zone map 上的边界排序段，边界缺失（全 NULL 或不可比较）的段排在最后。 */
    private static ColumnSegment[] orderedSegments(ColumnSegment[] segments, int sortCol,
            boolean descending, SessionLocal session, CompareMode compareMode) {
        ColumnSegment[] copy = segments.clone();
        Arrays.sort(copy, (x, y) -> {
            Value vx = segmentBound(x, sortCol, descending);
            Value vy = segmentBound(y, sortCol, descending);
            if (vx == null) {
                return vy == null ? 0 : 1;
            }
            if (vy == null) {
                return -1;
            }
            return compareSort(vx, vy, session, compareMode, descending, false);
        });
        return copy;
    }

    private static Value segmentBound(ColumnSegment segment, int sortCol, boolean descending) {
        ZoneMap zoneMap = segment.getZoneMap(sortCol);
        Value bound = descending ? zoneMap.getMax() : zoneMap.getMin();
        return bound == ValueNull.INSTANCE ? null : bound;
    }

    /** 按排序列比较两个值；返回负数表示 {@code a} 在结果中更靠前。 */
    private static int compareSort(Value a, Value b, SessionLocal session, CompareMode compareMode,
            boolean descending, boolean nullsFirst) {
        boolean an = a == null || a == ValueNull.INSTANCE;
        boolean bn = b == null || b == ValueNull.INSTANCE;
        if (an || bn) {
            if (an && bn) {
                return 0;
            }
            return an == nullsFirst ? -1 : 1;
        }
        int cmp = a.compareTo(b, session, compareMode);
        return descending ? -cmp : cmp;
    }

    public static boolean tryExecuteGroup(
            SessionLocal session,
            TableFilter tableFilter,
            Expression condition,
            Expression having,
            int havingIndex,
            List<Expression> expressions,
            int resultColumnCount,
            int[] groupIndex,
            boolean[] groupByExpression,
            int[] groupByCopies,
            LocalResult result) {
        if (!(tableFilter.getTable() instanceof OmapTable)) {
            return false;
        }

        if (qualifyIndexCheck(expressions, resultColumnCount, havingIndex)) {
            return false;
        }

        VectorizedFilter filter = null;
        if (condition != null) {
            filter = VectorizedFilter.compile(condition, tableFilter, session);
            if (filter == null) {
                return false;
            }
        }

        int groupCount = groupIndex != null ? groupIndex.length : 0;
        int[] groupKeyCols = new int[groupCount];
        int[] groupResultIdxs = new int[groupCount];
        for (int g = 0; g < groupCount; g++) {
            int rIdx = groupIndex[g];
            groupResultIdxs[g] = rIdx;
            Expression gExpr = expressions.get(rIdx).getNonAliasExpression();
            if (!(gExpr instanceof ExpressionColumn)) {
                return false;
            }
            ExpressionColumn ec = (ExpressionColumn) gExpr;
            if (ec.getTableFilter() != tableFilter) {
                return false;
            }
            groupKeyCols[g] = ec.getColumn().getColumnId();
        }

        List<AggSpec> aggList = new ArrayList<>(resultColumnCount);
        for (int i = 0; i < resultColumnCount; i++) {
            if (groupByExpression != null && groupByExpression[i]) {
                continue;
            }
            if (groupByCopies != null && groupByCopies[i] >= 0) {
                continue;
            }
            Expression expr = expressions.get(i).getNonAliasExpression();
            if (!(expr instanceof Aggregate)) {
                return false;
            }
            Aggregate agg = (Aggregate) expr;
            AggSpec spec = parseAggregate(agg, tableFilter, i, session);
            if (spec == null) {
                return false;
            }
            aggList.add(spec);
        }

        Expression havingExpr = having != null ? having
                : (havingIndex >= 0 && havingIndex < expressions.size() ? expressions.get(havingIndex) : null);
        HavingPredicate havingPred = null;
        if (havingExpr != null) {
            havingPred = compileHaving(havingExpr, tableFilter, groupKeyCols, aggList, session);
            if (havingPred == null) {
                return false;
            }
        }

        AggSpec[] aggSpecs = aggList.toArray(new AggSpec[0]);
        int aggCount = aggSpecs.length;

        OmapTable table = (OmapTable) tableFilter.getTable();
        CompareMode compareMode = table.getCompareMode();

        // 段级部分聚合缓存：多列分组暂不参与；含 DISTINCT 聚合或无签名形态时不缓存。
        String cacheSig = groupCount <= 1
                ? buildCacheSignature(filter, groupKeyCols, groupCount, aggSpecs)
                : null;
        SegmentAggCache aggCache = cacheSig != null ? table.getStore().getAggCache() : null;

        // 裸 MIN/MAX 单列无谓词：骨架 zone map 定界求值，只扫可能持有最优值的段，
        // 逐出段仅读目标列；列类型或瞬态不支持时交回下方常规向量化路径。
        if (filter == null && havingPred == null && groupCount == 0 && aggCount == 1) {
            AggSpec spec = aggSpecs[0];
            if (!spec.distinct && spec.arithOp == null
                    && (spec.type == AggregateType.MIN || spec.type == AggregateType.MAX)) {
                ColumnarStore.ExtremeValue extreme = table.getStore()
                        .extremeColumn(spec.col, spec.type == AggregateType.MIN, session);
                if (extreme.supported) {
                    Value[] out = new Value[resultColumnCount];
                    if (spec.outIdx >= 0 && spec.outIdx < resultColumnCount) {
                        out[spec.outIdx] = extreme.value != null
                                ? extreme.value : ValueNull.INSTANCE;
                    }
                    result.addRow(out);
                    return true;
                }
            }
        }

        // 部分列读：逐出段只读分组键、聚合参数列与谓词列。
        int[] aggColumns = new int[aggCount * 2];
        for (int a = 0; a < aggCount; a++) {
            aggColumns[a * 2] = aggSpecs[a].col;
            aggColumns[a * 2 + 1] = aggSpecs[a].rightCol;
        }
        int[] snapshotColumns = unionColumns(groupKeyCols, aggColumns,
                filter != null ? filter.referencedColumns() : null);
        ColumnarStore.VectorizedSnapshot snap = takeSnapshot(table, filter, tableFilter, snapshotColumns);

        if (groupCount == 0) {
            executeGlobal(snap, filter, havingPred, aggSpecs, aggCount, resultColumnCount,
                    result, session, compareMode, aggCache, cacheSig);
        } else if (groupCount == 1) {
            executeSingleGroup(snap, filter, havingPred, groupKeyCols[0], groupResultIdxs[0],
                    groupByCopies, aggSpecs, aggCount, resultColumnCount, result, session, compareMode,
                    aggCache, cacheSig);
        } else {
            executeMultiGroup(snap, filter, havingPred, groupKeyCols, groupResultIdxs,
                    groupByCopies, aggSpecs, aggCount, resultColumnCount, result, session, compareMode);
        }
        return true;
    }

    /**
     * 取向量化快照，并把过滤条件给出的列值区间交给存储层按段剪枝；
     * 逐出段按请求列集做部分列读，不进驻留模型。
     *
     * <p>区间只由顶层 {@code AND} 的合取项给出，与合取项的排列顺序无关，
     * 所以不可能命中的段在任何谓词开始求值之前就被跳过。
     */
    private static ColumnarStore.VectorizedSnapshot takeSnapshot(OmapTable table, VectorizedFilter filter,
            TableFilter tableFilter, int[] columns) {
        ColumnarStore store = table.getStore();
        if (filter == null) {
            return store.takeVectorizedSnapshot(null, null, columns);
        }
        Value[][] bounds = filter.segmentBounds(tableFilter.getTable().getColumns().length);
        return store.takeVectorizedSnapshot(bounds[0], bounds[1], columns);
    }

    /** 部分列读的请求列集：并集升序，负值（常量或无排序键）忽略。 */
    private static int[] unionColumns(int[]... sets) {
        TreeSet<Integer> all = new TreeSet<>();
        for (int[] set : sets) {
            if (set != null) {
                for (int c : set) {
                    if (c >= 0) {
                        all.add(c);
                    }
                }
            }
        }
        int[] result = new int[all.size()];
        int i = 0;
        for (int c : all) {
            result[i++] = c;
        }
        return result;
    }

    private static boolean qualifyIndexCheck(List<Expression> expressions, int resultColumnCount, int havingIndex) {
        int expectedExtra = havingIndex >= 0 ? 1 : 0;
        return expressions.size() > resultColumnCount + expectedExtra;
    }

    private static AggSpec parseAggregate(Aggregate agg, TableFilter filter, int outIdx, SessionLocal session) {
        AggregateType type = agg.getAggregateType();
        Expression[] args = H2AggregateAccess.getArgs(agg);
        boolean distinct = agg.isDistinct();
        switch (type) {
        case COUNT_ALL:
            return distinct ? null : new AggSpec(type, -1, outIdx, agg.getType(), false);
        case AVG: {
            int rt = agg.getType().getValueType();
            if (rt != Value.DOUBLE && rt != Value.NUMERIC) {
                return null;
            }
            // fall through
        }
        case COUNT:
        case SUM:
        case MIN:
        case MAX: {
            if (distinct && type != AggregateType.COUNT) {
                return null;
            }
            if (args.length != 1) {
                return null;
            }
            Expression arg = args[0].getNonAliasExpression();
            if (arg instanceof ExpressionColumn) {
                ExpressionColumn ec = (ExpressionColumn) arg;
                if (ec.getTableFilter() != filter) {
                    return null;
                }
                return new AggSpec(type, ec.getColumn().getColumnId(), outIdx, agg.getType(), distinct);
            }
            if (!distinct && arg instanceof BinaryOperation) {
                return parseArithAggregate(type, (BinaryOperation) arg, filter, outIdx, agg.getType(), session);
            }
            return null;
        }
        default:
            return null;
        }
    }

    private static AggSpec parseArithAggregate(AggregateType type, BinaryOperation bin, TableFilter filter,
            int outIdx, TypeInfo resultType, SessionLocal session) {
        BinaryOperation.OpType op = bin.getOperationType();
        if (op != BinaryOperation.OpType.PLUS
                && op != BinaryOperation.OpType.MINUS
                && op != BinaryOperation.OpType.MULTIPLY) {
            return null;
        }
        Expression left = H2ExpressionAccess.getLeft(bin);
        Expression right = H2ExpressionAccess.getRight(bin);
        if (left instanceof ExpressionColumn && right.isConstant()) {
            ExpressionColumn ec = (ExpressionColumn) left;
            if (ec.getTableFilter() != filter || !isNumericType(ec.getType().getValueType())) {
                return null;
            }
            Value c = right.getValue(session);
            if (c == null || c == ValueNull.INSTANCE || !isNumericType(c.getValueType())) {
                return null;
            }
            return new AggSpec(type, ec.getColumn().getColumnId(), -1, op, true, c, outIdx, resultType);
        }
        if (right instanceof ExpressionColumn && left.isConstant()) {
            ExpressionColumn ec = (ExpressionColumn) right;
            if (ec.getTableFilter() != filter || !isNumericType(ec.getType().getValueType())) {
                return null;
            }
            Value c = left.getValue(session);
            if (c == null || c == ValueNull.INSTANCE || !isNumericType(c.getValueType())) {
                return null;
            }
            return new AggSpec(type, ec.getColumn().getColumnId(), -1, op, false, c, outIdx, resultType);
        }
        if (left instanceof ExpressionColumn && right instanceof ExpressionColumn) {
            ExpressionColumn lc = (ExpressionColumn) left;
            ExpressionColumn rc = (ExpressionColumn) right;
            if (lc.getTableFilter() != filter || rc.getTableFilter() != filter) {
                return null;
            }
            if (!isNumericType(lc.getType().getValueType()) || !isNumericType(rc.getType().getValueType())) {
                return null;
            }
            return new AggSpec(type, lc.getColumn().getColumnId(), rc.getColumn().getColumnId(),
                    op, true, null, outIdx, resultType);
        }
        return null;
    }

    private static boolean isNumericType(int valueType) {
        return valueType == Value.TINYINT || valueType == Value.SMALLINT
                || valueType == Value.INTEGER || valueType == Value.BIGINT
                || valueType == Value.REAL || valueType == Value.DOUBLE;
    }

    private static void executeGlobal(
            ColumnarStore.VectorizedSnapshot snap,
            VectorizedFilter filter,
            HavingPredicate havingPred,
            AggSpec[] specs,
            int aggCount,
            int resultColumnCount,
            LocalResult result,
            SessionLocal session,
            CompareMode compareMode,
            SegmentAggCache aggCache,
            String cacheSig) {
        AggState[] states = new AggState[aggCount];
        for (int a = 0; a < aggCount; a++) {
            states[a] = new AggState(specs[a]);
        }

        int[] sel = null;
        for (ColumnSegment seg : snap.segments) {
            int segCount = seg.getRowCount();
            if (segCount == 0) {
                continue;
            }
            int segId = seg.getSegmentId();
            long segVer = snap.deltaVersions.getOrDefault(segId, 0L);
            AggState[] partial = null;
            if (aggCache != null) {
                SegmentAggCache.Entry cached = aggCache.lookup(cacheSig, segId,
                        seg.getGeneration(), segVer);
                if (cached != null) {
                    partial = (AggState[]) cached.partial;
                }
            }
            if (partial == null) {
                partial = newStates(specs, aggCount);
                BitSet masked = snap.maskedBySegment.get(segId);
                if (filter == null && (masked == null || masked.isEmpty())) {
                    for (int a = 0; a < aggCount; a++) {
                        partial[a].accumulateSegmentFull(seg, segCount, session, compareMode);
                    }
                } else {
                    if (sel == null || sel.length < segCount) {
                        sel = new int[segCount];
                    }
                    int selCount;
                    if (filter != null) {
                        selCount = filter.filterSegment(seg, sel, masked, session, compareMode);
                    } else {
                        selCount = 0;
                        for (int i = 0; i < segCount; i++) {
                            if (!masked.get(i)) {
                                sel[selCount++] = i;
                            }
                        }
                    }
                    if (selCount > 0) {
                        for (int a = 0; a < aggCount; a++) {
                            partial[a].accumulateSegmentSel(seg, sel, selCount, session, compareMode);
                        }
                    }
                }
                if (aggCache != null) {
                    aggCache.store(cacheSig, segId, seg.getGeneration(), segVer, partial);
                }
            }
            for (int a = 0; a < aggCount; a++) {
                states[a].merge(partial[a], session, compareMode);
            }
        }

        for (Value[] row : snap.extraRows) {
            if (filter == null || filter.evalRow(row, session, compareMode)) {
                for (int a = 0; a < aggCount; a++) {
                    states[a].accumulateRow(row, session, compareMode);
                }
            }
        }

        Value[] aggVals = new Value[aggCount];
        for (int a = 0; a < aggCount; a++) {
            aggVals[a] = states[a].result(session);
        }
        if (havingPred != null && !havingPred.eval(null, aggVals, session, compareMode)) {
            return;
        }
        Value[] out = new Value[resultColumnCount];
        for (int a = 0; a < aggCount; a++) {
            int outIdx = specs[a].outIdx;
            if (outIdx >= 0 && outIdx < resultColumnCount) {
                out[outIdx] = aggVals[a];
            }
        }
        result.addRow(out);
    }

    private static void executeSingleGroup(
            ColumnarStore.VectorizedSnapshot snap,
            VectorizedFilter filter,
            HavingPredicate havingPred,
            int groupKeyCol,
            int groupResultIdx,
            int[] groupByCopies,
            AggSpec[] specs,
            int aggCount,
            int resultColumnCount,
            LocalResult result,
            SessionLocal session,
            CompareMode compareMode,
            SegmentAggCache aggCache,
            String cacheSig) {
        Map<Value, AggState[]> groups = new HashMap<>();
        int[] sel = null;

        for (ColumnSegment seg : snap.segments) {
            int segCount = seg.getRowCount();
            if (segCount == 0) {
                continue;
            }
            int segId = seg.getSegmentId();
            long segVer = snap.deltaVersions.getOrDefault(segId, 0L);
            Map<Value, AggState[]> partial = null;
            if (aggCache != null) {
                SegmentAggCache.Entry cached = aggCache.lookup(cacheSig, segId,
                        seg.getGeneration(), segVer);
                if (cached != null) {
                    partial = castGroupPartial(cached.partial);
                }
            }
            if (partial == null) {
                partial = new HashMap<>();
                BitSet masked = snap.maskedBySegment.get(segId);
                boolean full = filter == null && (masked == null || masked.isEmpty());
                int selCount;
                if (full) {
                    selCount = segCount;
                } else {
                    if (sel == null || sel.length < segCount) {
                        sel = new int[segCount];
                    }
                    if (filter != null) {
                        selCount = filter.filterSegment(seg, sel, masked, session, compareMode);
                    } else {
                        selCount = 0;
                        for (int i = 0; i < segCount; i++) {
                            if (!masked.get(i)) {
                                sel[selCount++] = i;
                            }
                        }
                    }
                }
                if (selCount > 0) {
                    ColumnVector keyVec = seg.getColumn(groupKeyCol);
                    if (keyVec instanceof DictVarcharVector) {
                        DictVarcharVector dv = (DictVarcharVector) keyVec;
                        groupByDict(seg, dv.rawCodes(), dv.rawDict(), dv.dictSize(),
                                sel, selCount, full, specs, aggCount, partial, session, compareMode);
                    } else if (keyVec instanceof DictValueVector) {
                        DictValueVector dv = (DictValueVector) keyVec;
                        groupByDict(seg, dv.rawCodes(), dv.rawDict(), dv.dictSize(),
                                sel, selCount, full, specs, aggCount, partial, session, compareMode);
                    } else {
                        for (int i = 0; i < selCount; i++) {
                            int idx = full ? i : sel[i];
                            Value key = keyVec.get(idx);
                            AggState[] states = partial.get(key);
                            if (states == null) {
                                states = newStates(specs, aggCount);
                                partial.put(key, states);
                            }
                            for (int a = 0; a < aggCount; a++) {
                                states[a].accumulateAt(seg, idx, session, compareMode);
                            }
                        }
                    }
                }
                // 高基数分组的段级部分结果本身就太大，不缓存。
                if (aggCache != null && partial.size() <= SegmentAggCache.MAX_GROUP_KEYS) {
                    aggCache.store(cacheSig, segId, seg.getGeneration(), segVer, partial);
                }
            }
            mergeGroupPartials(groups, partial, specs, aggCount, session, compareMode);
        }

        for (Value[] row : snap.extraRows) {
            if (filter == null || filter.evalRow(row, session, compareMode)) {
                Value key = row[groupKeyCol];
                AggState[] states = groups.get(key);
                if (states == null) {
                    states = newStates(specs, aggCount);
                    groups.put(key, states);
                }
                for (int a = 0; a < aggCount; a++) {
                    states[a].accumulateRow(row, session, compareMode);
                }
            }
        }

        Value[] singleKeyHolder = havingPred != null ? new Value[1] : null;
        Value[] aggVals = new Value[aggCount];
        for (Map.Entry<Value, AggState[]> entry : groups.entrySet()) {
            AggState[] states = entry.getValue();
            for (int a = 0; a < aggCount; a++) {
                aggVals[a] = states[a].result(session);
            }
            if (havingPred != null) {
                singleKeyHolder[0] = entry.getKey();
                if (!havingPred.eval(singleKeyHolder, aggVals, session, compareMode)) {
                    continue;
                }
            }
            Value[] out = new Value[resultColumnCount];
            if (groupResultIdx >= 0 && groupResultIdx < resultColumnCount) {
                out[groupResultIdx] = entry.getKey();
            }
            if (groupByCopies != null) {
                for (int i = 0; i < resultColumnCount && i < groupByCopies.length; i++) {
                    if (groupByCopies[i] >= 0) {
                        out[i] = out[groupByCopies[i]];
                    }
                }
            }
            for (int a = 0; a < aggCount; a++) {
                int outIdx = specs[a].outIdx;
                if (outIdx >= 0 && outIdx < resultColumnCount) {
                    out[outIdx] = aggVals[a];
                }
            }
            result.addRow(out);
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<Value, AggState[]> castGroupPartial(Object partial) {
        return (Map<Value, AggState[]>) partial;
    }

    private static void mergeGroupPartials(Map<Value, AggState[]> groups,
            Map<Value, AggState[]> partial, AggSpec[] specs, int aggCount,
            SessionLocal session, CompareMode compareMode) {
        for (Map.Entry<Value, AggState[]> entry : partial.entrySet()) {
            AggState[] target = groups.get(entry.getKey());
            if (target == null) {
                target = newStates(specs, aggCount);
                groups.put(entry.getKey(), target);
            }
            // 不按引用共享：partial 可能是缓存里的对象，groups 的状态随后还会被
            // extraRows 累加，共享引用会污染缓存的部分结果。
            AggState[] source = entry.getValue();
            for (int a = 0; a < target.length; a++) {
                target[a].merge(source[a], session, compareMode);
            }
        }
    }

    /**
     * 构造「过滤 × 分组键 × 聚合口径」的查询签名，作为段级部分聚合缓存的键。
     *
     * <p>签名必须完整覆盖语义：过滤树的列、比较与常量，分组键列，每个聚合的
     * 类型、列、算术口径与结果类型。含 DISTINCT 聚合或无签名过滤形态
     * （子查询预计算等）时返回 {@code null}，该查询不参与缓存。
     */
    private static String buildCacheSignature(VectorizedFilter filter, int[] groupKeyCols,
            int groupCount, AggSpec[] specs) {
        String filterSig = filter == null ? "" : filter.signature();
        if (filterSig == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder(filterSig).append("|g");
        for (int g = 0; g < groupCount; g++) {
            sb.append(groupKeyCols[g]).append(',');
        }
        sb.append('|');
        for (AggSpec spec : specs) {
            if (spec.distinct) {
                return null;
            }
            sb.append(spec.type.ordinal()).append(',')
                    .append(spec.col).append(',')
                    .append(spec.rightCol).append(',')
                    .append(spec.arithOp).append(',')
                    .append(spec.colOnLeft).append(',')
                    .append(spec.resultType.getValueType()).append(',');
            if (spec.arithConst != null) {
                sb.append(spec.arithConst.getValueType()).append(':')
                        .append(spec.arithConst).append(';');
            } else {
                sb.append(';');
            }
        }
        return sb.toString();
    }

    private static void executeMultiGroup(
            ColumnarStore.VectorizedSnapshot snap,
            VectorizedFilter filter,
            HavingPredicate havingPred,
            int[] groupKeyCols,
            int[] groupResultIdxs,
            int[] groupByCopies,
            AggSpec[] specs,
            int aggCount,
            int resultColumnCount,
            LocalResult result,
            SessionLocal session,
            CompareMode compareMode) {
        int groupCount = groupKeyCols.length;
        Map<MultiGroupKey, AggState[]> groups = new HashMap<>();
        int[] sel = null;
        ColumnVector[] keyVecs = new ColumnVector[groupCount];

        for (ColumnSegment seg : snap.segments) {
            int segCount = seg.getRowCount();
            if (segCount == 0) {
                continue;
            }
            BitSet masked = snap.maskedBySegment.get(seg.getSegmentId());
            boolean full = filter == null && (masked == null || masked.isEmpty());
            int selCount;
            if (full) {
                selCount = segCount;
            } else {
                if (sel == null || sel.length < segCount) {
                    sel = new int[segCount];
                }
                if (filter != null) {
                    selCount = filter.filterSegment(seg, sel, masked, session, compareMode);
                } else {
                    selCount = 0;
                    for (int i = 0; i < segCount; i++) {
                        if (!masked.get(i)) {
                            sel[selCount++] = i;
                        }
                    }
                }
            }
            if (selCount == 0) {
                continue;
            }

            for (int g = 0; g < groupCount; g++) {
                keyVecs[g] = seg.getColumn(groupKeyCols[g]);
            }
            MultiGroupKey probe = new MultiGroupKey(new Value[groupCount]);
            for (int i = 0; i < selCount; i++) {
                int idx = full ? i : sel[i];
                for (int g = 0; g < groupCount; g++) {
                    probe.values[g] = keyVecs[g].get(idx);
                }
                AggState[] states = groups.get(probe);
                if (states == null) {
                    states = newStates(specs, aggCount);
                    groups.put(new MultiGroupKey(probe.values.clone()), states);
                }
                for (int a = 0; a < aggCount; a++) {
                    states[a].accumulateAt(seg, idx, session, compareMode);
                }
            }
        }

        MultiGroupKey probe = new MultiGroupKey(new Value[groupCount]);
        for (Value[] row : snap.extraRows) {
            if (filter == null || filter.evalRow(row, session, compareMode)) {
                for (int g = 0; g < groupCount; g++) {
                    probe.values[g] = row[groupKeyCols[g]];
                }
                AggState[] states = groups.get(probe);
                if (states == null) {
                    states = newStates(specs, aggCount);
                    groups.put(new MultiGroupKey(probe.values.clone()), states);
                }
                for (int a = 0; a < aggCount; a++) {
                    states[a].accumulateRow(row, session, compareMode);
                }
            }
        }

        Value[] aggVals = new Value[aggCount];
        for (Map.Entry<MultiGroupKey, AggState[]> entry : groups.entrySet()) {
            Value[] keys = entry.getKey().values;
            AggState[] states = entry.getValue();
            for (int a = 0; a < aggCount; a++) {
                aggVals[a] = states[a].result(session);
            }
            if (havingPred != null && !havingPred.eval(keys, aggVals, session, compareMode)) {
                continue;
            }
            Value[] out = new Value[resultColumnCount];
            for (int g = 0; g < groupCount; g++) {
                int rIdx = groupResultIdxs[g];
                if (rIdx >= 0 && rIdx < resultColumnCount) {
                    out[rIdx] = keys[g];
                }
            }
            if (groupByCopies != null) {
                for (int i = 0; i < resultColumnCount && i < groupByCopies.length; i++) {
                    if (groupByCopies[i] >= 0) {
                        out[i] = out[groupByCopies[i]];
                    }
                }
            }
            for (int a = 0; a < aggCount; a++) {
                int outIdx = specs[a].outIdx;
                if (outIdx >= 0 && outIdx < resultColumnCount) {
                    out[outIdx] = aggVals[a];
                }
            }
            result.addRow(out);
        }
    }

    private static final class MultiGroupKey {
        final Value[] values;

        MultiGroupKey(Value[] values) {
            this.values = values;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof MultiGroupKey)) {
                return false;
            }
            return Arrays.equals(values, ((MultiGroupKey) o).values);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(values);
        }
    }

    private static void groupByDict(
            ColumnSegment seg,
            int[] codes,
            Value[] dict,
            int dictSize,
            int[] sel,
            int selCount,
            boolean full,
            AggSpec[] specs,
            int aggCount,
            Map<Value, AggState[]> globalGroups,
            SessionLocal session,
            CompareMode compareMode) {
        // 槽位 dictSize 留给 NULL（code == -1）
        AggState[][] local = new AggState[dictSize + 1][];
        for (int i = 0; i < selCount; i++) {
            int idx = full ? i : sel[i];
            int code = codes[idx];
            int slot = code >= 0 ? code : dictSize;
            AggState[] states = local[slot];
            if (states == null) {
                states = newStates(specs, aggCount);
                local[slot] = states;
            }
            for (int a = 0; a < aggCount; a++) {
                states[a].accumulateAt(seg, idx, session, compareMode);
            }
        }
        for (int slot = 0; slot <= dictSize; slot++) {
            AggState[] localStates = local[slot];
            if (localStates == null) {
                continue;
            }
            Value key = slot < dictSize ? dict[slot] : ValueNull.INSTANCE;
            AggState[] target = globalGroups.get(key);
            if (target == null) {
                globalGroups.put(key, localStates);
            } else {
                for (int a = 0; a < aggCount; a++) {
                    target[a].merge(localStates[a], session, compareMode);
                }
            }
        }
    }

    private static AggState[] newStates(AggSpec[] specs, int aggCount) {
        AggState[] states = new AggState[aggCount];
        for (int a = 0; a < aggCount; a++) {
            states[a] = new AggState(specs[a]);
        }
        return states;
    }

    private static final class AggSpec {
        final AggregateType type;
        final int col;
        final int rightCol;
        final BinaryOperation.OpType arithOp;
        final boolean colOnLeft;
        final Value arithConst;
        final int outIdx;
        final TypeInfo resultType;
        final boolean distinct;

        AggSpec(AggregateType type, int col, int outIdx, TypeInfo resultType, boolean distinct) {
            this(type, col, -1, null, true, null, outIdx, resultType, distinct);
        }

        AggSpec(AggregateType type, int col, int rightCol, BinaryOperation.OpType arithOp,
                boolean colOnLeft, Value arithConst, int outIdx, TypeInfo resultType) {
            this(type, col, rightCol, arithOp, colOnLeft, arithConst, outIdx, resultType, false);
        }

        private AggSpec(AggregateType type, int col, int rightCol, BinaryOperation.OpType arithOp,
                boolean colOnLeft, Value arithConst, int outIdx, TypeInfo resultType, boolean distinct) {
            this.type = type;
            this.col = col;
            this.rightCol = rightCol;
            this.arithOp = arithOp;
            this.colOnLeft = colOnLeft;
            this.arithConst = arithConst;
            this.outIdx = outIdx;
            this.resultType = resultType;
            this.distinct = distinct;
        }

        boolean sameKey(AggSpec other) {
            if (type != other.type || col != other.col || rightCol != other.rightCol
                    || arithOp != other.arithOp || colOnLeft != other.colOnLeft
                    || distinct != other.distinct
                    || resultType.getValueType() != other.resultType.getValueType()) {
                return false;
            }
            if (arithConst == null && other.arithConst == null) {
                return true;
            }
            return arithConst != null && arithConst.equals(other.arithConst);
        }
    }

    private static final class AggState {
        final AggSpec spec;
        long count;
        long longSum;
        double doubleSum;
        BigDecimal decimalSum;
        boolean hasValue;
        Value bestVal;
        Set<Value> distinctSet;

        AggState(AggSpec spec) {
            this.spec = spec;
        }

        void accumulateSegmentFull(ColumnSegment seg, int segCount,
                SessionLocal session, CompareMode compareMode) {
            if (spec.type == AggregateType.COUNT_ALL) {
                count += segCount;
                return;
            }
            if (spec.distinct) {
                accumulateDistinctSegment(seg, null, segCount, true);
                return;
            }
            if (spec.arithOp != null) {
                accumulateArithSegment(seg, null, segCount, true, session, compareMode);
                return;
            }
            ColumnVector vec = seg.getColumn(spec.col);
            if (spec.type == AggregateType.COUNT) {
                NullBitmap nulls = vec.nulls();
                if (!nulls.hasNull()) {
                    count += segCount;
                } else {
                    for (int i = 0; i < segCount; i++) {
                        if (!nulls.isNull(i)) {
                            count++;
                        }
                    }
                }
                return;
            }
            if ((spec.type == AggregateType.SUM || spec.type == AggregateType.AVG)
                    && vec instanceof IntVector) {
                IntVector iv = (IntVector) vec;
                int[] vals = iv.rawValues();
                NullBitmap nulls = iv.nulls();
                if (!nulls.hasNull()) {
                    long s = 0;
                    for (int i = 0; i < segCount; i++) {
                        s += vals[i];
                    }
                    longSum += s;
                    count += segCount;
                    hasValue = true;
                } else {
                    long s = 0;
                    long c = 0;
                    for (int i = 0; i < segCount; i++) {
                        if (!nulls.isNull(i)) {
                            s += vals[i];
                            c++;
                        }
                    }
                    longSum += s;
                    count += c;
                    if (c > 0) {
                        hasValue = true;
                    }
                }
                return;
            }
            if ((spec.type == AggregateType.SUM || spec.type == AggregateType.AVG)
                    && vec instanceof DoubleVector) {
                DoubleVector dv = (DoubleVector) vec;
                double[] vals = dv.rawValues();
                NullBitmap nulls = dv.nulls();
                if (!nulls.hasNull()) {
                    double s = 0;
                    for (int i = 0; i < segCount; i++) {
                        s += vals[i];
                    }
                    doubleSum += s;
                    count += segCount;
                    hasValue = true;
                } else {
                    double s = 0;
                    long c = 0;
                    for (int i = 0; i < segCount; i++) {
                        if (!nulls.isNull(i)) {
                            s += vals[i];
                            c++;
                        }
                    }
                    doubleSum += s;
                    count += c;
                    if (c > 0) {
                        hasValue = true;
                    }
                }
                return;
            }
            for (int i = 0; i < segCount; i++) {
                accumulateValue(vec.get(i), session, compareMode);
            }
        }

        void accumulateSegmentSel(ColumnSegment seg, int[] sel, int selCount,
                SessionLocal session, CompareMode compareMode) {
            if (spec.type == AggregateType.COUNT_ALL) {
                count += selCount;
                return;
            }
            if (spec.distinct) {
                accumulateDistinctSegment(seg, sel, selCount, false);
                return;
            }
            if (spec.arithOp != null) {
                accumulateArithSegment(seg, sel, selCount, false, session, compareMode);
                return;
            }
            ColumnVector vec = seg.getColumn(spec.col);
            if (spec.type == AggregateType.COUNT) {
                NullBitmap nulls = vec.nulls();
                if (!nulls.hasNull()) {
                    count += selCount;
                } else {
                    for (int i = 0; i < selCount; i++) {
                        if (!nulls.isNull(sel[i])) {
                            count++;
                        }
                    }
                }
                return;
            }
            if ((spec.type == AggregateType.SUM || spec.type == AggregateType.AVG)
                    && vec instanceof IntVector) {
                IntVector iv = (IntVector) vec;
                int[] vals = iv.rawValues();
                NullBitmap nulls = iv.nulls();
                if (!nulls.hasNull()) {
                    long s = 0;
                    for (int i = 0; i < selCount; i++) {
                        s += vals[sel[i]];
                    }
                    longSum += s;
                    count += selCount;
                    hasValue = true;
                } else {
                    long s = 0;
                    long c = 0;
                    for (int i = 0; i < selCount; i++) {
                        int idx = sel[i];
                        if (!nulls.isNull(idx)) {
                            s += vals[idx];
                            c++;
                        }
                    }
                    longSum += s;
                    count += c;
                    if (c > 0) {
                        hasValue = true;
                    }
                }
                return;
            }
            if ((spec.type == AggregateType.SUM || spec.type == AggregateType.AVG)
                    && vec instanceof DoubleVector) {
                DoubleVector dv = (DoubleVector) vec;
                double[] vals = dv.rawValues();
                NullBitmap nulls = dv.nulls();
                if (!nulls.hasNull()) {
                    double s = 0;
                    for (int i = 0; i < selCount; i++) {
                        s += vals[sel[i]];
                    }
                    doubleSum += s;
                    count += selCount;
                    hasValue = true;
                } else {
                    double s = 0;
                    long c = 0;
                    for (int i = 0; i < selCount; i++) {
                        int idx = sel[i];
                        if (!nulls.isNull(idx)) {
                            s += vals[idx];
                            c++;
                        }
                    }
                    doubleSum += s;
                    count += c;
                    if (c > 0) {
                        hasValue = true;
                    }
                }
                return;
            }
            for (int i = 0; i < selCount; i++) {
                accumulateValue(vec.get(sel[i]), session, compareMode);
            }
        }

        private void accumulateDistinctSegment(ColumnSegment seg, int[] sel, int selCount, boolean full) {
            if (distinctSet == null) {
                distinctSet = new HashSet<>();
            }
            ColumnVector vec = seg.getColumn(spec.col);
            if (vec instanceof DictVarcharVector) {
                DictVarcharVector dv = (DictVarcharVector) vec;
                int dictSize = dv.dictSize();
                boolean[] seen = new boolean[dictSize];
                int[] codes = dv.rawCodes();
                if (full) {
                    for (int i = 0; i < selCount; i++) {
                        int c = codes[i];
                        if (c >= 0) {
                            seen[c] = true;
                        }
                    }
                } else {
                    for (int i = 0; i < selCount; i++) {
                        int c = codes[sel[i]];
                        if (c >= 0) {
                            seen[c] = true;
                        }
                    }
                }
                Value[] dict = dv.rawDict();
                for (int d = 0; d < dictSize; d++) {
                    if (seen[d]) {
                        distinctSet.add(dict[d]);
                    }
                }
                return;
            }
            if (vec instanceof DictValueVector) {
                DictValueVector dv = (DictValueVector) vec;
                int dictSize = dv.dictSize();
                boolean[] seen = new boolean[dictSize];
                int[] codes = dv.rawCodes();
                if (full) {
                    for (int i = 0; i < selCount; i++) {
                        int c = codes[i];
                        if (c >= 0) {
                            seen[c] = true;
                        }
                    }
                } else {
                    for (int i = 0; i < selCount; i++) {
                        int c = codes[sel[i]];
                        if (c >= 0) {
                            seen[c] = true;
                        }
                    }
                }
                Value[] dict = dv.rawDict();
                for (int d = 0; d < dictSize; d++) {
                    if (seen[d]) {
                        distinctSet.add(dict[d]);
                    }
                }
                return;
            }
            NullBitmap nulls = vec.nulls();
            boolean hasNull = nulls.hasNull();
            if (full) {
                for (int i = 0; i < selCount; i++) {
                    if (!hasNull || !nulls.isNull(i)) {
                        distinctSet.add(vec.get(i));
                    }
                }
            } else {
                for (int i = 0; i < selCount; i++) {
                    int idx = sel[i];
                    if (!hasNull || !nulls.isNull(idx)) {
                        distinctSet.add(vec.get(idx));
                    }
                }
            }
        }

        private void accumulateArithSegment(ColumnSegment seg, int[] sel, int selCount, boolean full,
                SessionLocal session, CompareMode compareMode) {
            ColumnVector leftVec = seg.getColumn(spec.col);
            ColumnVector rightVec = spec.rightCol >= 0 ? seg.getColumn(spec.rightCol) : null;
            int rt = spec.resultType.getValueType();
            if ((spec.type == AggregateType.SUM || spec.type == AggregateType.AVG)
                    && rightVec == null && rt == Value.BIGINT && leftVec instanceof IntVector) {
                IntVector iv = (IntVector) leftVec;
                int[] vals = iv.rawValues();
                NullBitmap nulls = iv.nulls();
                boolean hasNull = nulls.hasNull();
                long c = spec.arithConst.getLong();
                long s = 0;
                long cnt = 0;
                for (int i = 0; i < selCount; i++) {
                    int idx = full ? i : sel[i];
                    if (!hasNull || !nulls.isNull(idx)) {
                        s += evalLongOp(vals[idx], c);
                        cnt++;
                    }
                }
                longSum += s;
                count += cnt;
                if (cnt > 0) {
                    hasValue = true;
                }
                return;
            }
            if ((spec.type == AggregateType.SUM || spec.type == AggregateType.AVG)
                    && rightVec == null && rt == Value.DOUBLE && leftVec instanceof DoubleVector) {
                DoubleVector dv = (DoubleVector) leftVec;
                double[] vals = dv.rawValues();
                NullBitmap nulls = dv.nulls();
                boolean hasNull = nulls.hasNull();
                double c = spec.arithConst.getDouble();
                double s = 0;
                long cnt = 0;
                for (int i = 0; i < selCount; i++) {
                    int idx = full ? i : sel[i];
                    if (!hasNull || !nulls.isNull(idx)) {
                        s += evalDoubleOp(vals[idx], c);
                        cnt++;
                    }
                }
                doubleSum += s;
                count += cnt;
                if (cnt > 0) {
                    hasValue = true;
                }
                return;
            }
            for (int i = 0; i < selCount; i++) {
                int idx = full ? i : sel[i];
                Value lv = leftVec.get(idx);
                if (lv == ValueNull.INSTANCE) {
                    continue;
                }
                Value rv = rightVec != null ? rightVec.get(idx) : spec.arithConst;
                if (rv == ValueNull.INSTANCE) {
                    continue;
                }
                accumulateValue(evalArithValue(lv, rv), session, compareMode);
            }
        }

        private long evalLongOp(long colVal, long constVal) {
            long l = spec.colOnLeft ? colVal : constVal;
            long r = spec.colOnLeft ? constVal : colVal;
            switch (spec.arithOp) {
            case PLUS:
                return l + r;
            case MINUS:
                return l - r;
            case MULTIPLY:
                return l * r;
            default:
                return 0L;
            }
        }

        private double evalDoubleOp(double colVal, double constVal) {
            double l = spec.colOnLeft ? colVal : constVal;
            double r = spec.colOnLeft ? constVal : colVal;
            switch (spec.arithOp) {
            case PLUS:
                return l + r;
            case MINUS:
                return l - r;
            case MULTIPLY:
                return l * r;
            default:
                return 0.0d;
            }
        }

        private Value evalArithValue(Value colVal, Value otherVal) {
            Value l = spec.colOnLeft ? colVal : otherVal;
            Value r = spec.colOnLeft ? otherVal : colVal;
            int rt = spec.resultType.getValueType();
            if (rt == Value.DOUBLE) {
                return ValueDouble.get(evalDoubleOpRaw(l.getDouble(), r.getDouble()));
            }
            if (rt == Value.NUMERIC) {
                BigDecimal ld = l.getBigDecimal();
                BigDecimal rd = r.getBigDecimal();
                switch (spec.arithOp) {
                case PLUS:
                    return ValueNumeric.get(ld.add(rd));
                case MINUS:
                    return ValueNumeric.get(ld.subtract(rd));
                case MULTIPLY:
                    return ValueNumeric.get(ld.multiply(rd));
                default:
                    return ValueNull.INSTANCE;
                }
            }
            long lv = l.getLong();
            long rv = r.getLong();
            switch (spec.arithOp) {
            case PLUS:
                return ValueBigint.get(lv + rv);
            case MINUS:
                return ValueBigint.get(lv - rv);
            case MULTIPLY:
                return ValueBigint.get(lv * rv);
            default:
                return ValueNull.INSTANCE;
            }
        }

        private double evalDoubleOpRaw(double l, double r) {
            switch (spec.arithOp) {
            case PLUS:
                return l + r;
            case MINUS:
                return l - r;
            case MULTIPLY:
                return l * r;
            default:
                return 0.0d;
            }
        }

        void accumulateAt(ColumnSegment seg, int idx, SessionLocal session, CompareMode compareMode) {
            if (spec.type == AggregateType.COUNT_ALL) {
                count++;
                return;
            }
            if (spec.arithOp != null) {
                Value lv = seg.getColumn(spec.col).get(idx);
                if (lv == ValueNull.INSTANCE) {
                    return;
                }
                Value rv = spec.rightCol >= 0 ? seg.getColumn(spec.rightCol).get(idx) : spec.arithConst;
                if (rv == ValueNull.INSTANCE) {
                    return;
                }
                accumulateValue(evalArithValue(lv, rv), session, compareMode);
                return;
            }
            accumulateValue(seg.getColumn(spec.col).get(idx), session, compareMode);
        }

        void accumulateRow(Value[] row, SessionLocal session, CompareMode compareMode) {
            if (spec.type == AggregateType.COUNT_ALL) {
                count++;
                return;
            }
            if (spec.arithOp != null) {
                Value lv = row[spec.col];
                if (lv == ValueNull.INSTANCE) {
                    return;
                }
                Value rv = spec.rightCol >= 0 ? row[spec.rightCol] : spec.arithConst;
                if (rv == ValueNull.INSTANCE) {
                    return;
                }
                accumulateValue(evalArithValue(lv, rv), session, compareMode);
                return;
            }
            accumulateValue(row[spec.col], session, compareMode);
        }

        private void accumulateValue(Value v, SessionLocal session, CompareMode compareMode) {
            if (v == ValueNull.INSTANCE) {
                return;
            }
            if (spec.distinct) {
                if (distinctSet == null) {
                    distinctSet = new HashSet<>();
                }
                distinctSet.add(v);
                return;
            }
            count++;
            hasValue = true;
            switch (spec.type) {
            case COUNT:
                break;
            case SUM:
            case AVG:
                int rt = spec.resultType.getValueType();
                if (rt == Value.BIGINT) {
                    longSum += v.getLong();
                } else if (rt == Value.DOUBLE) {
                    doubleSum += v.getDouble();
                } else {
                    BigDecimal d = v.getBigDecimal();
                    decimalSum = decimalSum == null ? d : decimalSum.add(d);
                }
                break;
            case MIN:
                if (bestVal == null || v.compareTo(bestVal, session, compareMode) < 0) {
                    bestVal = v;
                }
                break;
            case MAX:
                if (bestVal == null || v.compareTo(bestVal, session, compareMode) > 0) {
                    bestVal = v;
                }
                break;
            default:
                break;
            }
        }

        void merge(AggState other, SessionLocal session, CompareMode compareMode) {
            if (spec.distinct) {
                if (other.distinctSet != null && !other.distinctSet.isEmpty()) {
                    if (distinctSet == null) {
                        distinctSet = new HashSet<>();
                    }
                    distinctSet.addAll(other.distinctSet);
                }
                return;
            }
            count += other.count;
            if (!other.hasValue) {
                return;
            }
            hasValue = true;
            switch (spec.type) {
            case SUM:
            case AVG:
                longSum += other.longSum;
                doubleSum += other.doubleSum;
                if (other.decimalSum != null) {
                    decimalSum = decimalSum == null ? other.decimalSum : decimalSum.add(other.decimalSum);
                }
                break;
            case MIN:
                if (bestVal == null || (other.bestVal != null && other.bestVal.compareTo(bestVal, session, compareMode) < 0)) {
                    bestVal = other.bestVal;
                }
                break;
            case MAX:
                if (bestVal == null || (other.bestVal != null && other.bestVal.compareTo(bestVal, session, compareMode) > 0)) {
                    bestVal = other.bestVal;
                }
                break;
            default:
                break;
            }
        }

        Value result(SessionLocal session) {
            if (spec.distinct) {
                return ValueBigint.get(distinctSet != null ? distinctSet.size() : 0L);
            }
            if (spec.type == AggregateType.COUNT_ALL || spec.type == AggregateType.COUNT) {
                return ValueBigint.get(count);
            }
            if (!hasValue) {
                return ValueNull.INSTANCE;
            }
            int rt = spec.resultType.getValueType();
            switch (spec.type) {
            case SUM:
                if (rt == Value.BIGINT) {
                    return ValueBigint.get(longSum);
                }
                if (rt == Value.DOUBLE) {
                    return ValueDouble.get(doubleSum);
                }
                return ValueNumeric.get(decimalSum != null ? decimalSum : BigDecimal.valueOf(longSum));
            case AVG:
                if (rt == Value.DOUBLE) {
                    return ValueDouble.get((doubleSum + longSum) / count);
                }
                BigDecimal sum = decimalSum != null ? decimalSum : BigDecimal.valueOf(longSum);
                return ValueNumeric.get(sum.divide(
                        BigDecimal.valueOf(count), spec.resultType.getScale(), RoundingMode.HALF_DOWN));
            case MIN:
            case MAX:
                return bestVal;
            default:
                return ValueNull.INSTANCE;
            }
        }
    }

    // ---- HAVING 谓词编译与求值 ----

    private static HavingPredicate compileHaving(
            Expression expr,
            TableFilter tableFilter,
            int[] groupKeyCols,
            List<AggSpec> aggList,
            SessionLocal session) {
        Expression e = expr.getNonAliasExpression();
        if (e instanceof ConditionAndOr) {
            ConditionAndOr cao = (ConditionAndOr) e;
            int op = H2ConditionAccess.getAndOrType(cao);
            HavingPredicate left = compileHaving(cao.getSubexpression(0), tableFilter, groupKeyCols, aggList, session);
            HavingPredicate right = compileHaving(cao.getSubexpression(1), tableFilter, groupKeyCols, aggList, session);
            if (left == null || right == null) {
                return null;
            }
            return new HavingAndOr(op == ConditionAndOr.AND, new HavingPredicate[] { left, right });
        }
        if (e instanceof ConditionAndOrN) {
            ConditionAndOrN caon = (ConditionAndOrN) e;
            int op = H2ConditionAccess.getAndOrType(caon);
            int subCount = caon.getSubexpressionCount();
            HavingPredicate[] compiled = new HavingPredicate[subCount];
            for (int i = 0; i < subCount; i++) {
                compiled[i] = compileHaving(caon.getSubexpression(i), tableFilter, groupKeyCols, aggList, session);
                if (compiled[i] == null) {
                    return null;
                }
            }
            return new HavingAndOr(op == ConditionAndOr.AND, compiled);
        }
        if (e instanceof Comparison) {
            Comparison cmp = (Comparison) e;
            int cmpType = cmp.getCompareType();
            if (cmpType != Comparison.EQUAL
                    && cmpType != Comparison.NOT_EQUAL
                    && cmpType != Comparison.BIGGER
                    && cmpType != Comparison.BIGGER_EQUAL
                    && cmpType != Comparison.SMALLER
                    && cmpType != Comparison.SMALLER_EQUAL) {
                return null;
            }
            Expression left = cmp.getSubexpression(0).getNonAliasExpression();
            Expression right = cmp.getSubexpression(1).getNonAliasExpression();
            if (right.isConstant()) {
                return buildHavingComparison(left, cmpType, right.getValue(session),
                        tableFilter, groupKeyCols, aggList, session);
            }
            if (left.isConstant()) {
                return buildHavingComparison(right, flipCmp(cmpType), left.getValue(session),
                        tableFilter, groupKeyCols, aggList, session);
            }
            return null;
        }
        return null;
    }

    private static HavingPredicate buildHavingComparison(
            Expression target,
            int cmpType,
            Value constant,
            TableFilter tableFilter,
            int[] groupKeyCols,
            List<AggSpec> aggList,
            SessionLocal compileSession) {
        if (constant == ValueNull.INSTANCE) {
            return (groupKeys, aggVals, session, compareMode) -> false;
        }
        if (target instanceof Aggregate) {
            Aggregate agg = (Aggregate) target;
            AggSpec spec = parseAggregate(agg, tableFilter, -1, compileSession);
            if (spec == null) {
                return null;
            }
            int aggIdx = -1;
            for (int i = 0; i < aggList.size(); i++) {
                AggSpec existing = aggList.get(i);
                if (existing.sameKey(spec)) {
                    aggIdx = i;
                    break;
                }
            }
            if (aggIdx < 0) {
                aggIdx = aggList.size();
                aggList.add(spec);
            }
            return new HavingComparison(-1, aggIdx, cmpType, constant);
        }
        if (target instanceof ExpressionColumn) {
            ExpressionColumn ec = (ExpressionColumn) target;
            if (ec.getTableFilter() != tableFilter) {
                return null;
            }
            int colId = ec.getColumn().getColumnId();
            for (int g = 0; g < groupKeyCols.length; g++) {
                if (groupKeyCols[g] == colId) {
                    return new HavingComparison(g, -1, cmpType, constant);
                }
            }
            return null;
        }
        return null;
    }

    private static int flipCmp(int cmpType) {
        switch (cmpType) {
        case Comparison.BIGGER:
            return Comparison.SMALLER;
        case Comparison.BIGGER_EQUAL:
            return Comparison.SMALLER_EQUAL;
        case Comparison.SMALLER:
            return Comparison.BIGGER;
        case Comparison.SMALLER_EQUAL:
            return Comparison.BIGGER_EQUAL;
        default:
            return cmpType;
        }
    }

    private interface HavingPredicate {
        boolean eval(Value[] groupKeys, Value[] aggVals, SessionLocal session, CompareMode compareMode);
    }

    private static final class HavingAndOr implements HavingPredicate {
        final boolean isAnd;
        final HavingPredicate[] children;

        HavingAndOr(boolean isAnd, HavingPredicate[] children) {
            this.isAnd = isAnd;
            this.children = children;
        }

        @Override
        public boolean eval(Value[] groupKeys, Value[] aggVals, SessionLocal session, CompareMode compareMode) {
            if (isAnd) {
                for (HavingPredicate c : children) {
                    if (!c.eval(groupKeys, aggVals, session, compareMode)) {
                        return false;
                    }
                }
                return true;
            }
            for (HavingPredicate c : children) {
                if (c.eval(groupKeys, aggVals, session, compareMode)) {
                    return true;
                }
            }
            return false;
        }
    }

    private static final class HavingComparison implements HavingPredicate {
        final int groupPos;
        final int aggIdx;
        final int cmpType;
        final Value constant;

        HavingComparison(int groupPos, int aggIdx, int cmpType, Value constant) {
            this.groupPos = groupPos;
            this.aggIdx = aggIdx;
            this.cmpType = cmpType;
            this.constant = constant;
        }

        @Override
        public boolean eval(Value[] groupKeys, Value[] aggVals, SessionLocal session, CompareMode compareMode) {
            Value v = aggIdx >= 0 ? aggVals[aggIdx] : groupKeys[groupPos];
            if (v == ValueNull.INSTANCE) {
                return false;
            }
            int c = v.compareTo(constant, session, compareMode);
            switch (cmpType) {
            case Comparison.EQUAL:
                return c == 0;
            case Comparison.NOT_EQUAL:
                return c != 0;
            case Comparison.BIGGER:
                return c > 0;
            case Comparison.BIGGER_EQUAL:
                return c >= 0;
            case Comparison.SMALLER:
                return c < 0;
            case Comparison.SMALLER_EQUAL:
                return c <= 0;
            default:
                return false;
            }
        }
    }
}

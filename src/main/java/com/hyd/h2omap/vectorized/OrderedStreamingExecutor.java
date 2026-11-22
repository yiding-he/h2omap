/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.vectorized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import org.h2.command.query.QueryOrderBy;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.TableFilter;
import org.h2.value.CompareMode;
import org.h2.value.Value;
import org.h2.value.ValueNull;

import com.hyd.h2omap.index.OmapScanIndex;
import com.hyd.h2omap.stats.ZoneMap;
import com.hyd.h2omap.storage.ColumnSegment;
import com.hyd.h2omap.storage.VectorizedSnapshot;
import com.hyd.h2omap.storage.RowId;
import com.hyd.h2omap.table.OmapTable;
import com.hyd.h2omap.vector.ColumnVector;

/**
 * 有序流式执行（M20）：{@code 无 LIMIT 的 ORDER BY} 在段间区间不相交时按段流式输出。
 *
 * <p>排序键取首列判定段间顺序：首列的段间 zone 区间两两严格不相交时，段 k 的任何一行
 * 都严格先于（或后于）段 k+1 的任何一行，与后续列取值无关；段按发射方向的 zone 边界
 * 排序后逐段推进，段内一次排序，delta 新像与开放段行按复合键预先排好序后归并进输出流。
 * 客户端取一行才推进一次：停止取数时，未触碰的段零读取。
 *
 * <p>投影延迟物化：排序阶段每段只读「排序键 ∪ 谓词列」（逐出段按段文件列目录切片读），
 * 行真正要发出时才经 {@code ColumnarStore.readDeferredProjection} 读回投影列；
 * 谓词整段过滤空的段，投影列一次都不读。
 *
 * <p>首列声明为 {@code NOT NULL} 才下推：NULL 组不受 zone 约束、跨段存在，
 * {@code NULLS FIRST} 时出首行前就得收齐全部段的 NULL 行；可空首列交回 H2。
 * 区间相交（随机写入形态）、多表、表达式排序键等一律交回 H2 行式路径。
 * 形态合法但区间相交时 {@link #tryClaim} 返回 {@code null}，调用方无缝回退。
 */
final class OrderedStreamingExecutor {

    private OrderedStreamingExecutor() {
    }

    static OrderedStreamingRowSource tryClaim(
            SessionLocal session, TableFilter tableFilter, Expression condition,
            List<Expression> expressions, int resultColumnCount, long offset,
            SortOrder sortOrder) {
        if (!(tableFilter.getTable() instanceof OmapTable)) {
            return null;
        }
        // 主键/唯一索引点查或范围查找走 OmapRowIdIndex 更快，不走全表扫描
        if (tableFilter.getIndex() != null && !(tableFilter.getIndex() instanceof OmapScanIndex)) {
            return null;
        }
        if (expressions == null || expressions.isEmpty()) {
            return null;
        }
        List<QueryOrderBy> orderList = sortOrder.getOrderList();
        int[] sortTypes = sortOrder.getSortTypesWithNullOrdering();
        if (orderList == null || orderList.isEmpty() || sortTypes == null
                || orderList.size() != sortTypes.length) {
            return null;
        }
        int keyCount = orderList.size();
        int[] sortCols = new int[keyCount];
        boolean[] desc = new boolean[keyCount];
        boolean[] nullsFirst = new boolean[keyCount];
        for (int i = 0; i < keyCount; i++) {
            sortCols[i] = VectorizedFilter.resolveColumn(orderList.get(i).expression, tableFilter);
            if (sortCols[i] < 0) {
                return null;
            }
            desc[i] = (sortTypes[i] & SortOrder.DESCENDING) != 0;
            nullsFirst[i] = (sortTypes[i] & SortOrder.NULLS_FIRST) != 0;
        }
        OmapTable table = (OmapTable) tableFilter.getTable();
        Column[] columns = table.getColumns();
        // NULL 组跨段：可空首列不参与流式，交回 H2。
        if (columns[sortCols[0]].isNullable()) {
            return null;
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
                    return null;
                }
                projCols[i] = ec.getColumn().getColumnId();
            } else if (e.isConstant()) {
                projCols[i] = -1;
                projConsts[i] = e.getValue(session);
            } else {
                return null;
            }
        }
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

        VectorizedFilter filter = null;
        if (condition != null) {
            filter = VectorizedFilter.compile(condition, tableFilter, session);
            if (filter == null) {
                return null;
            }
        }

        // 排序阶段的最小读取集：排序键与谓词列；投影列延迟到行发出时读回。
        int[] snapshotColumns = OmapVectorizedExecutor.unionColumns(
                sortCols, filter != null ? filter.referencedColumns() : null);

        Built built = build(session, table, tableFilter, filter, snapshotColumns,
                sortCols, desc, nullsFirst, compareMode(table));
        if (built == null) {
            return null;
        }
        table.getStore().recordOrderedStreamingClaim();
        return new Cursor(session, table, tableFilter, filter, compareMode(table),
                projCols, projConsts, projToUnique,
                Arrays.copyOf(uniqueProjCols, uniqueCount), uniqueCount, outLen,
                sortCols, desc, nullsFirst, snapshotColumns, offset, built);
    }

    private static CompareMode compareMode(OmapTable table) {
        return table.getCompareMode();
    }

    /** 一次流式执行的不可变状态：快照、发射序段数组与预先排好序的活像/开放段行。 */
    private static final class Built {
        final VectorizedSnapshot snap;
        final ColumnSegment[] orderedSegs;
        final Value[][] extras;
        final Value[][] extraKeys;
        final long[] extraIds;

        Built(VectorizedSnapshot snap, ColumnSegment[] orderedSegs,
                Value[][] extras, Value[][] extraKeys, long[] extraIds) {
            this.snap = snap;
            this.orderedSegs = orderedSegs;
            this.extras = extras;
            this.extraKeys = extraKeys;
            this.extraIds = extraIds;
        }
    }

    /**
     * 截取快照并做发射前准备。返回 {@code null} 表示段间区间相交（或边界缺失），
     * 调用方交回 H2 行式路径。
     */
    private static Built build(SessionLocal session, OmapTable table, TableFilter tableFilter,
            VectorizedFilter filter, int[] snapshotColumns, int[] sortCols, boolean[] desc,
            boolean[] nullsFirst, CompareMode compareMode) {
        VectorizedSnapshot snap = OmapVectorizedExecutor.takeSnapshot(table, filter, tableFilter,
                snapshotColumns);
        List<ColumnSegment> active = new ArrayList<>();
        for (ColumnSegment seg : snap.segments) {
            if (seg.getRowCount() == 0) {
                continue;
            }
            BitSet m = snap.maskedBySegment.get(seg.getSegmentId());
            if (m != null && m.cardinality() >= seg.getRowCount()) {
                continue;
            }
            active.add(seg);
        }
        ColumnSegment[] ordered = orderByFirstZone(active, sortCols[0], desc[0], session,
                compareMode);
        if (ordered == null) {
            return null;
        }
        // 活像与开放段行：过滤后按复合键排序，rowId 作并列定序。
        List<Value[]> keptRows = new ArrayList<>();
        List<Value[]> keptKeys = new ArrayList<>();
        List<Long> keptIds = new ArrayList<>();
        for (int i = 0; i < snap.extraRows.size(); i++) {
            Value[] row = snap.extraRows.get(i);
            if (filter != null && !filter.evalRow(row, session, compareMode)) {
                continue;
            }
            keptRows.add(row);
            keptKeys.add(extractKeys(row, sortCols));
            keptIds.add(snap.extraRowIds[i]);
        }
        int n = keptRows.size();
        Integer[] ord = new Integer[n];
        for (int i = 0; i < n; i++) {
            ord[i] = i;
        }
        Arrays.sort(ord, (x, y) -> compareKeys(session, compareMode, sortCols, desc, nullsFirst,
                keptKeys.get(x), keptIds.get(x), keptKeys.get(y), keptIds.get(y)));
        Value[][] extras = new Value[n][];
        Value[][] extraKeys = new Value[n][];
        long[] extraIds = new long[n];
        for (int i = 0; i < n; i++) {
            extras[i] = keptRows.get(ord[i]);
            extraKeys[i] = keptKeys.get(ord[i]);
            extraIds[i] = keptIds.get(ord[i]);
        }
        return new Built(snap, ordered, extras, extraKeys, extraIds);
    }

    private static Value[] extractKeys(Value[] row, int[] sortCols) {
        Value[] keys = new Value[sortCols.length];
        for (int i = 0; i < sortCols.length; i++) {
            keys[i] = row[sortCols[i]];
        }
        return keys;
    }

    /**
     * 复合键比较：逐排序项先判 NULL 位次（按各项 NULLS 旗标），再按值比较并按方向取反；
     * 全部相等时按 rowId 升序定序（与 H2 稳定排序的行序一致）。
     * 返回负数表示 {@code a} 在结果中更靠前。
     */
    static int compareKeys(SessionLocal session, CompareMode compareMode, int[] sortCols,
            boolean[] desc, boolean[] nullsFirst, Value[] ka, long ida, Value[] kb, long idb) {
        for (int i = 0; i < sortCols.length; i++) {
            Value a = ka[i];
            Value b = kb[i];
            boolean an = a == null || a == ValueNull.INSTANCE;
            boolean bn = b == null || b == ValueNull.INSTANCE;
            if (an || bn) {
                if (an && bn) {
                    continue;
                }
                return an == nullsFirst[i] ? -1 : 1;
            }
            int cmp = a.compareTo(b, session, compareMode);
            if (desc[i]) {
                cmp = -cmp;
            }
            if (cmp != 0) {
                return cmp;
            }
        }
        return Long.compare(ida, idb);
    }

    /** 首列单值比较（发射口径）；调用方保证两值非 NULL。 */
    private static int compareFirstCol(SessionLocal session, CompareMode compareMode,
            boolean descending, Value a, Value b) {
        int cmp = a.compareTo(b, session, compareMode);
        return descending ? -cmp : cmp;
    }

    /** 单值比较的发射口径：返回负数表示 {@code a} 更靠前。 */
    private static int compareValue(SessionLocal session, CompareMode compareMode,
            boolean descending, Value a, Value b) {
        int cmp = a.compareTo(b, session, compareMode);
        return descending ? -cmp : cmp;
    }

    /**
     * 段按首列 zone 边界排成发射序并校验区间两两严格分离。
     * 边界缺失（全 NULL、不可比较类型）或相邻区间相接/相交返回 {@code null}。
     */
    private static ColumnSegment[] orderByFirstZone(List<ColumnSegment> active, int sortCol,
            boolean descending, SessionLocal session, CompareMode compareMode) {
        int n = active.size();
        Value[] far = new Value[n];
        Value[] near = new Value[n];
        for (int i = 0; i < n; i++) {
            ZoneMap zone = active.get(i).getZoneMap(sortCol);
            Value lo = zone.getMin();
            Value hi = zone.getMax();
            if (lo == null || hi == null || lo == ValueNull.INSTANCE || hi == ValueNull.INSTANCE) {
                return null;
            }
            far[i] = descending ? hi : lo;
            near[i] = descending ? lo : hi;
        }
        Integer[] ord = new Integer[n];
        for (int i = 0; i < n; i++) {
            ord[i] = i;
        }
        Arrays.sort(ord, (x, y) -> {
            int c = compareValue(session, compareMode, descending, far[x], far[y]);
            return c != 0 ? c : Integer.compare(x, y);
        });
        ColumnSegment[] out = new ColumnSegment[n];
        for (int i = 0; i < n; i++) {
            out[i] = active.get(ord[i]);
        }
        // 相邻严格分离：DESC 要求 min_i > max_{i+1}，ASC 要求 max_i < min_{i+1}。
        for (int i = 0; i + 1 < n; i++) {
            Value a = descending
                    ? out[i].getZoneMap(sortCol).getMin()
                    : out[i].getZoneMap(sortCol).getMax();
            Value b = descending
                    ? out[i + 1].getZoneMap(sortCol).getMax()
                    : out[i + 1].getZoneMap(sortCol).getMin();
            if (compareValue(session, compareMode, descending, a, b) >= 0) {
                return null;
            }
        }
        return out;
    }

    /** 流式游标的段内状态。 */
    private static final class Seg {
        final ColumnSegment seg;
        final Value virtualFarBound;
        boolean initialized;
        boolean exhausted;
        int[] rows;
        Value[][] keys;
        int n;
        int ptr;
        ColumnVector[] projVecs;

        Seg(ColumnSegment seg, Value virtualFarBound) {
            this.seg = seg;
            this.virtualFarBound = virtualFarBound;
        }
    }

    private static final class Cursor implements OrderedStreamingRowSource {

        private final SessionLocal session;
        private final OmapTable table;
        private final TableFilter tableFilter;
        private final VectorizedFilter filter;
        private final CompareMode compareMode;
        private final int[] projCols;
        private final Value[] projConsts;
        private final int[] projToUnique;
        private final int[] uniqueProjCols;
        private final int uniqueCount;
        private final int outLen;
        private final int[] sortCols;
        private final boolean[] desc;
        private final boolean[] nullsFirst;
        private final int[] snapshotColumns;
        private final long offset;

        private VectorizedSnapshot snap;
        private ColumnSegment[] orderedSegs;
        private Value[][] extras;
        private Value[][] extraKeys;
        private long[] extraIds;
        private int extraIdx;
        private int segPos;
        private Seg cur;
        private long pendingOffset;

        Cursor(SessionLocal session, OmapTable table, TableFilter tableFilter,
                VectorizedFilter filter, CompareMode compareMode,
                int[] projCols, Value[] projConsts, int[] projToUnique,
                int[] uniqueProjCols, int uniqueCount, int outLen,
                int[] sortCols, boolean[] desc, boolean[] nullsFirst,
                int[] snapshotColumns, long offset, Built built) {
            this.session = session;
            this.table = table;
            this.tableFilter = tableFilter;
            this.filter = filter;
            this.compareMode = compareMode;
            this.projCols = projCols;
            this.projConsts = projConsts;
            this.projToUnique = projToUnique;
            this.uniqueProjCols = uniqueProjCols;
            this.uniqueCount = uniqueCount;
            this.outLen = outLen;
            this.sortCols = sortCols;
            this.desc = desc;
            this.nullsFirst = nullsFirst;
            this.snapshotColumns = snapshotColumns;
            this.offset = offset;
            adopt(built);
        }

        private void adopt(Built built) {
            this.snap = built.snap;
            this.orderedSegs = built.orderedSegs;
            this.extras = built.extras;
            this.extraKeys = built.extraKeys;
            this.extraIds = built.extraIds;
            this.extraIdx = 0;
            this.segPos = -1;
            this.cur = null;
            this.pendingOffset = offset;
        }

        @Override
        public Value[] nextRow() {
            Value[] row = nextEmitted();
            while (row != null && pendingOffset > 0) {
                pendingOffset--;
                row = nextEmitted();
            }
            return row;
        }

        @Override
        public void reset() {
            Built rebuilt = build(session, table, tableFilter, filter, snapshotColumns,
                    sortCols, desc, nullsFirst, compareMode);
            // 结果复用路径只在没有写入时触发（写入会使 modificationId 失效复用判定），
            // 无写入时段区间形状不变；此分支实际不可达，可达即判据被破坏，显式暴露。
            if (rebuilt == null) {
                throw new IllegalStateException("有序流式重置时段区间不再严格不相交");
            }
            adopt(rebuilt);
        }

        private Value[] nextEmitted() {
            while (true) {
                Seg st = cur;
                if (st == null || st.exhausted || (st.initialized && st.ptr >= st.n)) {
                    segPos++;
                    st = segPos < orderedSegs.length
                            ? new Seg(orderedSegs[segPos], virtualFar(orderedSegs[segPos]))
                            : null;
                    cur = st;
                }
                Value[] extraRow = extraIdx < extras.length ? extras[extraIdx] : null;
                if (st == null) {
                    if (extraRow == null) {
                        return null;
                    }
                    extraIdx++;
                    return emitExtra(extraRow);
                }
                if (!st.initialized) {
                    if (extraRow != null && compareFirstCol(session, compareMode, desc[0],
                            extraRow[sortCols[0]], st.virtualFarBound) < 0) {
                        // 活像首列严格优于段的边界最优值，必在整段之前
                        extraIdx++;
                        return emitExtra(extraRow);
                    }
                    initSeg(st);
                    continue;
                }
                // 段已初始化且还有行
                if (extraRow != null) {
                    int cmp = compareKeys(session, compareMode, sortCols, desc, nullsFirst,
                            st.keys[st.ptr], RowId.ofSegment(st.seg.getSegmentId(), st.rows[st.ptr]),
                            extraKeys[extraIdx], extraIds[extraIdx]);
                    if (cmp > 0) {
                        extraIdx++;
                        return emitExtra(extraRow);
                    }
                }
                return emitSegment(st);
            }
        }

        private Value virtualFar(ColumnSegment seg) {
            ZoneMap zone = seg.getZoneMap(sortCols[0]);
            return desc[0] ? zone.getMax() : zone.getMin();
        }

        private void initSeg(Seg st) {
            ColumnSegment s = st.seg;
            BitSet masked = snap.maskedBySegment.get(s.getSegmentId());
            int rc = s.getRowCount();
            int[] sel = new int[rc];
            int n = filter == null ? fillAll(sel, rc, masked)
                    : filter.filterSegment(s, sel, masked, session, compareMode);
            if (n == 0) {
                st.exhausted = true;
                return;
            }
            ColumnVector[] sortVecs = new ColumnVector[sortCols.length];
            for (int i = 0; i < sortCols.length; i++) {
                sortVecs[i] = s.getColumn(sortCols[i]);
            }
            Value[][] keys = new Value[n][];
            for (int k = 0; k < n; k++) {
                Value[] key = new Value[sortCols.length];
                for (int i = 0; i < sortCols.length; i++) {
                    key[i] = sortVecs[i].get(sel[k]);
                }
                keys[k] = key;
            }
            Integer[] ord = new Integer[n];
            for (int k = 0; k < n; k++) {
                ord[k] = k;
            }
            final Value[][] fkeys = keys;
            final int[] fsel = sel;
            Arrays.sort(ord, (x, y) -> compareKeys(session, compareMode, sortCols, desc, nullsFirst,
                    fkeys[x], fsel[x], fkeys[y], fsel[y]));
            st.rows = new int[n];
            st.keys = new Value[n][];
            for (int k = 0; k < n; k++) {
                st.rows[k] = fsel[ord[k]];
                st.keys[k] = fkeys[ord[k]];
            }
            st.n = n;
            st.ptr = 0;
            st.initialized = true;
        }

        private Value[] emitSegment(Seg st) {
            int idx = st.rows[st.ptr];
            Value[] out = new Value[outLen];
            if (uniqueCount > 0) {
                if (st.projVecs == null) {
                    // 延迟物化：段内首次发出一行才读回投影列，不进驻留模型。
                    st.projVecs = table.getStore().readDeferredProjection(
                            st.seg.getSegmentId(), uniqueProjCols);
                }
                for (int c = 0; c < outLen; c++) {
                    int u = projToUnique[c];
                    out[c] = u >= 0 ? st.projVecs[u].get(idx) : projConsts[c];
                }
            } else {
                for (int c = 0; c < outLen; c++) {
                    out[c] = projConsts[c];
                }
            }
            st.ptr++;
            return out;
        }

        private Value[] emitExtra(Value[] extraRow) {
            Value[] out = new Value[outLen];
            for (int c = 0; c < outLen; c++) {
                int col = projCols[c];
                out[c] = col >= 0 ? extraRow[col] : projConsts[c];
            }
            return out;
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
}

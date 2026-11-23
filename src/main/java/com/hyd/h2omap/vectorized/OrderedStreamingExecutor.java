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
import org.h2.result.SortOrder;
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
 * <p>排序键取首列判定段间顺序：首列的段间 zone 区间次邻严格分离（M22 起；相邻段
 * 允许边界相交——迟到写入形态）。发射单元 U_k = 段 k 的主区间行（首键严格优于
 * 段 k+1 的近端界）∪ 段 k-1 的迟到带行（首键不严格优于段 k 的近端界），单元内按
 * 复合键一次归并，单元之间严格有序；次邻区间相接/相交时单元间无法保证有序，整条
 * 查询交回 H2。delta 新像与开放段行按复合键预先排好序后归并进输出流。
 * 客户端取一行才推进一次：停止取数时，未触碰的段零读取。
 *
 * <p>投影延迟物化：排序阶段每段只读「排序键 ∪ 谓词列」（逐出段按段文件列目录切片读），
 * 行真正要发出时才经 {@code ColumnarStore.readDeferredProjection} 读回投影列；
 * 谓词整段过滤空的段，投影列一次都不读。
 *
 * <p>首列 NULL 门控按内容而非声明判定：快照内全部参与段的首列都不含 NULL 时才下推
 * （段级 hasNull 随段文件 v3 目录与 manifest v3 骨架持久化）；任一段实际含 NULL，
 * 或段无骨架信息（无持久化内存库的未加载段），则整条查询交回 H2。
 * {@code NULLS FIRST} 时出首行前就得收齐全部段的 NULL 行；可空首列交回 H2。
 * 次邻区间相交（跨两段以上的极迟写入）、多表、表达式排序键等一律交回 H2 行式路径。
 * 形态合法但区间相交时 {@link #tryClaim} 返回 {@code null}，调用方无缝回退。
 *
 * <p>表达式投影（M20-2）：排序阶段不受投影影响，投影里的表达式在行真正发出时
 * 才逐行求值——把该行引用列的值经 {@code TableFilter.set(Row)} 就位后调
 * {@code Expression.getValue(session)}，只对真正发射的行付费。接受边界：
 * 确定表达式（{@code ExpressionVisitor.DETERMINISTIC_VISITOR} 判定，{@code RAND()}
 * 等交回 H2）、引用列全在本表数据列范围内（{@code _ROWID_} 等伪列交回）；
 * 顶层 {@code Subquery} 交回 H2，嵌套在其他表达式里的子查询照 H2 行式口径逐行求值。
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
        OmapTable table = (OmapTable) tableFilter.getTable();

        // 投影分类（列引用/常量/确定表达式）与最小读取列集：M23 起与平面流式共用
        ProjectionPlan plan = ProjectionPlan.claim(session, tableFilter, expressions, resultColumnCount);
        if (plan == null) {
            return null;
        }
        int outLen = plan.outLen;
        int[] projCols = plan.projCols;
        Value[] projConsts = plan.projConsts;
        Expression[] projExprs = plan.projExprs;
        int[] projToUnique = plan.projToUnique;
        int[] uniqueProjCols = plan.uniqueProjCols;
        int uniqueCount = plan.uniqueCount;

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
                projCols, projConsts, projExprs, projToUnique,
                uniqueProjCols, uniqueCount, outLen,
                sortCols, desc, nullsFirst, snapshotColumns, offset, built);
    }

    private static CompareMode compareMode(OmapTable table) {
        return table.getCompareMode();
    }

    /** 一次流式执行的不可变状态：快照、发射序段数组、各段近端界与预先排好序的活像/开放段行。 */
    private static final class Built {
        final VectorizedSnapshot snap;
        final ColumnSegment[] orderedSegs;
        /** 各段发射方向上的近端界：迟到带切分与活像预发射的判据。 */
        final Value[] nearBounds;
        final Value[][] extras;
        final Value[][] extraKeys;
        final long[] extraIds;

        Built(VectorizedSnapshot snap, ColumnSegment[] orderedSegs, Value[] nearBounds,
                Value[][] extras, Value[][] extraKeys, long[] extraIds) {
            this.snap = snap;
            this.orderedSegs = orderedSegs;
            this.nearBounds = nearBounds;
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
            // NULL 组跨段且不受 zone 约束：任一参与段的首列实际含 NULL 即交回 H2。
            // 惰性加载下无骨架的段拿不到 hasNull，同样拒绝。
            if (seg.hasNull(sortCols[0])) {
                return null;
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
        Value[] nearBounds = new Value[ordered.length];
        for (int i = 0; i < ordered.length; i++) {
            ZoneMap zone = ordered[i].getZoneMap(sortCols[0]);
            nearBounds[i] = desc[0] ? zone.getMax() : zone.getMin();
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
        return new Built(snap, ordered, nearBounds, extras, extraKeys, extraIds);
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
     * 段按首列 zone 边界排成发射序并校验次邻区间严格分离（M22）。
     * 相邻段允许边界相交（迟到写入形态）：相交带的行归入下一发射单元一起归并；
     * 次邻段必须严格分离，否则单元之间无法保证严格有序。
     * 边界缺失（全 NULL、不可比较类型）返回 {@code null}。
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
        // 次邻严格分离：ASC 要求 max_i < min_{i+2}，DESC 要求 min_i > max_{i+2}。
        // 相邻对相交交给发射单元的迟到带归并，次邻相接/相交则单元间无法保证有序。
        for (int i = 0; i + 2 < n; i++) {
            Value a = descending
                    ? out[i].getZoneMap(sortCol).getMin()
                    : out[i].getZoneMap(sortCol).getMax();
            Value b = descending
                    ? out[i + 2].getZoneMap(sortCol).getMax()
                    : out[i + 2].getZoneMap(sortCol).getMin();
            if (compareValue(session, compareMode, descending, a, b) >= 0) {
                return null;
            }
        }
        return out;
    }

    /** 一个冻结段的发射准备状态：过滤与段内排序一次完成，行分成主区间前缀与迟到带后缀。 */
    private static final class Seg {
        final ColumnSegment seg;
        int[] rows;
        Value[][] keys;
        int n;
        /** 迟到带起点：首键不严格优于下一段近端界的行从该下标起。 */
        int carryStart;
        ColumnVector[] projVecs;

        Seg(ColumnSegment seg) {
            this.seg = seg;
        }
    }

    /**
     * 一个发射单元：当前段的主区间行与上一段的迟到带行按复合键归并后的结果。
     * 单元之间由次邻门控保证严格有序；单元内的行序与 H2 稳定排序一致（rowId 并列定序）。
     */
    private static final class Unit {
        /** 单元序号，即发射序段数组下标。 */
        final int index;
        /** 迟到带来源段（构造单元时已初始化），首单元为 null。 */
        final Seg srcA;
        /** 主区间来源段，初始化时才过滤排序。 */
        final ColumnSegment segB;
        /** {@link Unit#segB} 的发射准备状态，初始化后可用。 */
        Seg segBSeg;
        int[] rows;
        Value[][] keys;
        boolean[] fromA;
        int n;
        boolean initialized;
        boolean exhausted;
        int ptr;

        Unit(int index, Seg srcA, ColumnSegment segB) {
            this.index = index;
            this.srcA = srcA;
            this.segB = segB;
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
        private final Expression[] projExprs;
        private final int[] projToUnique;
        private final int[] uniqueProjCols;
        private final int uniqueCount;
        private final int outLen;
        private final int[] sortCols;
        private final boolean[] desc;
        private final boolean[] nullsFirst;
        private final int[] snapshotColumns;
        private final long offset;
        private final boolean hasExprs;
        private final int[] colToVec;

        private VectorizedSnapshot snap;
        private ColumnSegment[] orderedSegs;
        private Value[] nearBounds;
        private Value[][] extras;
        private Value[][] extraKeys;
        private long[] extraIds;
        private int extraIdx;
        private int segPos;
        private Unit cur;
        /** 最近完成发射准备的段，供下一单元取迟到带。 */
        private Seg prevSeg;
        private long pendingOffset;

        Cursor(SessionLocal session, OmapTable table, TableFilter tableFilter,
                VectorizedFilter filter, CompareMode compareMode,
                int[] projCols, Value[] projConsts, Expression[] projExprs, int[] projToUnique,
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
            this.projExprs = projExprs;
            this.projToUnique = projToUnique;
            this.uniqueProjCols = uniqueProjCols;
            this.uniqueCount = uniqueCount;
            this.outLen = outLen;
            this.sortCols = sortCols;
            this.desc = desc;
            this.nullsFirst = nullsFirst;
            this.snapshotColumns = snapshotColumns;
            this.offset = offset;
            boolean exprs = false;
            for (Expression e : projExprs) {
                if (e != null) {
                    exprs = true;
                    break;
                }
            }
            this.hasExprs = exprs;            this.colToVec = new int[table.getColumns().length];
            Arrays.fill(colToVec, -1);
            for (int u = 0; u < uniqueCount; u++) {
                colToVec[uniqueProjCols[u]] = u;
            }
            adopt(built);
        }

        private void adopt(Built built) {
            this.snap = built.snap;
            this.orderedSegs = built.orderedSegs;
            this.nearBounds = built.nearBounds;
            this.extras = built.extras;
            this.extraKeys = built.extraKeys;
            this.extraIds = built.extraIds;
            this.extraIdx = 0;
            this.segPos = -1;
            this.cur = null;
            this.prevSeg = null;
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
                throw new IllegalStateException("有序流式重置时段区间不再次邻严格分离");
            }
            adopt(rebuilt);
        }

        private Value[] nextEmitted() {
            while (true) {
                Unit u = cur;
                if (u == null || u.exhausted || (u.initialized && u.ptr >= u.n)) {
                    // 推进到下一发射单元：单元 k = 段 k 的主区间 + 段 k-1 的迟到带
                    segPos++;
                    cur = u = segPos < orderedSegs.length
                            ? new Unit(segPos, segPos == 0 ? null : prevSeg, orderedSegs[segPos])
                            : null;
                }
                Value[] extraRow = extraIdx < extras.length ? extras[extraIdx] : null;
                if (u == null) {
                    if (extraRow == null) {
                        return null;
                    }
                    extraIdx++;
                    return emitExtra(extraRow, extraIds[extraIdx - 1]);
                }
                if (!u.initialized) {
                    if (extraRow != null) {
                        Value extraKey = extraRow[sortCols[0]];
                        boolean extraKeyNull = extraKey == null || extraKey == ValueNull.INSTANCE;
                        if (extraKeyNull) {
                            if (nullsFirst[0]) {
                                // NULLS FIRST：NULL 键活像先于一切段行发射。
                                extraIdx++;
                                return emitExtra(extraRow, extraIds[extraIdx - 1]);
                            }
                            // NULLS LAST：单元初始化后由 compareKeys 正确把 NULL 活像
                            // 排到全部段行之后。
                        } else if (compareFirstCol(session, compareMode, desc[0],
                                extraKey, nearBounds[u.index]) < 0) {
                            // 活像首列严格优于单元的近端界，必在整单元之前
                            extraIdx++;
                            return emitExtra(extraRow, extraIds[extraIdx - 1]);
                        }
                    }
                    initUnit(u);
                    continue;
                }
                // 单元已初始化且还有行
                if (extraRow != null) {
                    int cmp = compareKeys(session, compareMode, sortCols, desc, nullsFirst,
                            u.keys[u.ptr], RowId.ofSegment(
                                    (u.fromA[u.ptr] ? u.srcA.seg : u.segBSeg.seg)
                                            .getSegmentId(),
                                    u.rows[u.ptr]),
                            extraKeys[extraIdx], extraIds[extraIdx]);
                    if (cmp > 0) {
                        extraIdx++;
                        return emitExtra(extraRow, extraIds[extraIdx - 1]);
                    }
                }
                return emitUnitRow(u);
            }
        }

        /** 初始化发射单元：过滤排序当前段，切分主区间与迟到带，归并两个来源。 */
        private void initUnit(Unit u) {
            u.initialized = true;
            Seg segB = initSeg(u.segB,
                    u.index + 1 < orderedSegs.length ? nearBounds[u.index + 1] : null);
            prevSeg = segB;
            Seg srcA = u.srcA;
            int na = srcA == null ? 0 : srcA.n - srcA.carryStart;
            int nb = segB.carryStart;
            if (na + nb == 0) {
                u.n = 0;
                u.exhausted = true;
                return;
            }
            int[] rows = new int[na + nb];
            Value[][] keys = new Value[na + nb][];
            boolean[] fromA = new boolean[na + nb];
            int ia = srcA == null ? 0 : srcA.carryStart;
            int ib = 0;
            for (int t = 0; t < rows.length; t++) {
                boolean takeA;
                if (srcA == null || ia >= srcA.n) {
                    takeA = false;
                } else if (ib >= nb) {
                    takeA = true;
                } else {
                    int cmp = compareKeys(session, compareMode, sortCols, desc, nullsFirst,
                            srcA.keys[ia],
                            RowId.ofSegment(srcA.seg.getSegmentId(), srcA.rows[ia]),
                            segB.keys[ib],
                            RowId.ofSegment(segB.seg.getSegmentId(), segB.rows[ib]));
                    takeA = cmp < 0;
                }
                if (takeA) {
                    rows[t] = srcA.rows[ia];
                    keys[t] = srcA.keys[ia];
                    fromA[t] = true;
                    ia++;
                } else {
                    rows[t] = segB.rows[ib];
                    keys[t] = segB.keys[ib];
                    fromA[t] = false;
                    ib++;
                }
            }
            u.segBSeg = segB;
            u.rows = rows;
            u.keys = keys;
            u.fromA = fromA;
            u.n = rows.length;
        }

        /**
         * 段的发射准备：过滤可见行、按复合键排序，再按下一段的近端界切分——
         * 首键严格优于近端界的前缀是主区间（本单元发射），其余归入迟到带（下一单元发射）。
         */
        private Seg initSeg(ColumnSegment s, Value nearNext) {
            Seg st = new Seg(s);
            BitSet masked = snap.maskedBySegment.get(s.getSegmentId());
            int rc = s.getRowCount();
            int[] sel = new int[rc];
            int n = filter == null ? fillAll(sel, rc, masked)
                    : filter.filterSegment(s, sel, masked, session, compareMode);
            if (n == 0) {
                return st;
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
            // 迟到带切分：排序后首键非严格优于近端界的行是连续后缀，二分定位
            if (nearNext == null) {
                st.carryStart = n;
            } else {
                int lo = 0, hi = n;
                while (lo < hi) {
                    int mid = (lo + hi) >>> 1;
                    if (compareFirstCol(session, compareMode, desc[0],
                            st.keys[mid][0], nearNext) < 0) {
                        lo = mid + 1;
                    } else {
                        hi = mid;
                    }
                }
                st.carryStart = lo;
            }
            return st;
        }

        private Value[] emitUnitRow(Unit u) {
            // 行来源段决定延迟投影与 rowId：迟到带行读上一段、主区间行读当前段
            Seg st = u.fromA[u.ptr] ? u.srcA : u.segBSeg;
            int idx = u.rows[u.ptr];
            Value[] out = new Value[outLen];
            if (uniqueCount > 0) {
                if (st.projVecs == null) {
                    // 延迟物化：该段首次发出一行才读回投影列，不进驻留模型；
                    // 迟到带行与主区间行共享同一段的投影缓存。
                    st.projVecs = table.getStore().readDeferredProjection(
                            st.seg.getSegmentId(), uniqueProjCols);
                }
                for (int c = 0; c < outLen; c++) {
                    int uc = projToUnique[c];
                    out[c] = uc >= 0 ? st.projVecs[uc].get(idx) : projConsts[c];
                }
            } else {
                for (int c = 0; c < outLen; c++) {
                    out[c] = projConsts[c];
                }
            }
            if (hasExprs) {
                // 表达式投影：行发出时才逐行求值，只对该行付费
                tableFilter.set(new EmitRow(st.projVecs, colToVec, idx,
                        RowId.ofSegment(st.seg.getSegmentId(), idx)));
                evaluateExprs(out);
            }
            u.ptr++;
            return out;
        }

        private Value[] emitExtra(Value[] extraRow, long rowId) {
            Value[] out = new Value[outLen];
            for (int c = 0; c < outLen; c++) {
                int col = projCols[c];
                out[c] = col >= 0 ? extraRow[col] : projConsts[c];
            }
            if (hasExprs) {
                tableFilter.set(new EmitRow(extraRow, rowId));
                evaluateExprs(out);
            }
            return out;
        }

        private void evaluateExprs(Value[] out) {
            for (int c = 0; c < outLen; c++) {
                if (projExprs[c] != null) {
                    out[c] = projExprs[c].getValue(session);
                }
            }
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

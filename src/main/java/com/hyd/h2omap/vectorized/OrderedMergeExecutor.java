/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.vectorized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.result.SortOrder;
import org.h2.table.TableFilter;
import org.h2.value.CompareMode;
import org.h2.value.Value;

import com.hyd.h2omap.storage.ColumnSegment;
import com.hyd.h2omap.storage.RowId;
import com.hyd.h2omap.storage.VectorizedSnapshot;
import com.hyd.h2omap.table.OmapTable;
import com.hyd.h2omap.vector.ColumnVector;

/**
 * 归并流式执行（M24）：排序首列段间 zone 次邻分离不成立（随机写入形态）时，
 * 无 LIMIT 的单表 ORDER BY 不再整条交回 H2 全量排序，改为全体参与段的
 * 排序键流归并发射——每段段内按复合键排序（rowId 定序），k 路小底堆逐行归并；
 * 活像与开放段行作为一条附加键流参与归并。投影延迟物化、谓词列集、OFFSET、
 * 表达式发射期求值与 M20/M22 完全同口径。
 *
 * <p>与有序流式（M20/M22）的分工：调用方先试有序流式（次邻分离，任一时刻只
 * 驻留一个发射单元的键数据），失败后由本执行器接管。归并不依赖段间任何先验
 * 顺序，因此也不需要 zone map 与 hasNull 门控——全体段头在同一个堆里，NULL 组
 * 由复合键比较器统一排序，不存在流式单元推进拆散 NULL 组的问题。
 *
 * <p>内存上界（简单版的容量控制点）：归并期全体段键流必须同时驻留，装箱字节按
 * {@code omap.mergeStreamMaxBytes}（默认 2 GiB）设限，超限整条交回 H2 行式路径。
 * 估算两级：先按行数粗估（每行 12 B + 每排序键 64 B）在装箱前拒绝；装箱后按
 * {@code Value.getMemory()} 精确累加再校验一次，宽键（装箱字节远超粗估）最坏
 * 多装一段即放弃。排序阶段读键列走向量化快照的临时切片，不进驻留模型，与 M16
 * 同口径；重复查询命中 M19 切片缓存后键列零读盘。
 */
final class OrderedMergeExecutor {

    /** 键流量上限的系统属性名（字节），默认 {@link #DEFAULT_MAX_BYTES}，主要供测试收缩。 */
    static final String MAX_BYTES_PROPERTY = "omap.mergeStreamMaxBytes";
    private static final long DEFAULT_MAX_BYTES = 2L << 30;
    /** 每行粗估开销：行号槽 12 B。 */
    private static final long EST_ROW_OVERHEAD = 12;
    /** 每排序键的粗估开销：装箱后小于 64 B 的占绝大多数，宽键由精确累加兜底。 */
    private static final long EST_KEY_OVERHEAD = 64;

    private OrderedMergeExecutor() {
    }

    static OrderedStreamingRowSource tryClaim(
            SessionLocal session, TableFilter tableFilter, Expression condition,
            List<Expression> expressions, int resultColumnCount, long offset,
            SortOrder sortOrder) {
        OrderedStreamingExecutor.Prepared p = OrderedStreamingExecutor.prepare(session,
                tableFilter, condition, expressions, resultColumnCount, sortOrder);
        if (p == null) {
            return null;
        }
        Built built = build(session, p);
        if (built == null) {
            return null;
        }
        p.table.getStore().recordMergeStreamingClaim();
        return new MergeCursor(session, p, offset, built);
    }

    /** 一次归并执行的不可变状态：全体段键流与排好序的活像键流。 */
    private static final class Built {
        final Stream[] segs;
        final Stream extras;

        Built(Stream[] segs, Stream extras) {
            this.segs = segs;
            this.extras = extras;
        }
    }

    /**
     * 物化全体键流。返回 {@code null} 表示键流量超出上限（或活像粗估超限），
     * 调用方交回 H2 行式路径。超限判定逐段进行：粗估在装箱前拒绝行数驱动的
     * 超限，装箱后按实际装箱字节修正，任何一段越界立即放弃。
     */
    private static Built build(SessionLocal session, OrderedStreamingExecutor.Prepared p) {
        CompareMode cm = p.table.getCompareMode();
        long maxBytes = Long.getLong(MAX_BYTES_PROPERTY, DEFAULT_MAX_BYTES);
        VectorizedSnapshot snap = OmapVectorizedExecutor.takeSnapshot(
                p.table, p.filter, p.tableFilter, p.snapshotColumns);
        long est = 0;
        List<Stream> streams = new ArrayList<>();
        for (ColumnSegment seg : snap.segments) {
            int rc = seg.getRowCount();
            if (rc == 0) {
                continue;
            }
            BitSet masked = snap.maskedBySegment.get(seg.getSegmentId());
            if (masked != null && masked.cardinality() >= rc) {
                continue;
            }
            int[] sel = new int[rc];
            int n = p.filter == null
                    ? OrderedStreamingExecutor.fillAll(sel, rc, masked)
                    : p.filter.filterSegment(seg, sel, masked, session, cm);
            if (n == 0) {
                continue;
            }
            est += (long) n * (EST_ROW_OVERHEAD + EST_KEY_OVERHEAD * p.sortCols.length);
            if (est > maxBytes) {
                return null;
            }
            Stream st = sortSegment(session, p, cm, seg, sel, n);
            est += st.boxedKeyBytes
                    - (long) n * EST_KEY_OVERHEAD * p.sortCols.length;
            if (est > maxBytes) {
                return null;
            }
            streams.add(st);
        }
        // 活像与开放段行：过滤后按复合键排序，rowId 定序，整流参与归并
        List<Value[]> keptRows = new ArrayList<>();
        List<Value[]> keptKeys = new ArrayList<>();
        List<Long> keptIds = new ArrayList<>();
        for (int i = 0; i < snap.extraRows.size(); i++) {
            Value[] row = snap.extraRows.get(i);
            if (p.filter != null && !p.filter.evalRow(row, session, cm)) {
                continue;
            }
            keptRows.add(row);
            keptKeys.add(OrderedStreamingExecutor.extractKeys(row, p.sortCols));
            keptIds.add(snap.extraRowIds[i]);
        }
        int n = keptRows.size();
        est += (long) n * (EST_ROW_OVERHEAD + EST_KEY_OVERHEAD * p.sortCols.length);
        if (est > maxBytes) {
            return null;
        }
        Stream extras = null;
        if (n > 0) {
            Integer[] ord = new Integer[n];
            for (int i = 0; i < n; i++) {
                ord[i] = i;
            }
            Arrays.sort(ord, (x, y) -> OrderedStreamingExecutor.compareKeys(session, cm,
                    p.sortCols, p.desc, p.nullsFirst,
                    keptKeys.get(x), keptIds.get(x), keptKeys.get(y), keptIds.get(y)));
            Value[][] extraRows = new Value[n][];
            Value[][] extraKeys = new Value[n][];
            long[] extraIds = new long[n];
            long bytes = 0;
            for (int i = 0; i < n; i++) {
                extraRows[i] = keptRows.get(ord[i]);
                extraKeys[i] = keptKeys.get(ord[i]);
                extraIds[i] = keptIds.get(ord[i]);
                for (Value k : extraKeys[i]) {
                    bytes += k.getMemory();
                }
            }
            extras = Stream.ofExtras(extraRows, extraKeys, extraIds, bytes);
        }
        return new Built(streams.toArray(new Stream[0]), extras);
    }

    /**
     * 段键流物化：读排序键列、装箱、按复合键排序（rowId 定序）。
     * 返回流的 {@code boxedKeyBytes} 含装箱值与每行键数组的实际开销。
     */
    private static Stream sortSegment(SessionLocal session, OrderedStreamingExecutor.Prepared p,
            CompareMode cm, ColumnSegment seg, int[] sel, int n) {
        int kc = p.sortCols.length;
        ColumnVector[] vecs = new ColumnVector[kc];
        for (int i = 0; i < kc; i++) {
            vecs[i] = seg.getColumn(p.sortCols[i]);
        }
        Value[][] keys = new Value[n][];
        long bytes = (long) n * (16 + 8L * kc);
        for (int k = 0; k < n; k++) {
            Value[] key = new Value[kc];
            for (int i = 0; i < kc; i++) {
                Value v = vecs[i].get(sel[k]);
                key[i] = v;
                bytes += v.getMemory();
            }
            keys[k] = key;
        }
        Integer[] ord = new Integer[n];
        for (int k = 0; k < n; k++) {
            ord[k] = k;
        }
        final Value[][] fkeys = keys;
        final int[] fsel = sel;
        Arrays.sort(ord, (x, y) -> OrderedStreamingExecutor.compareKeys(session, cm,
                p.sortCols, p.desc, p.nullsFirst,
                fkeys[x], RowId.ofSegment(seg.getSegmentId(), fsel[x]),
                fkeys[y], RowId.ofSegment(seg.getSegmentId(), fsel[y])));
        int[] rows = new int[n];
        Value[][] sorted = new Value[n][];
        for (int k = 0; k < n; k++) {
            rows[k] = fsel[ord[k]];
            sorted[k] = fkeys[ord[k]];
        }
        return Stream.ofSegment(seg, rows, sorted, bytes);
    }

    /** 一条键流：段内过滤后的行按复合键排序，或整流排好序的活像/开放段行。 */
    private static final class Stream {
        /** 段流非 null；活像流为 null。 */
        final ColumnSegment seg;
        final int[] rows;
        final Value[][] keys;
        /** 活像流的整行与 rowId；段流为 null。 */
        final Value[][] extraRows;
        final long[] extraIds;
        /** 装箱键字节（键流量上界的精确项）。 */
        final long boxedKeyBytes;
        int n;
        int ptr;
        /** 段流的延迟投影缓存，该段行首次发出才读回。 */
        ColumnVector[] projVecs;

        private Stream(ColumnSegment seg, int[] rows, Value[][] keys, Value[][] extraRows,
                long[] extraIds, long boxedKeyBytes, int n) {
            this.seg = seg;
            this.rows = rows;
            this.keys = keys;
            this.extraRows = extraRows;
            this.extraIds = extraIds;
            this.boxedKeyBytes = boxedKeyBytes;
            this.n = n;
        }

        static Stream ofSegment(ColumnSegment seg, int[] rows, Value[][] keys, long boxedKeyBytes) {
            return new Stream(seg, rows, keys, null, null, boxedKeyBytes, rows.length);
        }

        static Stream ofExtras(Value[][] extraRows, Value[][] keys, long[] extraIds,
                long boxedKeyBytes) {
            return new Stream(null, null, keys, extraRows, extraIds, boxedKeyBytes, extraRows.length);
        }
    }

    private static final class MergeCursor implements OrderedStreamingRowSource {

        private final SessionLocal session;
        private final OrderedStreamingExecutor.Prepared p;
        private final CompareMode compareMode;
        private final OmapTable table;
        private final TableFilter tableFilter;
        private final int[] projCols;
        private final Value[] projConsts;
        private final Expression[] projExprs;
        private final int[] projToUnique;
        private final int[] uniqueProjCols;
        private final int uniqueCount;
        private final int outLen;
        private final boolean hasExprs;
        private final int[] colToVec;
        private final long offset;

        private Stream[] heap;
        private int hn;
        private long pendingOffset;

        MergeCursor(SessionLocal session, OrderedStreamingExecutor.Prepared p, long offset,
                Built built) {
            this.session = session;
            this.p = p;
            this.compareMode = p.table.getCompareMode();
            this.table = p.table;
            this.tableFilter = p.tableFilter;
            this.projCols = p.plan.projCols;
            this.projConsts = p.plan.projConsts;
            this.projExprs = p.plan.projExprs;
            this.projToUnique = p.plan.projToUnique;
            this.uniqueProjCols = p.plan.uniqueProjCols;
            this.uniqueCount = p.plan.uniqueCount;
            this.outLen = p.plan.outLen;
            this.hasExprs = p.plan.hasExprs;
            this.colToVec = new int[table.getColumns().length];
            Arrays.fill(colToVec, -1);
            for (int u = 0; u < uniqueCount; u++) {
                colToVec[uniqueProjCols[u]] = u;
            }
            this.offset = offset;
            adopt(built);
        }

        private void adopt(Built built) {
            this.heap = new Stream[built.segs.length + (built.extras != null ? 1 : 0)];
            this.hn = 0;
            for (Stream st : built.segs) {
                push(st);
            }
            if (built.extras != null) {
                push(built.extras);
            }
            this.pendingOffset = offset;
        }

        @Override
        public Value[] nextRow() {
            while (hn > 0) {
                Stream top = heap[0];
                if (pendingOffset > 0) {
                    pendingOffset--;
                    advance();
                    continue;
                }
                Value[] out = emit(top);
                advance();
                return out;
            }
            return null;
        }

        @Override
        public void reset() {
            Built rebuilt = build(session, p);
            // 与 M20 同口径：结果复用只在没有写入时触发，无写入则键流量不变，
            // 重建必然成功；可达即判据被破坏，显式暴露。
            if (rebuilt == null) {
                throw new IllegalStateException("归并流式重置时键流量超出上限");
            }
            adopt(rebuilt);
        }

        private void advance() {
            Stream st = heap[0];
            st.ptr++;
            if (st.ptr >= st.n) {
                heap[0] = heap[--hn];
                if (hn > 0) {
                    siftDown(0);
                }
            } else {
                siftDown(0);
            }
        }

        private void push(Stream st) {
            heap[hn++] = st;
            siftUp(hn - 1);
        }

        private void siftUp(int i) {
            while (i > 0) {
                int par = (i - 1) >>> 1;
                if (!less(heap[i], heap[par])) {
                    return;
                }
                Stream t = heap[i];
                heap[i] = heap[par];
                heap[par] = t;
                i = par;
            }
        }

        private void siftDown(int i) {
            while (true) {
                int l = 2 * i + 1;
                int r = l + 1;
                int m = i;
                if (l < hn && less(heap[l], heap[m])) {
                    m = l;
                }
                if (r < hn && less(heap[r], heap[m])) {
                    m = r;
                }
                if (m == i) {
                    return;
                }
                Stream t = heap[i];
                heap[i] = heap[m];
                heap[m] = t;
                i = m;
            }
        }

        /** 堆序：当前头的复合键，并列按 rowId 定序（对齐 H2 稳定扫描序）。 */
        private boolean less(Stream a, Stream b) {
            return OrderedStreamingExecutor.compareKeys(session, compareMode,
                    p.sortCols, p.desc, p.nullsFirst,
                    a.keys[a.ptr], rowIdOf(a), b.keys[b.ptr], rowIdOf(b)) < 0;
        }

        private static long rowIdOf(Stream st) {
            return st.seg != null
                    ? RowId.ofSegment(st.seg.getSegmentId(), st.rows[st.ptr])
                    : st.extraIds[st.ptr];
        }

        private Value[] emit(Stream st) {
            Value[] out = new Value[outLen];
            if (st.seg != null) {
                int idx = st.rows[st.ptr];
                if (uniqueCount > 0) {
                    if (st.projVecs == null) {
                        // 延迟物化：该段首次发出一行才读回投影列，不进驻留模型；
                        // 同段全部行共享该段的投影缓存。
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
                    tableFilter.set(new EmitRow(st.projVecs, colToVec, idx,
                            RowId.ofSegment(st.seg.getSegmentId(), idx)));
                    evaluateExprs(out);
                }
            } else {
                Value[] row = st.extraRows[st.ptr];
                for (int c = 0; c < outLen; c++) {
                    int col = projCols[c];
                    out[c] = col >= 0 ? row[col] : projConsts[c];
                }
                if (hasExprs) {
                    tableFilter.set(new EmitRow(row, st.extraIds[st.ptr]));
                    evaluateExprs(out);
                }
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
}

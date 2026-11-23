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
import org.h2.result.Row;
import org.h2.table.TableFilter;
import org.h2.value.CompareMode;
import org.h2.value.Value;

import com.hyd.h2omap.index.OmapScanIndex;
import com.hyd.h2omap.storage.ColumnSegment;
import com.hyd.h2omap.storage.RowId;
import com.hyd.h2omap.storage.VectorizedSnapshot;
import com.hyd.h2omap.table.OmapTable;
import com.hyd.h2omap.vector.ColumnVector;

/**
 * 平面流式执行（M23）：{@code 无 LIMIT 无 ORDER BY 的单表平面查询}流式输出，
 * 不建 {@code LocalResult}、不全量物化。
 *
 * <p>动机：行式全表扫描（如 {@code SELECT * FROM 大表}）对逐出段会把整表读回并
 * 全量装箱物化，服务端在发出首行前就付出全部代价。本执行器把发射改成按需：
 * 客户端取一行才推进一次，停止取数扫描即停，未触碰的段零读取。
 *
 * <p>发射顺序与既有无序口径一致（M16 的无序 LIMIT 先例）：先扫已驻留段（段号升序），
 * 再发 delta 新像与开放段行（{@code extraRows}，已在内存），最后按需读回逐出段
 * （段号升序）。逐出段首次取行才经向量化快照的临时切片读回列，不进驻留模型；
 * {@code OFFSET} 在发射流上跳过。因此无 {@code ORDER BY} 的行序不再隐含段号升序，
 * 依赖插入序的翻页应显式 {@code ORDER BY} 走 M20/M22。
 *
 * <p>投影口径与 M20-2 一致：单表列引用、常量、确定表达式（行发出时经
 * {@code TableFilter.set(Row)} 就位后逐行求值，见 {@link ProjectionPlan}）。
 * 带 {@code LIMIT} 的查询仍走 {@link FlatExecutor} 的物化路径（其有序模式与
 * 驻留优先 LIMIT 扫描已就位），带 {@code ORDER BY} 走 {@link OrderedStreamingExecutor}。
 * 形态不合法时 {@link #tryClaim} 返回 {@code null}，调用方无缝交回 H2 原路径。
 */
final class FlatStreamingExecutor {

    private FlatStreamingExecutor() {
    }

    static FlatStreamingRowSource tryClaim(
            SessionLocal session, TableFilter tableFilter, Expression condition,
            List<Expression> expressions, int resultColumnCount, long offset) {
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
        OmapTable table = (OmapTable) tableFilter.getTable();

        ProjectionPlan plan = ProjectionPlan.claim(session, tableFilter, expressions, resultColumnCount);
        if (plan == null) {
            return null;
        }

        VectorizedFilter filter = null;
        if (condition != null) {
            filter = VectorizedFilter.compile(condition, tableFilter, session);
            if (filter == null) {
                    return null;
            }
        }

        Built built = build(session, table, tableFilter, filter, plan);
        table.getStore().recordFlatStreamingClaim();
        return new Cursor(session, table, tableFilter, filter, plan, offset, built);
    }

    /** 一次流式执行的不可变状态：快照、驻留/逐出段分组与过滤后的活像行。 */
    private static final class Built {
        final VectorizedSnapshot snap;
        final ColumnSegment[] residentSegs;
        final ColumnSegment[] lazySegs;
        final Value[][] extras;
        final long[] extraIds;

        Built(VectorizedSnapshot snap, ColumnSegment[] residentSegs, ColumnSegment[] lazySegs,
                Value[][] extras, long[] extraIds) {
            this.snap = snap;
            this.residentSegs = residentSegs;
            this.lazySegs = lazySegs;
            this.extras = extras;
            this.extraIds = extraIds;
        }
    }

    /**
     * 截取快照并分好发射组：已驻留段在前、逐出段在后，活像行过滤后夹在中间
     * （与 {@link FlatExecutor} 无序路径的发射序一致）。
     */
    private static Built build(SessionLocal session, OmapTable table, TableFilter tableFilter,
            VectorizedFilter filter, ProjectionPlan plan) {
        int[] snapshotColumns = OmapVectorizedExecutor.unionColumns(
                plan.uniqueProjCols, filter != null ? filter.referencedColumns() : null);
        VectorizedSnapshot snap = OmapVectorizedExecutor.takeSnapshot(table, filter, tableFilter,
                snapshotColumns);
        List<ColumnSegment> resident = new ArrayList<>();
        List<ColumnSegment> lazy = new ArrayList<>();
        for (ColumnSegment seg : snap.segments) {
            if (seg.getRowCount() == 0) {
                continue;
            }
            BitSet m = snap.maskedBySegment.get(seg.getSegmentId());
            if (m != null && m.cardinality() >= seg.getRowCount()) {
                continue;
            }
            (seg.isLoaded() ? resident : lazy).add(seg);
        }
        List<Value[]> kept = new ArrayList<>();
        List<Long> keptIds = new ArrayList<>();
        for (int i = 0; i < snap.extraRows.size(); i++) {
            Value[] row = snap.extraRows.get(i);
            if (filter != null && !filter.evalRow(row, session, table.getCompareMode())) {
                continue;
            }
            kept.add(row);
            keptIds.add(snap.extraRowIds[i]);
        }
        Value[][] extras = kept.toArray(new Value[0][]);
        long[] extraIds = new long[extras.length];
        for (int i = 0; i < extras.length; i++) {
            extraIds[i] = keptIds.get(i);
        }
        return new Built(snap, resident.toArray(new ColumnSegment[0]),
                lazy.toArray(new ColumnSegment[0]), extras, extraIds);
    }

    private static final class Cursor implements FlatStreamingRowSource {

        private final SessionLocal session;
        private final OmapTable table;
        private final TableFilter tableFilter;
        private final VectorizedFilter filter;
        private final ProjectionPlan plan;
        private final CompareMode compareMode;
        private final int outLen;
        private final int[] projCols;
        private final Value[] projConsts;
        private final Expression[] projExprs;
        private final int[] projToUnique;
        private final int[] uniqueProjCols;
        private final int uniqueCount;
        private final boolean hasExprs;
        private final int[] colToVec;
        private final long offset;

        private VectorizedSnapshot snap;
        private ColumnSegment[] residentSegs;
        private ColumnSegment[] lazySegs;
        private Value[][] extras;
        private long[] extraIds;
        private long pendingOffset;
        /** 0：驻留段；1：活像；2：逐出段；3：结束。 */
        private int phase;
        private int segPos;
        /** 当前段的可见行下标数组与游标；首次取行才初始化（见 {@code colVecs}）。 */
        private int[] sel;
        private int selCount;
        private int selPtr;
        private int rowPtr;
        /** 当前段的列向量；{@code null} 表示当前段尚未做发射准备。 */
        private ColumnVector[] colVecs;

        Cursor(SessionLocal session, OmapTable table, TableFilter tableFilter,
                VectorizedFilter filter, ProjectionPlan plan, long offset, Built built) {
            this.session = session;
            this.table = table;
            this.tableFilter = tableFilter;
            this.filter = filter;
            this.plan = plan;
            this.compareMode = table.getCompareMode();
            this.outLen = plan.outLen;
            this.projCols = plan.projCols;
            this.projConsts = plan.projConsts;
            this.projExprs = plan.projExprs;
            this.projToUnique = plan.projToUnique;
            this.uniqueProjCols = plan.uniqueProjCols;
            this.uniqueCount = plan.uniqueCount;
            this.hasExprs = plan.hasExprs;
            this.colToVec = new int[table.getColumns().length];
            Arrays.fill(colToVec, -1);
            for (int u = 0; u < uniqueCount; u++) {
                colToVec[uniqueProjCols[u]] = u;
            }
            this.offset = offset;
            adopt(built);
        }

        private void adopt(Built built) {
            this.snap = built.snap;
            this.residentSegs = built.residentSegs;
            this.lazySegs = built.lazySegs;
            this.extras = built.extras;
            this.extraIds = built.extraIds;
            this.pendingOffset = offset;
            this.phase = 0;
            this.segPos = 0;
            resetSegState();
        }

        private void resetSegState() {
            this.selCount = 0;
            this.selPtr = 0;
            this.rowPtr = 0;
            this.colVecs = null;
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
            // 与 M20 同口径：lazy 结果经一次性验证不可达（H2 对 lazy 结果不复用），
            // 属防御性实现；重置即重截快照重放 OFFSET。
            adopt(build(session, table, tableFilter, filter, plan));
        }

        private Value[] nextEmitted() {
            while (true) {
                Value[] row;
                if (phase == 0) {
                    if (segPos >= residentSegs.length) {
                        phase = 1;
                        segPos = 0;
                        continue;
                    }
                    row = emitSegmentRow(residentSegs[segPos]);
                    if (row == null) {
                        segPos++;
                        resetSegState();
                        continue;
                    }
                    return row;
                }
                if (phase == 1) {
                    if (segPos >= extras.length) {
                        phase = 2;
                        segPos = 0;
                        resetSegState();
                        continue;
                    }
                    row = emitExtra(extras[segPos], extraIds[segPos]);
                    segPos++;
                    return row;
                }
                if (phase == 2) {
                    if (segPos >= lazySegs.length) {
                        phase = 3;
                        continue;
                    }
                    row = emitSegmentRow(lazySegs[segPos]);
                    if (row == null) {
                        segPos++;
                        resetSegState();
                        continue;
                    }
                    return row;
                }
                return null;
            }
        }

        /**
         * 从一个段取下一行：段首次被触碰才做发射准备（读列向量——逐出段此时触发
         * 临时切片读——并跑一次段级过滤），返回 {@code null} 表示该段已发尽。
         */
        private Value[] emitSegmentRow(ColumnSegment seg) {
            if (colVecs == null) {
                initSegment(seg);
            }
            int rowCount = seg.getRowCount();
            if (filter == null) {
                BitSet masked = snap.maskedBySegment.get(seg.getSegmentId());
                while (rowPtr < rowCount) {
                    int i = rowPtr++;
                    if (masked != null && masked.get(i)) {
                        continue;
                    }
                    return emitSegRow(seg, i);
                }
                return null;
            }
            if (selPtr < selCount) {
                return emitSegRow(seg, sel[selPtr++]);
            }
            return null;
        }

        /** 段的发射准备：读列向量引用并跑一次段级过滤。 */
        private void initSegment(ColumnSegment seg) {
            colVecs = new ColumnVector[uniqueCount];
            for (int u = 0; u < uniqueCount; u++) {
                colVecs[u] = seg.getColumn(uniqueProjCols[u]);
            }
            if (filter == null) {
                return;
            }
            int rowCount = seg.getRowCount();
            BitSet masked = snap.maskedBySegment.get(seg.getSegmentId());
            if (sel == null || sel.length < rowCount) {
                sel = new int[rowCount];
            }
            selCount = filter.filterSegment(seg, sel, masked, session, compareMode);
            selPtr = 0;
        }

        private Value[] emitSegRow(ColumnSegment seg, int idx) {
            Value[] out = new Value[outLen];
            for (int c = 0; c < outLen; c++) {
                int u = projToUnique[c];
                out[c] = u >= 0 ? colVecs[u].get(idx) : projConsts[c];
            }
            if (hasExprs) {
                tableFilter.set(new EmitRow(colVecs, colToVec, idx,
                        RowId.ofSegment(seg.getSegmentId(), idx)));
                for (int c = 0; c < outLen; c++) {
                    if (projExprs[c] != null) {
                        out[c] = projExprs[c].getValue(session);
                    }
                }
            }
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
                for (int c = 0; c < outLen; c++) {
                    if (projExprs[c] != null) {
                        out[c] = projExprs[c].getValue(session);
                    }
                }
            }
            return out;
        }
    }
}

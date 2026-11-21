package com.hyd.h2omap.vectorized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import org.h2.command.query.QueryOrderBy;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.result.LocalResult;
import org.h2.result.SortOrder;
import org.h2.table.TableFilter;
import org.h2.value.CompareMode;
import org.h2.value.Value;
import org.h2.value.ValueNull;

import com.hyd.h2omap.index.OmapScanIndex;
import com.hyd.h2omap.storage.ColumnSegment;
import com.hyd.h2omap.storage.ColumnarStore;
import com.hyd.h2omap.table.OmapTable;
import com.hyd.h2omap.vector.ColumnVector;

/**
 * 非聚合单表投影的下推执行：投影项限单表列引用或常量，可带 {@code LIMIT}/{@code OFFSET}；
 * {@code ORDER BY} 单列加常量 {@code LIMIT} 时交 {@link OrderedExecutor} 有序模式。
 * 主键/唯一索引进 {@code tryExecuteFlat} 前已被拒绝，这里只走扫描索引。执行期无存储锁，
 * 段内容来自向量化快照；逐出段按请求列集做部分列读，首次读列才触发磁盘读取。
 */
final class FlatExecutor {

    private FlatExecutor() {
    }

    static boolean execute(
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
        int[] snapshotColumns = OmapVectorizedExecutor.unionColumns(
                Arrays.copyOf(uniqueProjCols, uniqueCount),
                new int[] { preSortCol },
                filter != null ? filter.referencedColumns() : null);
        ColumnarStore.VectorizedSnapshot snap = OmapVectorizedExecutor.takeSnapshot(table, filter, tableFilter, snapshotColumns);

        if (sortOrder != null) {
            return OrderedExecutor.executeOrdered(session, snap, filter, sortOrder, tableFilter, projCols, projConsts,
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
}

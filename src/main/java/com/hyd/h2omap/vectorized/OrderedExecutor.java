package com.hyd.h2omap.vectorized;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.h2.command.query.QueryOrderBy;
import org.h2.engine.SessionLocal;
import org.h2.result.LocalResult;
import org.h2.result.SortOrder;
import org.h2.table.TableFilter;
import org.h2.value.CompareMode;
import org.h2.value.Value;
import org.h2.value.ValueNull;

import com.hyd.h2omap.stats.ZoneMap;
import com.hyd.h2omap.storage.ColumnSegment;
import com.hyd.h2omap.storage.ColumnarStore;
import com.hyd.h2omap.storage.VectorizedSnapshot;
import com.hyd.h2omap.storage.RowId;
import com.hyd.h2omap.vector.ColumnVector;

/**
 * 有序模式：{@code ORDER BY 单列 + LIMIT}。
 *
 * <p>段按排序列在 zone map 上的边界排序后依次扫描，用容量为 {@code limit + offset} 的堆
 * 留下前 N 行。堆满时，若下一个段内最好的一行也不优于堆内第 N 名，则整段跳过。
 * 物理聚簇与排序键同向时只需扫一两段，结果与全排序一致；同向不成立时也能得到
 * 正确结果，只是跳过不了段。形态不合法（多列排序、无 LIMIT、上限超 10000）返回
 * {@code false} 交回调用方回退。执行期无存储锁，段内容来自向量化快照。
 */
final class OrderedExecutor {

    private OrderedExecutor() {
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

    static boolean executeOrdered(SessionLocal session, VectorizedSnapshot snap,
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
}

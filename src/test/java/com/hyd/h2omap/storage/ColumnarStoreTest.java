/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.storage;

import java.util.Arrays;
import java.util.List;

import org.h2.result.Row;
import org.h2.table.Column;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueInteger;
import org.h2.value.ValueVarchar;
import org.junit.jupiter.api.Test;

import com.hyd.h2omap.engine.OmapOptions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * M1：段真正产生的单元测试。
 *
 * <p>直接驱动 {@link ColumnarStore}，断言段数与 rowId 的稳定性，
 * 这些无法只靠 SQL 层面的行为确认。
 */
class ColumnarStoreTest {

    private static ColumnarStore newStore(int segmentSize) {
        return new ColumnarStore(
                new Column[] {
                        new Column("A", TypeInfo.getTypeInfo(Value.INTEGER)),
                        new Column("B", TypeInfo.getTypeInfo(Value.VARCHAR)) },
                OmapOptions.from(List.of("segmentSize=" + segmentSize)));
    }

    private static Value[] row(int a, String b) {
        return new Value[] { ValueInteger.get(a), ValueVarchar.get(b) };
    }

    private static String column(Row row, int index) {
        return row.getValueList()[index].getString();
    }

    @Test
    void freezesWhenSegmentIsFull() {
        ColumnarStore store = newStore(2);
        store.append(row(1, "a"));
        assertEquals(0, store.segments().size());
        store.append(row(2, "b"));
        assertEquals(1, store.segments().size());
        store.append(row(3, "c"));
        assertEquals(1, store.segments().size());
        store.append(row(4, "d"));
        assertEquals(2, store.segments().size());
        assertEquals(4, store.getRowCount());
    }

    /**
     * 段号与列表下标解耦：段的列表位置不再决定 rowId，冻结前后 rowId 不变。
     */
    @Test
    void rowIdStaysValidAfterFreeze() {
        ColumnarStore store = newStore(2);
        long first = store.append(row(1, "a"));
        long second = store.append(row(2, "b"));
        long third = store.append(row(3, "c"));
        assertEquals(0, RowId.segmentId(first));
        assertEquals(0, RowId.segmentId(second));
        assertEquals(1, RowId.segmentId(third));
        assertEquals("1", column(store.materialize(first), 0));
        assertEquals("2", column(store.materialize(second), 0));
        assertEquals("3", column(store.materialize(third), 0));
        assertEquals(first, store.materialize(first).getKey());
        assertEquals(second, store.materialize(second).getKey());
    }

    @Test
    void updateAndDeleteFrozenRows() {
        ColumnarStore store = newStore(2);
        long first = store.append(row(1, "a"));
        long second = store.append(row(2, "b"));
        store.append(row(3, "c"));
        assertEquals(1, store.segments().size());

        store.update(first, row(1, "x"));
        assertEquals("x", column(store.materialize(first), 1));
        assertEquals("b", column(store.materialize(second), 1));

        store.remove(second);
        assertNull(store.materialize(second));
        assertEquals(2, store.getRowCount());
        assertEquals("3", column(store.materialize(RowId.ofSegment(1, 0)), 0));
    }

    @Test
    void updateAndDeleteOpenRows() {
        ColumnarStore store = newStore(4);
        long first = store.append(row(1, "a"));
        long second = store.append(row(2, "b"));
        assertEquals(0, store.segments().size());

        store.update(first, row(1, "x"));
        assertEquals("x", column(store.materialize(first), 1));
        // 开放段里的删除在冻结前不能丢：否则冻结后那行会重新可见。
        store.remove(second);
        assertNull(store.materialize(second));
        store.append(row(3, "c"));
        store.append(row(4, "d"));
        assertEquals(1, store.segments().size());
        assertNull(store.materialize(second));
        assertEquals(3, store.getRowCount());
        assertEquals("1", column(store.materialize(first), 0));
        assertEquals("3", column(store.materialize(RowId.ofSegment(0, 2)), 0));
    }

    @Test
    void truncateResetsSegmentsButKeepsRowIdsUnique() {
        ColumnarStore store = newStore(2);
        long before = store.append(row(1, "a"));
        store.append(row(2, "b"));
        assertEquals(1, store.segments().size());
        assertEquals(2, store.truncate());
        assertEquals(0, store.getRowCount());
        assertEquals(0, store.segments().size());
        long after = store.append(row(3, "c"));
        // 段号不回收，清空后重新分配的 rowId 不会和之前的撞车。
        assertNotEquals(before, after);
        assertEquals("3", column(store.materialize(after), 0));
    }

    @Test
    void collectsRowIdsAcrossSegmentsInOrder() {
        ColumnarStore store = newStore(2);
        long r0 = store.append(row(1, "a"));
        long r1 = store.append(row(2, "b"));
        long r2 = store.append(row(3, "c"));
        long r3 = store.append(row(4, "d"));
        long r4 = store.append(row(5, "e"));
        long[] ids = store.collectVisibleRowIds(false);
        assertEquals(Arrays.asList(r0, r1, r2, r3, r4),
                Arrays.asList(ids[0], ids[1], ids[2], ids[3], ids[4]));
        long[] reversed = store.collectVisibleRowIds(true);
        assertEquals(Arrays.asList(r4, r3, r2, r1, r0),
                Arrays.asList(reversed[0], reversed[1], reversed[2], reversed[3], reversed[4]));
    }
}

/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.storage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.h2.result.Row;
import org.h2.table.Column;
import org.h2.value.TypeInfo;
import org.h2.value.CompareMode;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueInteger;
import org.h2.value.ValueNull;
import org.h2.value.ValueVarchar;
import org.junit.jupiter.api.Test;

import com.hyd.h2omap.engine.OmapOptions;
import com.hyd.h2omap.vector.ArrayVector;
import com.hyd.h2omap.vector.ColumnVector;
import com.hyd.h2omap.vector.IntVector;
import com.hyd.h2omap.vector.ValueVector;
import com.hyd.h2omap.vector.VarcharVector;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * M1：段真正产生的单元测试。
 *
 * <p>直接驱动 {@link ColumnarStore}，断言段数与 rowId 的稳定性，
 * 以及数组列的向量实现（前缀和下标、NULL 与空数组的区分、合并后的长度变化），
 * 这些无法只靠 SQL 层面的行为确认。
 */
class ColumnarStoreTest {

    private static ColumnarStore newStore(int segmentSize) {
        return newStore(segmentSize, Integer.MAX_VALUE);
    }

    private static ColumnarStore newStore(int segmentSize, int compactionMinRows) {
        return new ColumnarStore(
                new Column[] {
                        new Column("A", TypeInfo.getTypeInfo(Value.INTEGER)),
                        new Column("B", TypeInfo.getTypeInfo(Value.VARCHAR)) },
                OmapOptions.from(List.of("segmentSize=" + segmentSize,
                        "compactionMinRows=" + compactionMinRows)),
                CompareMode.getInstance(null, 0));
    }

    private static Value[] row(int a, String b) {
        return new Value[] { ValueInteger.get(a), ValueVarchar.get(b) };
    }

    private static String column(Row row, int index) {
        return row.getValueList()[index].getString();
    }

    private static final TypeInfo INTEGER_TYPE = TypeInfo.getTypeInfo(Value.INTEGER);

    /** 单列、列为 {@code INTEGER ARRAY} 的存储。 */
    private static ColumnarStore newArrayStore(int segmentSize, int compactionMinRows) {
        return new ColumnarStore(
                new Column[] { new Column("A",
                        TypeInfo.getTypeInfo(Value.ARRAY, -1, -1, INTEGER_TYPE)) },
                OmapOptions.from(List.of("segmentSize=" + segmentSize,
                        "compactionMinRows=" + compactionMinRows)),
                CompareMode.getInstance(null, 0));
    }

    private static Value[] arrayRow(Value array) {
        return new Value[] { array };
    }

    private static Value intArray(int... elements) {
        Value[] list = new Value[elements.length];
        for (int i = 0; i < elements.length; i++) {
            list[i] = ValueInteger.get(elements[i]);
        }
        return ValueArray.get(INTEGER_TYPE, list, null);
    }

    private static List<Integer> intsOf(Value array) {
        List<Integer> out = new ArrayList<>();
        for (Value v : ((ValueArray) array).getList()) {
            out.add(v.getInt());
        }
        return out;
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

    /**
     * zone map 剪枝：A 列下界为 3 时，只应命中第二段。
     */
    @Test
    void zoneMapSkipsNonIntersectingSegments() {
        ColumnarStore store = newStore(2);
        store.append(row(1, "a"));
        store.append(row(2, "b"));
        long r2 = store.append(row(3, "c"));
        long r3 = store.append(row(4, "d"));

        Value[] lower = new Value[] { ValueInteger.get(3), null };
        long[] ids = store.collectVisibleRowIds(lower, null, false);
        assertEquals(Arrays.asList(r2, r3), Arrays.asList(ids[0], ids[1]));

        long[] reversed = store.collectVisibleRowIds(lower, null, true);
        assertEquals(Arrays.asList(r3, r2), Arrays.asList(reversed[0], reversed[1]));
    }

    /**
     * 合并：delta 的改动写回段，rowId 不变，zone map 重算后剪枝仍然有效。
     */
    @Test
    void compactWritesDeltaBackIntoSegments() {
        ColumnarStore store = newStore(2, 2);
        long first = store.append(row(1, "a"));
        long second = store.append(row(2, "b"));
        long third = store.append(row(3, "c"));
        assertEquals(1, store.segments().size());

        store.update(first, row(1, "x"));
        store.remove(second);
        // delta 达到 compactionMinRows，上面这次 remove 已经触发合并。
        assertEquals(2, store.getRowCount());
        assertEquals("x", column(store.materialize(first), 1));
        assertNull(store.materialize(second));
        assertEquals("3", column(store.materialize(third), 0));
        assertEquals(first, store.materialize(first).getKey());

        // 合并后 zone map 已重算：A >= 3 只命中第三行所在的段。
        Value[] lower = new Value[] { ValueInteger.get(3), null };
        long[] ids = store.collectVisibleRowIds(lower, null, false);
        assertEquals(1, ids.length);
        assertEquals(third, ids[0]);
    }

    /** 迭代器是创建时刻的快照：创建后插入的行不返回。 */
    @Test
    void iteratorIsSnapshotAtCreation() {
        ColumnarStore store = newStore(2);
        long first = store.append(row(1, "a"));
        ColumnarStore.RowIterator iterator = store.rowIterator(null, null, false);
        assertEquals(first, iterator.next());

        store.append(row(2, "b"));
        store.append(row(3, "c"));
        assertEquals(-1, iterator.next());
    }

    /** 迭代器跳过创建后才被删除的行。 */
    @Test
    void iteratorSkipsRowsDeletedAfterCreation() {
        ColumnarStore store = newStore(4);
        long first = store.append(row(1, "a"));
        long second = store.append(row(2, "b"));
        ColumnarStore.RowIterator iterator = store.rowIterator(null, null, false);

        assertEquals(first, iterator.next());
        store.remove(second);
        assertEquals(-1, iterator.next());
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

    /** 已冻结段内被 delta 删除的行不得计入 rowId 收集结果，且与批量迭代器一致。 */    @Test
    void collectVisibleRowIdsSkipsRowsDeletedFromFrozenSegments() {
        ColumnarStore store = newStore(2);
        long r0 = store.append(row(1, "a"));
        long r1 = store.append(row(2, "b"));
        long r2 = store.append(row(3, "c"));
        assertEquals(1, store.segments().size());
        store.remove(r0);

        long[] ids = store.collectVisibleRowIds(false);
        assertEquals(Arrays.asList(r1, r2), Arrays.asList(ids[0], ids[1]));

        List<Long> viaBatch = new ArrayList<>();
        ColumnarStore.RowIterator iterator = store.rowIterator(null, null, false);
        Row[] buffer = new Row[8];
        for (int n; (n = iterator.nextBatch(store, buffer)) > 0; ) {
            for (int i = 0; i < n; i++) {
                viaBatch.add(buffer[i].getKey());
            }
        }
        assertEquals(viaBatch, Arrays.asList(ids[0], ids[1]));
    }

    /**
     * 数组列拆成「前缀和下标 + 元素列」，元素个数不必重建 {@code ValueArray} 就能取到。
     */
    @Test
    void arrayColumnStoresPrefixSumsAndElements() {
        ColumnarStore store = newArrayStore(2, Integer.MAX_VALUE);
        long first = store.append(arrayRow(intArray(1, 2, 3)));
        store.append(arrayRow(intArray()));
        assertEquals(1, store.segments().size());

        ArrayVector vector = assertInstanceOf(ArrayVector.class,
                store.segments().get(0).getColumn(0));
        assertArrayEquals(new int[] { 0, 3, 3 }, vector.rawStarts());
        assertInstanceOf(IntVector.class, vector.elements());
        assertEquals(3, vector.length(0));
        assertEquals(0, vector.length(1));
        assertEquals(Arrays.asList(1, 2, 3),
                intsOf(store.materialize(first).getValueList()[0]));
    }

    /**
     * 整值 NULL 与空数组是两个不同的结果：前者长度 -1，后者长度 0。
     * {@code CARDINALITY(col) >= 1} 据此只命中非空数组。
     */
    @Test
    void arrayColumnDistinguishesNullFromEmpty() {
        ColumnarStore store = newArrayStore(3, Integer.MAX_VALUE);
        long nullRow = store.append(arrayRow(ValueNull.INSTANCE));
        long emptyRow = store.append(arrayRow(intArray()));
        long threeRow = store.append(arrayRow(intArray(1, 2, 3)));
        assertEquals(1, store.segments().size());

        ArrayVector vector = assertInstanceOf(ArrayVector.class,
                store.segments().get(0).getColumn(0));
        assertEquals(-1, vector.length(RowId.offset(nullRow)));
        assertEquals(0, vector.length(RowId.offset(emptyRow)));
        assertEquals(3, vector.length(RowId.offset(threeRow)));
        assertEquals(ValueNull.INSTANCE, store.materialize(nullRow).getValueList()[0]);
        assertEquals(0, ((ValueArray) store.materialize(emptyRow).getValueList()[0])
                .getList().length);

        List<Long> hit = new ArrayList<>();
        for (long rowId : new long[] { nullRow, emptyRow, threeRow }) {
            if (vector.length(RowId.offset(rowId)) >= 1) {
                hit.add(rowId);
            }
        }
        assertEquals(List.of(threeRow), hit);
    }

    /** 合并把长度变化、置 NULL、置空数组三种改动写回段，rowId 不变。 */
    @Test
    void compactHandlesArrayLengthChange() {
        ColumnarStore store = newArrayStore(2, 1);
        long first = store.append(arrayRow(intArray(1)));
        long second = store.append(arrayRow(intArray(2)));
        assertEquals(1, store.segments().size());

        store.update(first, arrayRow(intArray(7, 7, 7)));
        assertEquals(Arrays.asList(7, 7, 7),
                intsOf(store.materialize(first).getValueList()[0]));
        assertEquals(first, store.materialize(first).getKey());
        assertEquals(Arrays.asList(2),
                intsOf(store.materialize(second).getValueList()[0]));

        store.update(second, arrayRow(ValueNull.INSTANCE));
        assertEquals(ValueNull.INSTANCE, store.materialize(second).getValueList()[0]);

        store.update(second, arrayRow(intArray()));
        assertEquals(0, ((ValueArray) store.materialize(second).getValueList()[0])
                .getList().length);

        ArrayVector vector = assertInstanceOf(ArrayVector.class,
                store.segments().get(0).getColumn(0));
        assertEquals(3, vector.length(RowId.offset(first)));
        assertEquals(0, vector.length(RowId.offset(second)));
    }

    /** 元素值类型与列声明不一致时退回 {@link ValueVector}，值原样保存。 */
    @Test
    void arrayColumnFallsBackWhenElementTypeDiffers() {
        ColumnarStore store = newArrayStore(2, Integer.MAX_VALUE);
        Value strings = ValueArray.get(TypeInfo.getTypeInfo(Value.VARCHAR),
                new Value[] { ValueVarchar.get("x") }, null);
        long first = store.append(arrayRow(strings));
        store.append(arrayRow(intArray(1, 2)));
        assertEquals(1, store.segments().size());

        ColumnVector vector = store.segments().get(0).getColumn(0);
        assertInstanceOf(ValueVector.class, vector);
        assertEquals(strings, store.materialize(first).getValueList()[0]);
    }
    /** 单列 {@code VARCHAR(64)} 的存储，用于观察打包字节向量。 */
    private static ColumnarStore newVarcharStore(int segmentSize, int compactionMinRows) {
        return new ColumnarStore(
                new Column[] { new Column("S", TypeInfo.getTypeInfo(Value.VARCHAR, 64, 0, null)) },
                OmapOptions.from(List.of("segmentSize=" + segmentSize,
                        "compactionMinRows=" + compactionMinRows)),
                CompareMode.getInstance(null, 0));
    }

    private static Value[] varcharRow(Value value) {
        return new Value[] { value };
    }

    /**
     * 高基数 VARCHAR 列：段内按「字节池 + 偏移」存放，不再逐行持有 String。
     *
     * <p>行里混入 NULL，覆盖「NULL 行之后继续追加」时前缀和必须保持单调的情况。
     */
    @Test
    void varcharColumnPacksBytesAndOffsets() {
        ColumnarStore store = newVarcharStore(1000, Integer.MAX_VALUE);
        long[] ids = new long[1000];
        for (int i = 0; i < 1000; i++) {
            Value value = i % 7 == 0 ? ValueNull.INSTANCE
                    : ValueVarchar.get("value-" + String.format("%010d", i));
            ids[i] = store.append(varcharRow(value));
        }
        assertEquals(1, store.segments().size());
        ColumnVector vector = store.segments().get(0).getColumn(0);
        assertInstanceOf(VarcharVector.class, vector);
        long perRow = vector.getEstimatedMemory() / vector.size();
        assertTrue(perRow < 24, "打包后每行应低于 24 B，实际 " + perRow + " B");
        for (int i = 0; i < 1000; i++) {
            Value actual = store.materialize(ids[i]).getValueList()[0];
            if (i % 7 == 0) {
                assertEquals(ValueNull.INSTANCE, actual, "第 " + i + " 行应为 NULL");
            } else {
                assertEquals("value-" + String.format("%010d", i), actual.getString());
            }
        }
    }

    /**
     * 合并写回时值长度变化：前缀和整体调整，未改动的行仍然取到正确的值。
     *
     * <p>段长 200、每段 200 个互不相同的值，超过字典容量（{@code min(4096, 段长/4)}），
     * 因此该列走打包字节向量而不是字典向量。
     */
    @Test
    void compactRewritesVarcharWithDifferentLengths() {
        ColumnarStore store = newVarcharStore(200, 1);
        long[] ids = new long[200];
        for (int i = 0; i < 200; i++) {
            ids[i] = store.append(varcharRow(ValueVarchar.get("v" + i)));
        }
        assertEquals(1, store.segments().size());

        store.update(ids[0], varcharRow(ValueVarchar.get("这是一个明显更长的值")));
        store.update(ids[1], varcharRow(ValueVarchar.get("短")));
        store.update(ids[2], varcharRow(ValueNull.INSTANCE));

        assertEquals(1, store.segments().size());
        assertEquals("这是一个明显更长的值",
                store.materialize(ids[0]).getValueList()[0].getString());
        assertEquals("短", store.materialize(ids[1]).getValueList()[0].getString());
        assertEquals(ValueNull.INSTANCE, store.materialize(ids[2]).getValueList()[0]);
        assertEquals("v3", store.materialize(ids[3]).getValueList()[0].getString());
        assertEquals("v7", store.materialize(ids[7]).getValueList()[0].getString());
        assertEquals("v199", store.materialize(ids[199]).getValueList()[0].getString());
    }

    /**
     * 惰性加载：未加载的段对读路径完全不可见，已加载段正常参与。
     *
     * <p>后台加载在本用例中不启动（没有 persistence），所以未加载段会一直保持未加载。
     */
    @Test
    void unloadedSegmentsAreInvisibleToEveryReadPath() {
        ColumnarStore source = newStore(4, Integer.MAX_VALUE);
        for (int i = 0; i < 12; i++) {
            source.append(row(i, "v" + i));
        }
        List<ColumnSegment> built = source.segments();
        assertEquals(3, built.size());

        ColumnarStore target = newStore(4, Integer.MAX_VALUE);
        target.setLazyLoad(true);
        int[] segmentIds = new int[built.size()];
        int[] generations = new int[built.size()];
        ColumnSegment[] preloaded = new ColumnSegment[built.size()];
        for (int i = 0; i < built.size(); i++) {
            segmentIds[i] = built.get(i).getSegmentId();
            generations[i] = built.get(i).getGeneration();
            // 只把第 0 段当作已加载，其余两段保持未加载。
            preloaded[i] = i == 0 ? built.get(0) : null;
        }
        target.restoreFromPersistence(segmentIds, generations, preloaded, 3,
                List.of(), Set.of(), Map.of(), Set.of());

        assertEquals(3, target.segmentCount());
        assertEquals(1, target.loadedSegmentCount());
        assertEquals(4, target.getRowCount());
        assertArrayEquals(new long[] { 0, 1, 2, 3 }, target.collectVisibleRowIds(false));
        assertEquals(1, target.takeVectorizedSnapshot().segments.length);
        assertEquals("v2", target.materialize(2).getValueList()[1].getString());
        // 未加载段里的行按不可见处理，不能被物化。
        assertNull(target.materialize(5));
    }

    /**
     * MOR 批次 1：全删段整体丢弃，中间段丢弃后段号出现空洞，
     * 幸存行的 rowId 与内容不变，后续合并不受影响。
     */
    @Test
    void fullyDeletedSegmentsAreDroppedIndividually() {
        ColumnarStore store = newStore(10, Integer.MAX_VALUE);
        long[] ids = new long[40];
        for (int i = 0; i < 40; i++) {
            ids[i] = store.append(row(i, "v" + i));
        }
        assertEquals(4, store.segmentCount());

        // 删掉第 0 段与第 2 段的全部行，中间段也整段丢弃。
        for (int i = 0; i < 10; i++) {
            store.remove(ids[i]);
        }
        for (int i = 20; i < 30; i++) {
            store.remove(ids[i]);
        }

        assertEquals(2, store.segmentCount());
        assertEquals(20, store.getRowCount());
        assertEquals(20, store.collectVisibleRowIds(false).length);
        assertEquals("v15", column(store.materialize(ids[15]), 1));
        assertEquals("v39", column(store.materialize(ids[39]), 1));
        assertNull(store.materialize(ids[5]));
        assertNull(store.materialize(ids[25]));

        // 幸存段的更新照常合并，段号空洞不影响段定位。
        store.update(ids[15], row(15, "x"));
        store.compact();
        assertEquals(20, store.getRowCount());
        assertEquals("x", column(store.materialize(ids[15]), 1));
        assertEquals("v16", column(store.materialize(ids[16]), 1));
    }

    /**
     * MOR 批次 1：纯删除段参与边界剪枝（删除只收窄值域，zone map 仍有效），
     * 被删除一半的段在区间查询下不能丢幸存行。
     */
    @Test
    void deleteOnlySegmentsKeepBoundsQueriesCorrect() {
        ColumnarStore store = newStore(10, Integer.MAX_VALUE);
        long[] ids = new long[20];
        for (int i = 0; i < 20; i++) {
            ids[i] = store.append(row(i, "v" + i));
        }
        // 两段各删一半，都有幸存行，都不丢弃。
        for (int i = 5; i < 15; i++) {
            store.remove(ids[i]);
        }
        assertEquals(2, store.segmentCount());

        Value[] lower = new Value[] { ValueInteger.get(0), null };
        long[] visible = store.collectVisibleRowIds(lower, null, false);
        assertEquals(10, visible.length);
        assertEquals(10, store.getRowCount());
        assertEquals("v0", column(store.materialize(ids[0]), 1));
        assertEquals("v19", column(store.materialize(ids[19]), 1));
        long[] reversed = store.collectVisibleRowIds(lower, null, true);
        assertEquals(10, reversed.length);
    }
}

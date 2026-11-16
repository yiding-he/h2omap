/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.vector;

import java.util.List;

import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueNull;

/**
 * 数组列。每行的数组被拆成两部分：{@code starts} 记录每行元素在 {@code elements}
 * 中的起止下标（前缀和形式，长度等于行数加一），{@code elements} 是按行顺序拼接的
 * 元素列。
 *
 * <p>这样 {@code CARDINALITY} 只需一次整数减法，不必读取每行的 {@link ValueArray}
 * 对象图；元素列本身也是 primitive 数组，可以整列扫描。
 *
 * <p>{@link #get(int)} 会重建 {@link ValueArray}，只在 H2 真正取到该行该列时发生。
 * 重建时传入的元素类型取列声明中的元素类型，元素的值类型与之相同，
 * {@code ValueArray.get} 内部的元素转换因此不改变元素的值，不需要 {@code CastDataProvider}。
 *
 * <p>{@link #set(int, Value)} 在新旧长度相同时就地改写元素；长度变化时重建整个列。
 * 合并走 Copy-On-Compact，调用方已经复制过一份，重建的代价与这次复制同阶。
 */
public final class ArrayVector implements ColumnVector {

    /** 元素类型，不含数组维度。 */
    private final TypeInfo componentType;

    /** 每行元素的起止下标，`starts[行数]` 等于元素总数。 */
    private int[] starts;

    /** 按行顺序拼接的元素。 */
    private ColumnVector elements;

    /** 整值为 NULL 的行。元素自身的 NULL 由 `elements` 自己的位图记录。 */
    private final NullBitmap nulls;

    private ArrayVector(TypeInfo componentType, int[] starts, ColumnVector elements, NullBitmap nulls) {
        this.componentType = componentType;
        this.starts = starts;
        this.elements = elements;
        this.nulls = nulls;
    }

    public static ArrayVector of(TypeInfo componentType, int[] starts, ColumnVector elements,
            NullBitmap nulls) {
        return new ArrayVector(componentType, starts, elements, nulls);
    }

    /**
     * 由一批行构建数组列。列里有 NULL、空数组都正常；只要有一个非 NULL 值的元素
     * 值类型与 {@code componentType} 不一致，或元素类型带有无法序列化的扩展类型，
     * 就返回 {@code null} 让调用方退回 {@link ValueVector}。
     *
     * <p>只比较元素的值类型，不比较精度与长度：非空数组的元素类型由实际元素决定
     * （例如 {@code CHARACTER VARYING(2)}），比列声明的元素类型更窄，而空数组的
     * 元素类型是 {@code NULL}。存放的是元素本身，重建时再按列声明的元素类型还原。
     *
     * @param rows          行数据，第 0 个元素是 rowId
     * @param column        本列在行数组中的下标
     * @param componentType 元素类型
     * @return 数组列，无法安全构建时返回 {@code null}
     */
    public static ArrayVector tryBuild(List<Value[]> rows, int column, TypeInfo componentType) {
        if (componentType == null || componentType.getExtTypeInfo() != null) {
            return null;
        }
        int count = rows.size();
        int[] starts = new int[count + 1];
        long total = 0;
        for (int r = 0; r < count; r++) {
            Value v = rows.get(r)[column];
            if (v != ValueNull.INSTANCE) {
                // getValueType() 是常量，比 instanceof 先做类型判断更省一次强转失败的开销
                if (v.getValueType() != Value.ARRAY) {
                    return null;
                }
                ValueArray array = (ValueArray) v;
                if (array.getList().length != 0
                        && array.getComponentType().getValueType() != componentType.getValueType()) {
                    return null;
                }
                total += array.getList().length;
            }
            if (total > Integer.MAX_VALUE - 8) {
                return null;
            }
            starts[r + 1] = (int) total;
        }

        ColumnVector elements = ColumnVector.create(componentType.getValueType(), (int) total);
        NullBitmap nulls = new NullBitmap(count);
        for (int r = 0; r < count; r++) {
            Value v = rows.get(r)[column];
            if (v == ValueNull.INSTANCE) {
                nulls.setNull(r);
                continue;
            }
            Value[] list = ((ValueArray) v).getList();
            int base = starts[r];
            for (int j = 0; j < list.length; j++) {
                elements.set(base + j, list[j]);
            }
        }
        return new ArrayVector(componentType, starts, elements, nulls);
    }

    public TypeInfo getComponentType() {
        return componentType;
    }

    public int[] rawStarts() {
        return starts;
    }

    public ColumnVector elements() {
        return elements;
    }

    /**
     * 第 {@code index} 行数组的元素个数；整值为 NULL 时返回 {@code -1}。
     *
     * <p>与 {@code get(index).getList().length} 的区别是这里不重建对象。
     */
    public int length(int index) {
        return nulls.isNull(index) ? -1 : starts[index + 1] - starts[index];
    }

    @Override
    public int size() {
        return starts.length - 1;
    }

    @Override
    public Value get(int index) {
        if (nulls.isNull(index)) {
            return ValueNull.INSTANCE;
        }
        int from = starts[index];
        int len = starts[index + 1] - from;
        Value[] list = new Value[len];
        for (int j = 0; j < len; j++) {
            list[j] = elements.get(from + j);
        }
        return ValueArray.get(componentType, list, null);
    }

    @Override
    public void set(int index, Value value) {
        int from = starts[index];
        int oldLen = starts[index + 1] - from;
        if (value == null || value == ValueNull.INSTANCE) {
            if (oldLen == 0) {
                nulls.setNull(index);
            } else {
                rebuild(index, null);
            }
            return;
        }
        Value[] list = ((ValueArray) value).getList();
        if (list.length != oldLen) {
            rebuild(index, list);
            return;
        }
        nulls.clear(index);
        for (int j = 0; j < list.length; j++) {
            elements.set(from + j, list[j]);
        }
    }

    /** 换掉第 {@code index} 行的元素；{@code newRow} 为 null 表示该行变成 NULL。 */
    private void rebuild(int index, Value[] newRow) {
        int count = size();
        int dropped = starts[index + 1] - starts[index];
        int added = newRow == null ? 0 : newRow.length;
        int total = starts[count] - dropped + added;

        int[] newStarts = new int[count + 1];
        ColumnVector newElements = ColumnVector.create(componentType.getValueType(), total);
        int pos = 0;
        for (int r = 0; r < count; r++) {
            newStarts[r] = pos;
            if (r == index) {
                if (newRow != null) {
                    for (Value v : newRow) {
                        newElements.set(pos++, v);
                    }
                }
                continue;
            }
            for (int j = starts[r], end = starts[r + 1]; j < end; j++) {
                newElements.set(pos++, elements.get(j));
            }
        }
        newStarts[count] = pos;

        if (newRow == null) {
            nulls.setNull(index);
        } else {
            nulls.clear(index);
        }
        this.starts = newStarts;
        this.elements = newElements;
    }

    @Override
    public NullBitmap nulls() {
        return nulls;
    }

    @Override
    public long getEstimatedMemory() {
        return 16L
                + (long) starts.length * Integer.BYTES
                + elements.getEstimatedMemory()
                + nulls.getEstimatedMemory();
    }

    @Override
    public ColumnVector copy() {
        return new ArrayVector(componentType, starts.clone(), elements.copy(), nulls.copy());
    }
}

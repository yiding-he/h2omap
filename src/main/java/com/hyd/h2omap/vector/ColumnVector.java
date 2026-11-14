/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.vector;

import org.h2.value.Value;

/**
 * 一列的值。实现类用 primitive 数组加 null 位图，不用 {@code Value[]}。
 *
 * <p>只有被引用的列、被引用的行才会物化成 {@link Value}。
 * 这是列式存储相对于行式的主要收益处。
 */
public interface ColumnVector {

    /** 元素个数，含 null。 */
    int size();

    /** 取一个值，null 返回 {@code ValueNull.INSTANCE}。 */
    Value get(int index);

    /** 写一个值。 */
    void set(int index, Value value);

    /** null 位图。 */
    NullBitmap nulls();

    /** 估算内存占用，给统计和合并阈值用。 */
    long getEstimatedMemory();

    /**
     * 按 H2 的类型码创建向量。
     *
     * <p>最常用的类型有专用实现，其余类型用 {@link ValueVector} 原样存放。
     */
    static ColumnVector create(int valueType, int capacity) {
        switch (valueType) {
        case Value.TINYINT:
        case Value.SMALLINT:
        case Value.INTEGER:
            return new IntVector(capacity);
        case Value.BIGINT:
            return new LongVector(capacity);
        case Value.REAL:
        case Value.DOUBLE:
            return new DoubleVector(capacity);
        case Value.CHAR:
        case Value.VARCHAR:
            return new VarcharVector(capacity);
        default:
            return new ValueVector(capacity);
        }
    }
}

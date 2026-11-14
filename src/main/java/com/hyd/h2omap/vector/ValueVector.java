/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.vector;

import java.util.ArrayList;
import java.util.List;

import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * 按 {@code Value} 原样存放的列向量，用于 {@link ColumnVector#create}
 * 尚未特化的类型。
 *
 * <p>专用向量（int、long、double、varchar）比这个类省内存、访问更快，
 * 所以列的类型一旦进入特化列表，这个类就不再被使用。
 */
public final class ValueVector implements ColumnVector {

    private final List<Value> values;

    private final NullBitmap nulls;

    public ValueVector(int capacity) {
        this.values = new ArrayList<>(capacity);
        this.nulls = new NullBitmap(capacity);
    }

    @Override
    public int size() {
        return values.size();
    }

    @Override
    public Value get(int index) {
        return index >= 0 && index < values.size() ? values.get(index) : ValueNull.INSTANCE;
    }

    /**
     * 写入第 index 个值。建段时顺序追加，合并时按段内偏移改写。
     */
    @Override
    public void set(int index, Value value) {
        if (index == values.size()) {
            values.add(value);
        } else {
            values.set(index, value);
        }
        if (value == ValueNull.INSTANCE) {
            nulls.setNull(index);
        } else {
            nulls.clear(index);
        }
    }

    @Override
    public NullBitmap nulls() {
        return nulls;
    }

    @Override
    public long getEstimatedMemory() {
        long memory = 16L + (long) values.size() * Integer.BYTES;
        for (Value v : values) {
            // 值本身的大小随内容变化：数组列的元素数、字符串列的长度都在这里体现。
            memory += v.getMemory();
        }
        return memory + nulls.getEstimatedMemory();
    }
}

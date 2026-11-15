/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.vector;

import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.ValueVarchar;

/**
 * CHAR / VARCHAR 列。
 *
 * <p>骨架直接存 {@code String[]}。真正的实现应该加分字典，
 * 低基数列只存字典编码，这一步对 OLAP 的收益最大。
 */
public final class VarcharVector implements ColumnVector {

    private final String[] values;

    private final NullBitmap nulls;

    public VarcharVector(int capacity) {
        this.values = new String[capacity];
        this.nulls = new NullBitmap(capacity);
    }

    private VarcharVector(String[] values, NullBitmap nulls) {
        this.values = values;
        this.nulls = nulls;
    }

    @Override
    public ColumnVector copy() {
        return new VarcharVector(values.clone(), nulls.copy());
    }

    @Override
    public int size() {
        return values.length;
    }

    @Override
    public Value get(int index) {
        return nulls.isNull(index) ? ValueNull.INSTANCE : ValueVarchar.get(values[index]);
    }

    @Override
    public void set(int index, Value value) {
        if (value.getValueType() == Value.NULL) {
            nulls.setNull(index);
            return;
        }
        values[index] = value.getString();
        nulls.clear(index);
    }

    @Override
    public NullBitmap nulls() {
        return nulls;
    }

    public String[] rawValues() {
        return values;
    }

    @Override
    public long getEstimatedMemory() {
        return 16L + (long) values.length * Long.BYTES + nulls.getEstimatedMemory();
    }
}

/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.vector;

import org.h2.value.Value;
import org.h2.value.ValueDouble;
import org.h2.value.ValueNull;

/**
 * REAL / DOUBLE 列。
 */
public final class DoubleVector implements ColumnVector {

    private final double[] values;

    private final NullBitmap nulls;

    public DoubleVector(int capacity) {
        this.values = new double[capacity];
        this.nulls = new NullBitmap(capacity);
    }

    @Override
    public int size() {
        return values.length;
    }

    @Override
    public Value get(int index) {
        return nulls.isNull(index) ? ValueNull.INSTANCE : ValueDouble.get(values[index]);
    }

    @Override
    public void set(int index, Value value) {
        if (value.getValueType() == Value.NULL) {
            nulls.setNull(index);
            return;
        }
        values[index] = value.getDouble();
        nulls.clear(index);
    }

    @Override
    public NullBitmap nulls() {
        return nulls;
    }

    @Override
    public long getEstimatedMemory() {
        return 16L + (long) values.length * Double.BYTES + nulls.getEstimatedMemory();
    }
}

/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.vector;

import org.h2.value.Value;
import org.h2.value.ValueInteger;
import org.h2.value.ValueNull;

/**
 * TINYINT / SMALLINT / INTEGER 列。
 */
public final class IntVector implements ColumnVector {

    private final int[] values;

    private final NullBitmap nulls;

    public IntVector(int capacity) {
        this.values = new int[capacity];
        this.nulls = new NullBitmap(capacity);
    }

    private IntVector(int[] values, NullBitmap nulls) {
        this.values = values;
        this.nulls = nulls;
    }

    public static IntVector of(int[] values, NullBitmap nulls) {
        return new IntVector(values, nulls);
    }

    @Override
    public ColumnVector copy() {
        return new IntVector(values.clone(), nulls.copy());
    }

    @Override
    public int size() {
        return values.length;
    }

    @Override
    public Value get(int index) {
        return nulls.isNull(index) ? ValueNull.INSTANCE : ValueInteger.get(values[index]);
    }

    @Override
    public void set(int index, Value value) {
        if (value.getValueType() == Value.NULL) {
            nulls.setNull(index);
            return;
        }
        values[index] = value.getInt();
        nulls.clear(index);
    }

    @Override
    public NullBitmap nulls() {
        return nulls;
    }

    public int[] rawValues() {
        return values;
    }

    @Override
    public long getEstimatedMemory() {
        return 16L + (long) values.length * Integer.BYTES + nulls.getEstimatedMemory();
    }
}

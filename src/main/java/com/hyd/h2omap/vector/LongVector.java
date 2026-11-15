/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.vector;

import org.h2.value.Value;
import org.h2.value.ValueBigint;
import org.h2.value.ValueNull;

/**
 * BIGINT 列。
 */
public final class LongVector implements ColumnVector {

    private final long[] values;

    private final NullBitmap nulls;

    public LongVector(int capacity) {
        this.values = new long[capacity];
        this.nulls = new NullBitmap(capacity);
    }

    private LongVector(long[] values, NullBitmap nulls) {
        this.values = values;
        this.nulls = nulls;
    }

    @Override
    public ColumnVector copy() {
        return new LongVector(values.clone(), nulls.copy());
    }

    @Override
    public int size() {
        return values.length;
    }

    @Override
    public Value get(int index) {
        return nulls.isNull(index) ? ValueNull.INSTANCE : ValueBigint.get(values[index]);
    }

    @Override
    public void set(int index, Value value) {
        if (value.getValueType() == Value.NULL) {
            nulls.setNull(index);
            return;
        }
        values[index] = value.getLong();
        nulls.clear(index);
    }

    @Override
    public NullBitmap nulls() {
        return nulls;
    }

    @Override
    public long getEstimatedMemory() {
        return 16L + (long) values.length * Long.BYTES + nulls.getEstimatedMemory();
    }
}

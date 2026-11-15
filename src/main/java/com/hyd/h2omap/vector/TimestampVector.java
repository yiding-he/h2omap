/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.vector;

import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.ValueTimestamp;

/**
 * TIMESTAMP 列：按 dateValue 与 timeNanos 两个原始 long 数组连续存放，避免逐行对象分配。
 */
public final class TimestampVector implements ColumnVector {

    private final long[] dateValues;

    private final long[] timeNanos;

    private final NullBitmap nulls;

    public TimestampVector(int capacity) {
        this.dateValues = new long[capacity];
        this.timeNanos = new long[capacity];
        this.nulls = new NullBitmap(capacity);
    }

    private TimestampVector(long[] dateValues, long[] timeNanos, NullBitmap nulls) {
        this.dateValues = dateValues;
        this.timeNanos = timeNanos;
        this.nulls = nulls;
    }

    public static TimestampVector of(long[] dateValues, long[] timeNanos, NullBitmap nulls) {
        return new TimestampVector(dateValues, timeNanos, nulls);
    }

    @Override
    public ColumnVector copy() {
        return new TimestampVector(dateValues.clone(), timeNanos.clone(), nulls.copy());
    }

    @Override
    public int size() {
        return dateValues.length;
    }

    @Override
    public Value get(int index) {
        return nulls.isNull(index)
                ? ValueNull.INSTANCE
                : ValueTimestamp.fromDateValueAndNanos(dateValues[index], timeNanos[index]);
    }

    @Override
    public void set(int index, Value value) {
        if (value.getValueType() == Value.NULL) {
            nulls.setNull(index);
            return;
        }
        ValueTimestamp ts = value instanceof ValueTimestamp
                ? (ValueTimestamp) value
                : (ValueTimestamp) value.convertTo(TypeInfo.TYPE_TIMESTAMP, null);
        dateValues[index] = ts.getDateValue();
        timeNanos[index] = ts.getTimeNanos();
        nulls.clear(index);
    }

    @Override
    public NullBitmap nulls() {
        return nulls;
    }

    public long[] rawDateValues() {
        return dateValues;
    }

    public long[] rawTimeNanos() {
        return timeNanos;
    }

    @Override
    public long getEstimatedMemory() {
        return 24L + (long) dateValues.length * Long.BYTES * 2L + nulls.getEstimatedMemory();
    }
}

/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.vector;

import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.ValueTimestamp;
import org.h2.value.TypeInfo;

/**
 * TIMESTAMP 列：按 dateValue 与 timeNanos 两个原始 long 数组连续存放，避免逐行对象分配。
 *
 * <p>冻结后 dateValue 可位打包（{@link IntVector#tryEncode} 同口径：按最大值位宽、
 * 省不到 {@link IntVector#ENCODE_RATIO} 以上不编码）；timeNanos 值域约 37 位、超过
 * 位打包上限，始终是原始 {@code long[]}。查询路径用 {@link #dateAt} 与
 * {@link #nanosAt} 单点读。
 */
public final class TimestampVector implements ColumnVector {

    private long[] dateValues;
    private BitPacked packedDates;
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

    /** 对 dateValue 位打包；不划算时返回 {@code null}。 */
    public static TimestampVector tryEncode(TimestampVector v) {
        if (v.dateValues == null) {
            return null;
        }
        // 与 IntVector.tryEncode 同口径（按最大值位宽、同一阈值），dateValue 是 long 故自行打包。
        long[] a = v.dateValues;
        int n = a.length;
        if (n == 0) {
            return null;
        }
        long max = 0;
        for (int i = 0; i < n; i++) {
            if (a[i] > max) {
                max = a[i];
            }
        }
        int w = BitPacked.bitsFor(max);
        if (w <= 0 || w > 32) {
            return null;
        }
        TimestampVector out = new TimestampVector(null, v.timeNanos, v.nulls);
        BitPacked p = BitPacked.pack(a, w);
        if (p.getEstimatedMemory() > (long) n * Long.BYTES * IntVector.ENCODE_RATIO) {
            return null;
        }
        out.packedDates = p;
        return out;
    }

    @Override
    public ColumnVector copy() {
        return new TimestampVector(dateValues != null ? dateValues.clone() : rawDateValues(),
                timeNanos.clone(), nulls.copy());
    }

    @Override
    public int size() {
        return timeNanos.length;
    }

    @Override
    public Value get(int index) {
        return nulls.isNull(index)
                ? ValueNull.INSTANCE
                : ValueTimestamp.fromDateValueAndNanos(dateAt(index), timeNanos[index]);
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
        if (dateValues == null) {
            dateValues = rawDateValues();
            packedDates = null;
        }
        dateValues[index] = ts.getDateValue();
        timeNanos[index] = ts.getTimeNanos();
        nulls.clear(index);
    }

    @Override
    public NullBitmap nulls() {
        return nulls;
    }

    /** dateValue 的单点读。 */
    public long dateAt(int index) {
        if (dateValues != null) {
            return dateValues[index];
        }
        return packedDates.get(index);
    }

    /** timeNanos 的单点读；始终原始数组。 */
    public long nanosAt(int index) {
        return timeNanos[index];
    }

    /** 整列 dateValue（不缓存），供序列化与复制。 */
    public long[] rawDateValues() {
        if (dateValues != null) {
            return dateValues;
        }
        int n = packedDates.size();
        long[] out = new long[n];
        packedDates.readRange(0, n, out);
        return out;
    }

    public long[] rawTimeNanos() {
        return timeNanos;
    }

    public boolean isDateEncoded() {
        return dateValues == null;
    }

    @Override
    public long getEstimatedMemory() {
        long dates = dateValues != null
                ? 16L + (long) dateValues.length * Long.BYTES
                : packedDates.getEstimatedMemory();
        return 24L + dates + (long) timeNanos.length * Long.BYTES + nulls.getEstimatedMemory();
    }
}

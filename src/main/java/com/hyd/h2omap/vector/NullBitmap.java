/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.vector;

/**
 * null 位图。每 64 行用一个 long。
 */
public final class NullBitmap {

    private final long[] bits;

    private boolean hasNull;

    public NullBitmap(int capacity) {
        this.bits = new long[(capacity + 63) >>> 6];
    }

    private NullBitmap(long[] bits, boolean hasNull) {
        this.bits = bits;
        this.hasNull = hasNull;
    }

    public NullBitmap copy() {
        return new NullBitmap(bits.clone(), hasNull);
    }

    public boolean isNull(int index) {
        return hasNull && (bits[index >>> 6] & (1L << index)) != 0;
    }

    public void setNull(int index) {
        hasNull = true;
        bits[index >>> 6] |= 1L << index;
    }

    public void clear(int index) {
        bits[index >>> 6] &= ~(1L << index);
    }

    public long getEstimatedMemory() {
        return 16L + (long) bits.length * Long.BYTES;
    }
}

/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.vector;

import java.util.function.IntToLongFunction;

/**
 * 位打包数组：把每值 {@code width} 位的整数顺序装进 {@code long} 字包。
 * 定宽位宽、无符号读取。{@link #get} 是单点解包（一次移位加一次掩码，常数时间），
 * {@link #readRange} 顺序解包一段，{@link #unpack} 整体解包。
 *
 * <p>只做位打包，不做差分：差分态的随机单点读要 O(index) 累加，而向量化路径按
 * 选择向量随机访问占多数，因此数值列不用差分编码。
 * {@code width} 在 1..32 之间，没有常量列特例：全同值列按其值的位宽正常打包。
 */
public final class BitPacked {

    private final long[] words;
    private final int width;
    private final int size;

    private BitPacked(long[] words, int width, int size) {
        this.words = words;
        this.width = width;
        this.size = size;
    }

    /** 容纳 {@code max}（含）所需的最少位数，{@code max <= 0} 返回 0。 */
    public static int bitsFor(long max) {
        return max <= 0 ? 0 : 64 - Long.numberOfLeadingZeros(max);
    }

    public static BitPacked pack(int[] values, int width) {
        long[] tmp = new long[values.length];
        for (int i = 0; i < values.length; i++) {
            tmp[i] = values[i];
        }
        return pack(tmp, width);
    }

    public static BitPacked pack(long[] values, int width) {
        return pack(values.length, width, i -> values[i]);
    }

    public static BitPacked pack(int size, int width, IntToLongFunction at) {
        if (width < 1 || width > 32) {
            throw new IllegalArgumentException("width=" + width);
        }
        long mask = (1L << width) - 1;
        long[] words = new long[(size * width + 63) >>> 6];
        for (int i = 0; i < size; i++) {
            long v = at.applyAsLong(i) & mask;
            long bit = (long) i * width;
            int word = (int) (bit >>> 6);
            int off = (int) (bit & 63);
            words[word] |= v << off;
            if (off + width > 64) {
                words[word + 1] |= v >>> (64 - off);
            }
        }
        return new BitPacked(words, width, size);
    }

    public int size() {
        return size;
    }

    public int width() {
        return width;
    }

    /** 单点解包。 */
    public long get(int index) {
        long bit = (long) index * width;
        int word = (int) (bit >>> 6);
        int off = (int) (bit & 63);
        long v = words[word] >>> off;
        if (off + width > 64) {
            v |= words[word + 1] << (64 - off);
        }
        return v & ((1L << width) - 1);
    }

    /** 顺序解包 {@code [from, to)} 到 {@code dest[from..to)}。 */
    public void readRange(int from, int to, long[] dest) {
        for (int i = from; i < to; i++) {
            dest[i] = get(i);
        }
    }

    /** 整体解包为 {@code long[]}。 */
    public long[] unpack() {
        long[] out = new long[size];
        readRange(0, size, out);
        return out;
    }

    public long getEstimatedMemory() {
        return 24L + (long) words.length * Long.BYTES;
    }
}

/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.vector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Random;

import org.junit.jupiter.api.Test;

/**
 * 位打包工具类的边界：位宽换算、跨 64 位字边界读写、零宽常量、按最大位宽的往返。
 */
class BitPackedTest {

    @Test
    void bitsForCoversBoundaries() {
        assertEquals(0, BitPacked.bitsFor(0));
        assertEquals(1, BitPacked.bitsFor(1));
        assertEquals(0, BitPacked.bitsFor(-5));
        assertEquals(2, BitPacked.bitsFor(2));
        assertEquals(2, BitPacked.bitsFor(3));
        assertEquals(3, BitPacked.bitsFor(4));
        assertEquals(10, BitPacked.bitsFor(1023));
        assertEquals(10, BitPacked.bitsFor(700));
        assertEquals(32, BitPacked.bitsFor(1L << 31));
        assertEquals(33, BitPacked.bitsFor(1L << 32));
    }

    @Test
    void packRejectsWidthOver32() {
        assertThrows(IllegalArgumentException.class, () -> BitPacked.pack(4, 33, i -> 0));
        assertThrows(IllegalArgumentException.class, () -> BitPacked.pack(4, -1, i -> 0));
    }

    @Test
    void roundTripsAcrossWordBoundaries() {
        // 7 位：第 9 个值起跨字，覆盖 off + width > 64 的分支。
        int[] values = new int[200];
        Random random = new Random(42);
        for (int i = 0; i < values.length; i++) {
            values[i] = random.nextInt(1 << 7);
        }
        BitPacked packed = BitPacked.pack(values, 7);
        assertEquals(200, packed.size());
        assertEquals(7, packed.width());
        for (int i = 0; i < values.length; i++) {
            assertEquals(values[i], packed.get(i), "i=" + i);
        }
        assertEquals(24L + 22 * Long.BYTES, packed.getEstimatedMemory());
    }

    @Test
    void roundTripsWithLongSupplier() {
        long[] values = new long[130];
        for (int i = 0; i < values.length; i++) {
            values[i] = (long) i * 3 % 1000;
        }
        BitPacked packed = BitPacked.pack(values.length, 10, i -> values[i]);
        for (int i = 0; i < values.length; i++) {
            assertEquals(values[i], packed.get(i));
        }
    }

    @Test
    void zeroWidthIsRejected() {
        // 没有常量列特例：单值字典用 1 位区分 null 与该值，数值列 w <= 0 在调用方退回原数组。
        assertThrows(IllegalArgumentException.class, () -> BitPacked.pack(new long[10], 0));
        assertThrows(IllegalArgumentException.class, () -> BitPacked.pack(10, 0, i -> 0));
    }
}

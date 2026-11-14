/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.vector;

import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueInteger;
import org.h2.value.ValueNull;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link ValueVector} 的内存估算：必须随值的内容变化，
 * 否则合并阈值与内存统计会被一个 2000 元素数组记成 32 字节而失真。
 */
class ValueVectorTest {

    private static ValueArray array(int elements) {
        Value[] values = new Value[elements];
        for (int i = 0; i < elements; i++) {
            values[i] = ValueInteger.get(i);
        }
        return ValueArray.get(TypeInfo.getTypeInfo(Value.INTEGER), values, null);
    }

    private static ValueVector vectorOf(int rows, int elements) {
        ValueVector vector = new ValueVector(rows);
        for (int i = 0; i < rows; i++) {
            vector.set(i, array(elements));
        }
        return vector;
    }

    @Test
    void memoryFollowsElementCount() {
        ValueVector small = vectorOf(3, 3);
        ValueVector large = vectorOf(3, 2000);
        assertTrue(large.getEstimatedMemory() > small.getEstimatedMemory() * 100,
                () -> "small=" + small.getEstimatedMemory() + " large=" + large.getEstimatedMemory());
    }

    @Test
    void memoryGrowsWithRows() {
        assertTrue(vectorOf(8, 3).getEstimatedMemory() > vectorOf(2, 3).getEstimatedMemory());
    }

    @Test
    void nullValuesStillReportContainerOverhead() {
        ValueVector vector = new ValueVector(4);
        for (int i = 0; i < 4; i++) {
            vector.set(i, ValueNull.INSTANCE);
        }
        assertTrue(vector.getEstimatedMemory() > 0);
    }
}

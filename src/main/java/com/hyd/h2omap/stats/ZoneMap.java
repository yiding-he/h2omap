/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.stats;

import org.h2.value.CompareMode;
import org.h2.value.Value;
import org.h2.value.ValueNull;

import com.hyd.h2omap.vector.ColumnVector;

/**
 * 单列的 min/max。扫描时用它快速跳过不可能包含目标行的段。
 *
 * <p>min 和 max 都为 null 表示这个 zone map 恒与任何区间相交：
 * 全 null 的列，以及值不可比较的类型（JSON、几何等）都是这种情况，
 * 这些列不参与剪枝。
 */
public final class ZoneMap {

    private final Value min;

    private final Value max;

    private final CompareMode compareMode;

    private ZoneMap(Value min, Value max, CompareMode compareMode) {
        this.min = min;
        this.max = max;
        this.compareMode = compareMode;
    }

    /**
     * 扫描一个列向量，算出 min 和 max。
     *
     * @param vector      列向量
     * @param compareMode 数据库的比较模式，必须与扫描时使用的一致
     * @return zone map；值不可比较时返回恒相交的实例
     */
    public static ZoneMap of(ColumnVector vector, CompareMode compareMode) {
        Value min = null;
        Value max = null;
        for (int i = 0, n = vector.size(); i < n; i++) {
            Value v = vector.get(i);
            if (v == ValueNull.INSTANCE) {
                continue;
            }
            try {
                if (min == null || v.compareTo(min, null, compareMode) < 0) {
                    min = v;
                }
                if (max == null || v.compareTo(max, null, compareMode) > 0) {
                    max = v;
                }
            } catch (RuntimeException e) {
                // 剪枝只是优化，不能因为它让整张表无法产生段。
                return new ZoneMap(null, null, compareMode);
            }
        }
        return new ZoneMap(min, max, compareMode);
    }

    public Value getMin() {
        return min;
    }

    public Value getMax() {
        return max;
    }

    public boolean mayIntersect(Value low, Value high) {
        if (low != null && max != null && max.compareTo(low, null, compareMode) < 0) {
            return false;
        }
        if (high != null && min != null && min.compareTo(high, null, compareMode) > 0) {
            return false;
        }
        return true;
    }

    public long getEstimatedMemory() {
        return 32L;
    }
}

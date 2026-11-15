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

    /**
     * 尝试 O(1) 增量更新 zone map。
     *
     * @return 更新后的 zone map（未改变边界时返回 {@code this}）；返回 {@code null} 表示需要全列重算
     */
    public ZoneMap tryUpdate(Value oldVal, Value newVal) {
        if (min == null || max == null) {
            // 原先全 null 或不可比较：若新值非 null 则需要重算（不可比较类型会在 of() 中继续返回恒相交）
            return newVal == ValueNull.INSTANCE ? this : null;
        }
        try {
            if (oldVal != ValueNull.INSTANCE
                    && (oldVal.compareTo(min, null, compareMode) == 0
                            || oldVal.compareTo(max, null, compareMode) == 0)) {
                // 旧值位于区间边界，修改后边界可能收窄，必须全列重算
                return null;
            }
            if (newVal == ValueNull.INSTANCE) {
                return this;
            }
            Value nextMin = min;
            Value nextMax = max;
            if (newVal.compareTo(min, null, compareMode) < 0) {
                nextMin = newVal;
            }
            if (newVal.compareTo(max, null, compareMode) > 0) {
                nextMax = newVal;
            }
            return nextMin == min && nextMax == max
                    ? this
                    : new ZoneMap(nextMin, nextMax, compareMode);
        } catch (RuntimeException e) {
            return new ZoneMap(null, null, compareMode);
        }
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

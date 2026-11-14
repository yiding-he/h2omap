/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.stats;

import org.h2.value.CompareMode;
import org.h2.value.Value;
import org.h2.value.ValueNull;

import com.hyd.h2omap.vector.ColumnVector;

/**
 * 每个列段每列的最小值和最大值。
 *
 * <p>谓词下推的第一道筛子：查询给的下界上界和这里不相交，
 * 整段直接跳过，不用读列向量。
 */
public final class ZoneMap {

    private final Value min;

    private final Value max;

    public ZoneMap(Value min, Value max) {
        this.min = min;
        this.max = max;
    }

    /**
     * 扫描一个列向量，算出 min 和 max。全为 null 时两者都是 null。
     */
    public static ZoneMap of(ColumnVector vector) {
        CompareMode compareMode = CompareMode.getInstance(null, 0);
        Value min = null;
        Value max = null;
        for (int i = 0, n = vector.size(); i < n; i++) {
            Value v = vector.get(i);
            if (v == ValueNull.INSTANCE) {
                continue;
            }
            if (min == null || v.compareTo(min, null, compareMode) < 0) {
                min = v;
            }
            if (max == null || v.compareTo(max, null, compareMode) > 0) {
                max = v;
            }
        }
        return new ZoneMap(min, max);
    }

    public Value getMin() {
        return min;
    }

    public Value getMax() {
        return max;
    }

    /**
     * 判断查询区间是否可能与本段相交。null 表示该侧无界。
     *
     * @param low         查询下界，null 表示负无穷
     * @param high        查询上界，null 表示正无穷
     * @param compareMode 比较规则，来自数据库配置
     * @return false 表示整段可以跳过
     */
    public boolean mayIntersect(Value low, Value high, CompareMode compareMode) {
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

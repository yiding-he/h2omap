/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.storage;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 冻结段列切片缓存：重复的向量化查询读同一段、同一列集时直接复用内存，不再读盘。
 *
 * <p>缓存对象是「从段文件读出的列切片」，不是查询结果。冻结段内容在合并前不可变，
 * 因此失效坐标只有 generation：合并（{@code applyDelta}）生成新代段的实例，旧代条目随即
 * 不再被命中。delta 的未回写删除与更新新像不在切片内，查询时的可见性判定另走 delta，
 * 所以 delta 变化不影响本缓存的正确性。
 *
 * <p>与段级驻留（{@code residentBytes}）相互独立：本缓存不计入驻留预算、不参与逐出、
 * 不触发逐出，驻留通道也不复用本缓存。两者叠加时驻留段走内存，逐出段走本缓存。
 *
 * <p>预算由服务端系统属性 {@code -Dh2.omapSliceCacheBytes} 给出，默认 0 即关闭；纯数字按
 * 字节，可跟后缀 k/m/g（不区分大小写，按 1024 换算），与 residentBytes 同一写法。取值无法
 * 解析时按 0 处理，即关闭缓存。超出预算时按 LRU 淘汰最久未访问的条目。
 *
 * <p>并发：全部方法同步在同一个锁上；取出的切片其向量与删除位图只读，可跨线程共享。
 */
final class SliceCache {

    /** 列 id 不小于 64 时无法编进位掩码，调用方以此值表示「本请求不参与缓存」。 */
    static final long NO_CACHE = Long.MIN_VALUE;

    private record Key(int segmentId, int generation, long columnMask) {
    }

    private final long budget;
    private final LinkedHashMap<Key, ColumnSlice> lru =
            new LinkedHashMap<>(16, 0.75f, true);
    private long usedBytes;

    SliceCache() {
        this.budget = budgetBytes();
    }

    private static long budgetBytes() {
        String value = System.getProperty("h2.omapSliceCacheBytes");
        if (value == null) {
            return 0L;
        }
        String s = value.trim();
        int split = 0;
        while (split < s.length() && Character.isDigit(s.charAt(split))) {
            split++;
        }
        String number = s.substring(0, split);
        if (number.isEmpty()) {
            return 0L;
        }
        long multiplier;
        switch (s.substring(split).toLowerCase(java.util.Locale.ROOT)) {
        case "":
            multiplier = 1L;
            break;
        case "k":
            multiplier = 1024L;
            break;
        case "m":
            multiplier = 1024L * 1024;
            break;
        case "g":
            multiplier = 1024L * 1024 * 1024;
            break;
        default:
            return 0L;
        }
        try {
            long n = Long.parseLong(number);
            return n < 0 || n > Long.MAX_VALUE / multiplier ? 0L : n * multiplier;
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    boolean isEnabled() {
        return budget > 0;
    }

    long getUsedBytes() {
        synchronized (lru) {
            return usedBytes;
        }
    }

    int size() {
        synchronized (lru) {
            return lru.size();
        }
    }

    /**
     * 取缓存切片；命中返回切片，未命中返回 {@code null}。
     *
     * @param columnMask 请求列集的位掩码，列 id ≥ 64 时传 {@link #NO_CACHE}
     */
    ColumnSlice get(int segmentId, int generation, long columnMask) {
        if (columnMask == NO_CACHE) {
            return null;
        }
        synchronized (lru) {
            return lru.get(new Key(segmentId, generation, columnMask));
        }
    }

    /**
     * 放入缓存。单个切片就超出预算时不会放入（避免每次插入即淘汰自身）。
     */
    void put(int segmentId, int generation, long columnMask, ColumnSlice slice) {
        if (columnMask == NO_CACHE) {
            return;
        }
        long bytes = slice.getEstimatedMemory();
        synchronized (lru) {
            ColumnSlice prev = lru.put(new Key(segmentId, generation, columnMask), slice);
            if (prev != null) {
                usedBytes -= prev.getEstimatedMemory();
            }
            usedBytes += bytes;
            while (usedBytes > budget && !lru.isEmpty()) {
                Map.Entry<Key, ColumnSlice> eldest = lru.entrySet().iterator().next();
                lru.remove(eldest.getKey());
                usedBytes -= eldest.getValue().getEstimatedMemory();
            }
        }
    }

    /** 段被合并、全删摘除或清空后失效该段号的全部条目，回收预算。 */
    void invalidateSegment(int segmentId) {
        synchronized (lru) {
            lru.entrySet().removeIf(e -> {
                if (e.getKey().segmentId() != segmentId) {
                    return false;
                }
                usedBytes -= e.getValue().getEstimatedMemory();
                return true;
            });
        }
    }

    void clear() {
        synchronized (lru) {
            lru.clear();
            usedBytes = 0;
        }
    }

    /** 列集编不成位掩码时返回 {@link #NO_CACHE}。 */
    static long maskOf(int[] columnIds) {
        long mask = 0L;
        for (int c : columnIds) {
            if (c < 0 || c > 62) {
                return NO_CACHE;
            }
            mask |= 1L << c;
        }
        return mask;
    }
}

/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.vectorized;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 段级部分聚合缓存（MOR 批次 3）。
 *
 * <p>「查询签名 × 段」的部分聚合结果。签名相同、段内容与 delta 屏蔽位都
 * 没变的段，聚合不再重新扫段，直接把缓存的部分状态并进全局结果；
 * delta 与开放段的行每次查询照常求值。因此跨段写只使被改动的段失效，
 * 不会使整表的重型聚合（如 24 小时全表按列分组计数）每次都全量重扫。
 *
 * <p>部分状态由执行器生成，这里按不透明对象存放：无分组时是
 * {@code AggState[]}，分组时是 {@code Map<Value, AggState[]>}。
 *
 * <p>失效坐标有两个：段的 generation（合并会重建段并递增它）与该段的
 * delta 版本（未回写的删除或更新新像变化时递增）。两者都相同才允许复用，
 * 保证缓存的部分状态与本次快照的可见行集一致。
 *
 * <p>并发：查询线程在读锁外填充，写线程在写锁内失效。填充按
 * 「快照时的版本」打标：被并发写覆盖的条目版本不匹配，下次自然重算。
 *
 * <p>内存：签名数量上限 {@link #MAX_SIGNATURES}（最近最少使用淘汰）；
 * 分组部分结果的基数上限 {@link #MAX_GROUP_KEYS}（高基数分组的段级
 * 部分结果本身就太大，不缓存）。
 */
public final class SegmentAggCache {

    /** 单个段的部分聚合结果与它的失效坐标。 */
    public static final class Entry {

        public final int generation;

        public final long deltaVersion;

        public final Object partial;

        Entry(int generation, long deltaVersion, Object partial) {
            this.generation = generation;
            this.deltaVersion = deltaVersion;
            this.partial = partial;
        }
    }

    /** 分组部分结果的基数上限：超过则不缓存该段。 */
    public static final int MAX_GROUP_KEYS = 4096;

    private static final int MAX_SIGNATURES = 64;

    private final LinkedHashMap<String, Map<Integer, Entry>> bySignature =
            new LinkedHashMap<>(16, 0.75f, true);

    /** 累计命中并复用条目的次数，供用例与观测断言缓存真正生效。 */
    private long hits;

    /** 查缓存并校验失效坐标；坐标不符或无条目返回 {@code null}，命中递增计数。 */
    public synchronized Entry lookup(String signature, int segmentId, int generation, long deltaVersion) {
        Map<Integer, Entry> perSegment = bySignature.get(signature);
        if (perSegment == null) {
            return null;
        }
        Entry entry = perSegment.get(segmentId);
        if (entry == null || entry.generation != generation || entry.deltaVersion != deltaVersion) {
            return null;
        }
        hits++;
        return entry;
    }

    public synchronized long hitCount() {
        return hits;
    }

    /** 存部分结果；同签名同段再存会覆盖（并发填充时后写者胜，版本不匹配者下次重算）。 */
    public synchronized void store(String signature, int segmentId, int generation,
            long deltaVersion, Object partial) {
        Map<Integer, Entry> perSegment = bySignature.get(signature);
        if (perSegment == null) {
            perSegment = new HashMap<>();
            bySignature.put(signature, perSegment);
        }
        perSegment.put(segmentId, new Entry(generation, deltaVersion, partial));
        // 访问序：测过的签名排后面，队首是最久未用的；新存入的签名刚被访问，不会被淘汰。
        while (bySignature.size() > MAX_SIGNATURES) {
            java.util.Iterator<java.util.Map.Entry<String, Map<Integer, Entry>>> it =
                    bySignature.entrySet().iterator();
            it.next();
            it.remove();
        }
    }

    /** 段被整体丢弃时清掉它的条目。 */
    public synchronized void invalidateSegment(int segmentId) {
        for (Map<Integer, Entry> perSegment : bySignature.values()) {
            perSegment.remove(segmentId);
        }
    }

    /** truncate / 重放时清空全部条目。 */
    public synchronized void clear() {
        bySignature.clear();
    }
}

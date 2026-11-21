/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.storage;

import java.util.BitSet;
import java.util.List;
import java.util.Map;

import org.h2.value.Value;

/**
 * 向量化查询的一次性一致性快照：在极短读锁内截取不可变段数组、
 * 被 delta 屏蔽的段内偏移以及 delta/开放段活跃行，之后锁外无锁计算。
 */
public final class VectorizedSnapshot {

    public final ColumnSegment[] segments;

    public final Map<Integer, BitSet> maskedBySegment;

    public final List<Value[]> extraRows;

    /** 每段的 delta 屏蔽位版本，供部分聚合缓存判定条目是否仍有效。 */
    public final Map<Integer, Long> deltaVersions;

    VectorizedSnapshot(ColumnSegment[] segments,
            Map<Integer, BitSet> maskedBySegment, List<Value[]> extraRows,
            Map<Integer, Long> deltaVersions) {
        this.segments = segments;
        this.maskedBySegment = maskedBySegment;
        this.extraRows = extraRows;
        this.deltaVersions = deltaVersions;
    }
}

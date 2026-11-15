/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.storage;

import org.h2.engine.Constants;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;
import org.h2.value.ValueBigint;

/**
 * 延迟物化行视图。
 *
 * <p>H2 在 {@code TableFilter} 过滤与投影时通过 {@link #getValue(int)} 按列按需取值，
 * 未被引用的列与前置条件短路淘汰的列不会触发解码与装箱。只有当 H2 调用
 * {@link #getValueList()} 时才按需补齐整行 {@code Value[]}。
 *
 * <p>底层持有的 {@link ColumnSegment} 在冻结后保持事实不可变（合并时通过
 * Copy-On-Compact 生成新段），因此本视图在读锁之外按列解码是并发安全的。
 */
public final class LazyOmapRow extends Row {

    private final ColumnSegment segment;

    private final int offset;

    private Value[] materialized;

    public LazyOmapRow(ColumnSegment segment, int offset, long rowId) {
        this.segment = segment;
        this.offset = offset;
        this.key = rowId;
    }

    @Override
    public Value getValue(int i) {
        if (i == ROWID_INDEX) {
            return ValueBigint.get(key);
        }
        if (materialized != null) {
            return materialized[i];
        }
        return segment.get(offset, i);
    }

    @Override
    public void setValue(int i, Value v) {
        if (i == ROWID_INDEX) {
            key = v.getLong();
            return;
        }
        getValueList()[i] = v;
    }

    @Override
    public int getColumnCount() {
        return segment.getColumnCount();
    }

    @Override
    public int getMemory() {
        if (materialized == null) {
            return Constants.MEMORY_ROW + 24;
        }
        int m = Constants.MEMORY_ROW + Constants.MEMORY_ARRAY
                + materialized.length * Constants.MEMORY_POINTER;
        for (Value v : materialized) {
            if (v != null) {
                m += v.getMemory();
            }
        }
        return m;
    }

    @Override
    public Value[] getValueList() {
        if (materialized == null) {
            materialized = segment.getRow(offset);
        }
        return materialized;
    }

    @Override
    public void copyFrom(SearchRow source) {
        setKey(source.getKey());
        Value[] list = getValueList();
        for (int i = 0; i < list.length; i++) {
            list[i] = source.getValue(i);
        }
    }

    @Override
    public String toString() {
        return "( /* key:" + key + " lazy seg=" + segment.getSegmentId()
                + " off=" + offset + " */ )";
    }
}

/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.storage;

import java.util.ArrayList;
import java.util.List;

import org.h2.value.Value;

/**
 * 当前开放段的行缓冲。
 *
 * <p>每行在 0 号位置存 rowId，其余位置存值。段冻结时直接从缓冲里
 * 取出行数组，不需要调用方再传一次 rowId。
 */
public final class SegmentBuilder {

    private final List<Value[]> rows = new ArrayList<>();

    /** 追加一行，rowId 在 0 号元素。 */
    public void append(Value[] row) {
        rows.add(row);
    }

    /** 替换某行，行内容里 0 号元素是 rowId。 */
    public void setRow(int index, Value[] row) {
        rows.set(index, row);
    }

    public int rowCount() {
        return rows.size();
    }

    /** 取出某行，不含 0 号位置的 rowId。 */
    public Value[] rowValues(int index) {
        Value[] row = rows.get(index);
        Value[] values = new Value[row.length - 1];
        System.arraycopy(row, 1, values, 0, values.length);
        return values;
    }

    /** 取出全部行并清空缓冲。 */
    public List<Value[]> drain() {
        List<Value[]> drained = new ArrayList<>(rows);
        rows.clear();
        return drained;
    }

    public List<Value[]> rawRows() {
        return rows;
    }

    public void clear() {
        rows.clear();
    }
}

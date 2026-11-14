/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.index;

import org.h2.index.Cursor;
import org.h2.result.Row;
import org.h2.result.SearchRow;

import com.hyd.h2omap.storage.ColumnarStore;

/**
 * 扫描游标。H2 的取数模型是行式的：一次推进一行，再取整行。
 *
 * <p>这里的策略是迟物化。{@link #next()} 只移动 rowId 数组下标，
 * 真正拼 {@code Row} 的动作推迟到 {@link #get()}。这样没被引用的
 * 行完全不会产生装箱开销。
 *
 * <p>rowId 数组由 {@code ColumnarStore#collectVisibleRowIds} 预先算好。
 * 第一版先物化 rowId 列表，后续可以换成按段流式推进。
 */
public final class OmapCursor implements Cursor {

    private final ColumnarStore store;

    private final long[] rowIds;

    private int position = -1;

    private Row current;

    public OmapCursor(ColumnarStore store, long[] rowIds) {
        this.store = store;
        this.rowIds = rowIds;
    }

    @Override
    public Row get() {
        if (current == null && position >= 0 && position < rowIds.length) {
            current = store.materialize(rowIds[position]);
        }
        return current;
    }

    @Override
    public SearchRow getSearchRow() {
        return get();
    }

    @Override
    public boolean next() {
        int next = position + 1;
        if (next >= rowIds.length) {
            position = rowIds.length;
            current = null;
            return false;
        }
        position = next;
        current = null;
        return true;
    }

    @Override
    public boolean previous() {
        int prev = position - 1;
        if (prev < 0) {
            position = -1;
            current = null;
            return false;
        }
        position = prev;
        current = null;
        return true;
    }
}

/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.index;

import org.h2.index.Cursor;
import org.h2.result.Row;
import org.h2.result.SearchRow;

import com.hyd.h2omap.storage.ColumnarStore;

/**
 * 扫描游标。rowId 来自存储层的行迭代器，取行时按 rowId 物化。
 *
 * <p>只支持正向或反向的单向推进，{@link #previous()} 不支持：
 * H2 的 {@code IndexCursor.previous()} 本身就直接抛内部错误，不会调用到这里。
 */
public final class OmapCursor implements Cursor {

    private final ColumnarStore store;

    private final ColumnarStore.RowIterator rows;

    private Row current;

    public OmapCursor(ColumnarStore store, ColumnarStore.RowIterator rows) {
        this.store = store;
        this.rows = rows;
    }

    @Override
    public Row get() {
        return current;
    }

    @Override
    public SearchRow getSearchRow() {
        return current;
    }

    /**
     * 推进并立刻物化。物化放在这里而不是 {@code get()}，
     * 是为了避免「迭代器说这行可见、物化时已被删除」的空行。
     */
    @Override
    public boolean next() {
        while (true) {
            long next = rows.next();
            if (next < 0) {
                current = null;
                return false;
            }
            Row row = store.materialize(next);
            if (row != null) {
                current = row;
                return true;
            }
        }
    }

    @Override
    public boolean previous() {
        throw new UnsupportedOperationException("previous()");
    }
}

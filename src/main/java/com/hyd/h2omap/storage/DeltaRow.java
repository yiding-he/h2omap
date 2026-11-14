/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.storage;

import org.h2.value.Value;

/**
 * delta 里的一行。delta 是行式的，值就用 H2 的 {@link Value} 数组，
 * 不做列式组织，这样点改的代价与列数无关。
 */
public final class DeltaRow {

    private final long rowId;

    private final Value[] values;

    private boolean deleted;

    public DeltaRow(long rowId, Value[] values) {
        this.rowId = rowId;
        this.values = values;
    }

    public long getRowId() {
        return rowId;
    }

    public Value[] getValues() {
        return values;
    }

    public boolean isDeleted() {
        return deleted;
    }

    public void setDeleted(boolean deleted) {
        this.deleted = deleted;
    }
}

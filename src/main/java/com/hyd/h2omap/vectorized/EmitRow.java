/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.vectorized;

import org.h2.engine.Constants;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;

import com.hyd.h2omap.vector.ColumnVector;

/**
 * 流式执行（M20 有序 / M23 平面）发射期求值用的行视图：
 * 列值直接从段列向量或活像行读取，经 {@code TableFilter.set(Row)} 就位后
 * 供 {@code Expression.getValue(session)} 逐行求值。
 */
final class EmitRow extends Row {

    private final ColumnVector[] vecs;
    private final int[] colToVec;
    private final Value[] row;
    private final int idx;

    EmitRow(ColumnVector[] vecs, int[] colToVec, int idx, long rowId) {
        this.vecs = vecs;
        this.colToVec = colToVec;
        this.row = null;
        this.idx = idx;
        this.key = rowId;
    }

    EmitRow(Value[] row, long rowId) {
        this.vecs = null;
        this.colToVec = null;
        this.row = row;
        this.idx = -1;
        this.key = rowId;
    }

    @Override
    public Value getValue(int i) {
        return vecs != null ? vecs[colToVec[i]].get(idx) : row[i];
    }

    @Override
    public void setValue(int i, Value v) {
        throw new UnsupportedOperationException("流式发射行不可写入");
    }

    @Override
    public void copyFrom(SearchRow source) {
        throw new UnsupportedOperationException("流式发射行不可写入");
    }

    @Override
    public int getColumnCount() {
        return vecs != null ? colToVec.length : row.length;
    }

    @Override
    public Value[] getValueList() {
        throw new UnsupportedOperationException("流式发射行不支持整行物化");
    }

    @Override
    public int getMemory() {
        return Constants.MEMORY_ROW + 24;
    }
}

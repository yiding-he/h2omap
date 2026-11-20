/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.storage;

import com.hyd.h2omap.vector.ColumnVector;
import com.hyd.h2omap.vector.NullBitmap;
import org.h2.value.Value;

/**
 * 部分列读段里未被请求的列占位：任何访问都抛出异常。
 *
 * <p>向量化快照的执行器只触碰编译期给出的列集（谓词列、聚合列、分组键、投影列），
 * 占位列不会被读到。若有代码路径意外访问占位列，这里以异常显式暴露而不是静默
 * 返回错误值。
 */
public final class AbsentColumnVector implements ColumnVector {

    /** 无状态，全部占位共享一个实例。 */
    public static final AbsentColumnVector INSTANCE = new AbsentColumnVector();

    private AbsentColumnVector() {
    }

    private static UnsupportedOperationException unsupported() {
        return new UnsupportedOperationException("列未在部分列读的请求列集内");
    }

    @Override
    public int size() {
        throw unsupported();
    }

    @Override
    public Value get(int index) {
        throw unsupported();
    }

    @Override
    public void set(int index, Value value) {
        throw unsupported();
    }

    @Override
    public NullBitmap nulls() {
        throw unsupported();
    }

    @Override
    public long getEstimatedMemory() {
        throw unsupported();
    }

    @Override
    public ColumnVector copy() {
        throw unsupported();
    }
}

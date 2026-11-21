/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.storage;

import org.h2.value.Value;

/** 裸 MIN/MAX 定界求值的结果；supported=false 表示形态不支持，交回常规向量化路径。 */
public final class ExtremeValue {

    public final boolean supported;

    public final Value value;

    private ExtremeValue(boolean supported, Value value) {
        this.supported = supported;
        this.value = value;
    }

    static ExtremeValue unsupported() {
        return new ExtremeValue(false, null);
    }

    static ExtremeValue of(Value value) {
        return new ExtremeValue(true, value);
    }
}

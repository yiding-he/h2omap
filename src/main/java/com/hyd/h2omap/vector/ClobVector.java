/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.vector;

import java.util.Arrays;

import org.h2.value.Value;
import org.h2.value.ValueClob;
import org.h2.value.ValueNull;

/**
 * CLOB 列：复用 {@link VarcharVector} 的「UTF-8 字节池 + 行偏移」机制，
 * 区别只在 {@link #get(int)} 时装箱成 {@link ValueClob}，消掉逐值的
 * {@code ValueClob + LobDataInMemory + byte[]} 对象开销；内容仍驻堆，
 * 过滤与投影路径只多一次字节切片，不引入读盘。
 *
 * <p>谓词语义与打包前一致：等值比较走 {@code filterGeneric} 逐值比较，
 * LIKE 编译入口只接受 VARCHAR/CHAR 列、照旧交回 H2 行式路径。
 */
public final class ClobVector extends VarcharVector {

    public ClobVector(int capacity) {
        super(capacity);
    }

    private ClobVector(int[] starts, byte[] data, int filled, NullBitmap nulls) {
        super(starts, data, filled, nulls);
    }

    /**
     * 由段文件恢复。{@code data} 的有效长度是 {@code starts[starts.length - 1]}，
     * 调用方可以把读缓冲直接交给这里。
     */
    public static ClobVector of(int[] starts, byte[] data, NullBitmap nulls) {
        int filled = starts.length - 1;
        return new ClobVector(starts, trimToUsed(starts, data), filled, nulls);
    }

    @Override
    public Value get(int index) {
        if (nulls().isNull(index)) {
            return ValueNull.INSTANCE;
        }
        int[] starts = rawStarts();
        int from = starts[index];
        return ValueClob.createSmall(Arrays.copyOfRange(rawData(), from, starts[index + 1]));
    }

    @Override
    public ColumnVector copy() {
        return new ClobVector(rawStarts().clone(), rawData().clone(), filledRows(), nulls().copy());
    }
}

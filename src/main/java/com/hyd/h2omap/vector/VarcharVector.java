/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.vector;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.ValueVarchar;

/**
 * CHAR / VARCHAR 列：UTF-8 字节池加行偏移，不逐行持有 {@code String}。
 *
 * <p>每行占用约「长度 + 4 字节偏移」，与段文件里的写法一致；{@link #get(int)}
 * 时才解码成 {@code String} 与 {@link ValueVarchar}。CLOB 列由子类
 * {@link ClobVector} 复用同一机制，只在装箱时生成 {@code ValueClob}。
 */
public class VarcharVector implements ColumnVector {

    /** 行的结束位置构成前缀和，长度是容量加一；NULL 行的起止相等。 */
    private final int[] starts;

    private byte[] data;

    private int filled;

    private final NullBitmap nulls;

    private static final byte[] EMPTY = new byte[0];

    public VarcharVector(int capacity) {
        this.starts = new int[capacity + 1];
        this.data = new byte[Math.max(16, capacity * 8)];
        this.nulls = new NullBitmap(capacity);
    }

    /** 供子类 ClobVector 复用打包存储与复制。 */
    protected VarcharVector(int[] starts, byte[] data, int filled, NullBitmap nulls) {
        this.starts = starts;
        this.data = data;
        this.filled = filled;
        this.nulls = nulls;
    }

    /**
     * 由段文件恢复。{@code data} 的有效长度是 {@code starts[starts.length - 1]}，
     * 调用方可以把读缓冲直接交给这里。
     */
    public static VarcharVector of(int[] starts, byte[] data, NullBitmap nulls) {
        int filled = starts.length - 1;
        return new VarcharVector(starts, trimToUsed(starts, data), filled, nulls);
    }

    /** 裁掉读缓冲里前缀和末尾之后的字节，供恢复入口共用。 */
    static byte[] trimToUsed(int[] starts, byte[] data) {
        int used = starts[starts.length - 1];
        return used == data.length ? data : Arrays.copyOf(data, used);
    }

    @Override
    public ColumnVector copy() {
        return new VarcharVector(starts.clone(), data.clone(), filled, nulls.copy());
    }

    @Override
    public int size() {
        return starts.length - 1;
    }

    @Override
    public Value get(int index) {
        return nulls.isNull(index) ? ValueNull.INSTANCE : ValueVarchar.get(getString(index));
    }

    /** 解码一行的字符串；NULL 行返回 {@code null}。 */
    public String getString(int index) {
        if (nulls.isNull(index)) {
            return null;
        }
        return new String(data, starts[index], starts[index + 1] - starts[index],
                StandardCharsets.UTF_8);
    }

    @Override
    public void set(int index, Value value) {
        boolean isNull = value == null || value.getValueType() == Value.NULL;
        byte[] utf8 = isNull ? EMPTY : value.getString().getBytes(StandardCharsets.UTF_8);
        if (index >= filled) {
            // 顺序追加：建段与数组列重建都走这条路径。
            int base = extendTo(index);
            ensure(index + 1, base + utf8.length);
            System.arraycopy(utf8, 0, data, base, utf8.length);
            starts[index + 1] = base + utf8.length;
        } else {
            int base = starts[index];
            int oldLength = starts[index + 1] - base;
            if (oldLength == utf8.length) {
                ensure(filled, starts[filled]);
            } else {
                // 合并写回时值长度变化（含改成 NULL），把后面的行整体挪动 delta 字节，
                // 前缀和同步调整，否则后面的行会读到错位的字节。
                int delta = utf8.length - oldLength;
                int tailStart = starts[index + 1];
                int tailEnd = starts[filled];
                ensure(filled, tailEnd + delta);
                System.arraycopy(data, tailStart, data, tailStart + delta, tailEnd - tailStart);
                for (int r = index + 1; r <= filled; r++) {
                    starts[r] += delta;
                }
            }
            System.arraycopy(utf8, 0, data, base, utf8.length);
        }
        if (isNull) {
            nulls.setNull(index);
        } else {
            nulls.clear(index);
        }
    }

    /** 把前缀和铺到 {@code index}，返回该行的起始偏移；已写过的行原样返回。 */
    private int extendTo(int index) {
        if (index < filled) {
            return starts[index];
        }
        int tail = starts[filled];
        for (int r = filled + 1; r <= index; r++) {
            starts[r] = tail;
        }
        filled = index + 1;
        return tail;
    }

    private void ensure(int rows, int bytes) {
        if (rows > starts.length - 1) {
            throw new IllegalStateException(
                    "VarcharVector 已写满：" + rows + " > " + (starts.length - 1));
        }
        if (bytes > data.length) {
            int size = Math.max(bytes, data.length * 2);
            data = Arrays.copyOf(data, size);
        }
    }

    public int[] rawStarts() {
        return starts;
    }

    public byte[] rawData() {
        return data;
    }

    /** 已写入的行数；段文件只写这一部分。 */
    public int filledRows() {
        return filled;
    }

    @Override
    public NullBitmap nulls() {
        return nulls;
    }

    @Override
    public long getEstimatedMemory() {
        return 32L + data.length + (long) starts.length * Integer.BYTES + nulls.getEstimatedMemory();
    }
}

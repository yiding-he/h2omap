/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.vector;

import org.h2.value.Value;
import org.h2.value.ValueInteger;
import org.h2.value.ValueNull;

/**
 * TINYINT / SMALLINT / INTEGER 列。
 *
 * <p>两种形态：<b>可写态</b>持有 {@code int[]}（开放段写入、合并回写 {@link #set}）；
 * <b>编码态</b>持有位打包 {@link BitPacked}，段冻结时由 {@link #tryEncode} 选定。
 * 编码态不可写：{@link #copy} 与 {@link #set} 先解包回可写态。
 * 查询路径用 {@link #longAt} 单点读，两种形态都是常数时间——这是只做位打包、
 * 不做差分编码的原因：差分态的随机单点读要 O(index) 累加，而向量化路径按
 * 选择向量随机访问占多数。
 */
public final class IntVector implements ColumnVector {

    /** 编码选择阈值：编码后字节数不超过原始的 85% 才采用。 */
    static final double ENCODE_RATIO = 0.85;

    private int[] values;
    private BitPacked packed;

    private final NullBitmap nulls;

    public IntVector(int capacity) {
        this.values = new int[capacity];
        this.nulls = new NullBitmap(capacity);
    }

    private IntVector(int[] values, NullBitmap nulls) {
        this.values = values;
        this.nulls = nulls;
    }

    public static IntVector of(int[] values, NullBitmap nulls) {
        return new IntVector(values, nulls);
    }

    /**
     * 按列值域选编码：按最大值位宽位打包（全同值列基数 1，建段时已走单值字典，不会到这里）；
     * 编码后字节数不比原始数组省 {@link #ENCODE_RATIO} 以上则返回 {@code null}。
     * 含 NULL 的行以其实际存储值参与编码（NULL 由位图判定，不参与比较）。
     */
    public static IntVector tryEncode(IntVector v) {
        int[] a = v.values;
        if (a == null) {
            return null;
        }
        int n = a.length;
        if (n == 0) {
            return null;
        }
        long max = 0;
        for (int i = 0; i < n; i++) {
            int x = a[i];
            if (x > max) {
                max = x;
            }
        }
        IntVector out = new IntVector(null, v.nulls);
        int w = BitPacked.bitsFor(max);
        if (w <= 0 || w >= Integer.SIZE) {
            return null;
        }
        BitPacked p = BitPacked.pack(a, w);
        if (p.getEstimatedMemory() > (long) n * Integer.BYTES * ENCODE_RATIO) {
            return null;
        }
        out.packed = p;
        return out;
    }

    @Override
    public ColumnVector copy() {
        return new IntVector(values != null ? values.clone() : rawValues(), nulls.copy());
    }

    @Override
    public int size() {
        return packed != null ? packed.size() : values.length;
    }

    @Override
    public Value get(int index) {
        return nulls.isNull(index) ? ValueNull.INSTANCE : ValueInteger.get((int) longAt(index));
    }

    /** 编码态不可写；当前全部调用方都先经 {@link #copy()} 取可写态，这里的解包只是接口契约的防线。 */
    @Override
    public void set(int index, Value value) {
        if (value.getValueType() == Value.NULL) {
            nulls.setNull(index);
            return;
        }
        if (values == null) {
            values = rawValues();
            packed = null;
        }
        values[index] = value.getInt();
        nulls.clear(index);
    }

    @Override
    public NullBitmap nulls() {
        return nulls;
    }

    /** 查询路径的单点读，两种形态常数时间。 */
    public long longAt(int index) {
        if (values != null) {
            return values[index];
        }
        return packed.get(index);
    }

    /** 整列解包（不缓存），供序列化、复制与行式路径批量取值。 */
    public int[] rawValues() {
        if (values != null) {
            return values;
        }
        int n = packed.size();
        int[] out = new int[n];
        for (int i = 0; i < n; i++) {
            out[i] = (int) packed.get(i);
        }
        return out;
    }

    public boolean isEncoded() {
        return values == null;
    }

    @Override
    public long getEstimatedMemory() {
        long body = values != null
                ? 16L + (long) values.length * Integer.BYTES
                : packed.getEstimatedMemory();
        return 16L + body + nulls.getEstimatedMemory();
    }
}

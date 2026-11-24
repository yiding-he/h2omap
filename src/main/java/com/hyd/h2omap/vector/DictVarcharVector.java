/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.vector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * 低基数 CHAR / VARCHAR 列的字典编码向量。
 *
 * <p>段内去重值预存为不可变 {@link Value} 字典数组，每行只存字典下标：
 * 建段后是位打包 {@link BitPacked}（宽度 {@code ceil(log2(字典大小))}，10 个取值的列每行 4 位），
 * 开放段写入期间是 {@code int[]}（{@code -1} 表示 null）。读取时直接按编码返回缓存的
 * {@link Value} 引用，零字符串解码与包装对象分配。
 */
public final class DictVarcharVector implements ColumnVector {

    /** 可写态的码（{@code -1} 表示 null）；编码态为 {@code null}。 */
    private int[] codes;

    /** 编码态的位打包码；可写态为 {@code null}。 */
    private BitPacked packedCodes;

    private Value[] dict;

    private int dictSize;

    private Map<String, Integer> dictLookup;

    private final NullBitmap nulls;

    private DictVarcharVector(int[] codes, Value[] dict, int dictSize,
            Map<String, Integer> dictLookup, NullBitmap nulls) {
        this.codes = codes;
        this.packedCodes = null;
        this.dict = dict;
        this.dictSize = dictSize;
        this.dictLookup = dictLookup;
        this.nulls = nulls;
    }

    /**
     * 尝试按字典编码构建列向量；若去重数超过 {@code maxDictSize}，立刻中止并返回 {@code null}。
     */
    public static DictVarcharVector tryBuild(List<Value[]> rows, int valueColIndex, int maxDictSize) {
        int count = rows.size();
        int[] codes = new int[count];
        NullBitmap nulls = new NullBitmap(count);
        Map<String, Integer> lookup = new HashMap<>();
        List<Value> dictList = new ArrayList<>();

        for (int r = 0; r < count; r++) {
            Value v = rows.get(r)[valueColIndex];
            if (v.getValueType() == Value.NULL) {
                codes[r] = -1;
                nulls.setNull(r);
                continue;
            }
            String s = v.getString();
            Integer existing = lookup.get(s);
            if (existing != null) {
                codes[r] = existing;
            } else {
                if (dictList.size() >= maxDictSize) {
                    return null;
                }
                int id = dictList.size();
                lookup.put(s, id);
                dictList.add(v);
                codes[r] = id;
            }
        }
        DictVarcharVector v = new DictVarcharVector(
                codes, dictList.toArray(new Value[0]), dictList.size(), null, nulls);
        v.packCodes();
        return v;
    }

    /** 把可写态的码按字典大小位打包；字典大小为 1 时用 1 位区分「null / 该值」。 */
    private void packCodes() {
        if (codes == null || dictSize <= 0) {
            return;
        }
        if (dictSize == 1) {
            packedCodes = BitPacked.pack(codes.length, 1, i -> codes[i] < 0 ? 0 : 1);
            singleValue = true;
        } else {
            int w = BitPacked.bitsFor(dictSize - 1);
            packedCodes = BitPacked.pack(codes.length, w, i -> codes[i] < 0 ? 0 : codes[i]);
        }
        codes = null;
    }

    private boolean singleValue;

    public static DictVarcharVector of(int[] codes, Value[] dict, NullBitmap nulls) {
        DictVarcharVector v = new DictVarcharVector(codes, dict, dict.length, null, nulls);
        v.packCodes();
        return v;
    }

    /** 可写形态：开放段写入与合并回写用，码保持 {@code int[]}。 */
    private static DictVarcharVector writable(int[] codes, Value[] dict, int dictSize, NullBitmap nulls) {
        return new DictVarcharVector(codes, dict, dictSize, null, nulls);
    }

    @Override
    public ColumnVector copy() {
        return writable(codes != null ? codes.clone() : rawCodes(), Arrays.copyOf(dict, dictSize), dictSize,
                nulls.copy());
    }

    @Override
    public int size() {
        return packedCodes != null ? packedCodes.size() : codes.length;
    }

    @Override
    public Value get(int index) {
        int code = codeAt(index);
        return code < 0 ? ValueNull.INSTANCE : dict[code];
    }

    /** 编码态不可写；当前全部调用方都先经 {@link #copy()} 取可写态，这里的解包只是接口契约的防线。 */
    @Override
    public void set(int index, Value value) {
        if (codes == null) {
            unpackInPlace();
        }
        if (value.getValueType() == Value.NULL) {
            codes[index] = -1;
            nulls.setNull(index);
            return;
        }
        if (dictLookup == null) {
            dictLookup = new HashMap<>(dictSize * 2 + 1);
            for (int i = 0; i < dictSize; i++) {
                dictLookup.put(dict[i].getString(), i);
            }
        }
        String s = value.getString();
        Integer existing = dictLookup.get(s);
        int id;
        if (existing != null) {
            id = existing;
        } else {
            id = dictSize++;
            if (id >= dict.length) {
                dict = Arrays.copyOf(dict, Math.max(8, dict.length * 2));
            }
            dict[id] = value;
            dictLookup.put(s, id);
        }
        codes[index] = id;
        nulls.clear(index);
    }

    private void unpackInPlace() {
        this.codes = rawCodes();
        this.packedCodes = null;
        this.singleValue = false;
    }

    /** 整列码解包（不缓存），供序列化与复制。 */
    public int[] rawCodes() {
        if (codes != null) {
            return codes;
        }
        int n = packedCodes.size();
        int[] out = new int[n];
        for (int i = 0; i < n; i++) {
            out[i] = codeAt(i);
        }
        return out;
    }

    /** 查询路径的单点读：字典下标，{@code -1} 表示 null。两种形态常数时间。 */
    public int codeAt(int index) {
        if (codes != null) {
            return codes[index];
        }
        if (singleValue) {
            return packedCodes.get(index) == 0 ? -1 : 0;
        }
        return nulls.isNull(index) ? -1 : (int) packedCodes.get(index);
    }

    public boolean isEncoded() {
        return codes == null;
    }

    public Value[] rawDict() {
        return dict;
    }

    public int dictSize() {
        return dictSize;
    }

    @Override
    public NullBitmap nulls() {
        return nulls;
    }

    @Override
    public long getEstimatedMemory() {
        long dictMem = 16L + (long) dict.length * Integer.BYTES;
        for (int i = 0; i < dictSize; i++) {
            dictMem += dict[i].getMemory();
        }
        long codeMem = codes != null
                ? 16L + (long) codes.length * Integer.BYTES
                : packedCodes.getEstimatedMemory();
        return 16L + codeMem + dictMem + nulls.getEstimatedMemory();
    }
}

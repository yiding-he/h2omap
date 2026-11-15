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
 * 低基数数值列的字典编码向量（支持 TINYINT / SMALLINT / INTEGER / BIGINT / REAL / DOUBLE）。
 *
 * <p>段内去重值预存为不可变 {@link Value} 字典数组，每行仅存 4 字节整数编码
 * （{@code -1} 表示 null）。读取时直接按编码返回缓存的 {@link Value} 引用，
 * 消除重复装箱分配。
 */
public final class DictValueVector implements ColumnVector {

    private final int[] codes;

    private Value[] dict;

    private int dictSize;

    private Map<Value, Integer> dictLookup;

    private final NullBitmap nulls;

    private DictValueVector(int[] codes, Value[] dict, int dictSize,
            Map<Value, Integer> dictLookup, NullBitmap nulls) {
        this.codes = codes;
        this.dict = dict;
        this.dictSize = dictSize;
        this.dictLookup = dictLookup;
        this.nulls = nulls;
    }

    /**
     * 尝试按字典编码构建列向量；若去重数超过 {@code maxDictSize}，立刻中止并返回 {@code null}。
     */
    public static DictValueVector tryBuild(List<Value[]> rows, int valueColIndex, int maxDictSize) {
        int count = rows.size();
        int[] codes = new int[count];
        NullBitmap nulls = new NullBitmap(count);
        Map<Value, Integer> lookup = new HashMap<>();
        List<Value> dictList = new ArrayList<>();

        for (int r = 0; r < count; r++) {
            Value v = rows.get(r)[valueColIndex];
            if (v.getValueType() == Value.NULL) {
                codes[r] = -1;
                nulls.setNull(r);
                continue;
            }
            Integer existing = lookup.get(v);
            if (existing != null) {
                codes[r] = existing;
            } else {
                if (dictList.size() >= maxDictSize) {
                    return null;
                }
                int id = dictList.size();
                lookup.put(v, id);
                dictList.add(v);
                codes[r] = id;
            }
        }
        return new DictValueVector(
                codes, dictList.toArray(new Value[0]), dictList.size(), null, nulls);
    }

    @Override
    public ColumnVector copy() {
        return new DictValueVector(
                codes.clone(), Arrays.copyOf(dict, dictSize), dictSize, null, nulls.copy());
    }

    @Override
    public int size() {
        return codes.length;
    }

    @Override
    public Value get(int index) {
        int code = codes[index];
        return code < 0 ? ValueNull.INSTANCE : dict[code];
    }

    @Override
    public void set(int index, Value value) {
        if (value.getValueType() == Value.NULL) {
            codes[index] = -1;
            nulls.setNull(index);
            return;
        }
        if (dictLookup == null) {
            dictLookup = new HashMap<>(dictSize * 2 + 1);
            for (int i = 0; i < dictSize; i++) {
                dictLookup.put(dict[i], i);
            }
        }
        Integer existing = dictLookup.get(value);
        int id;
        if (existing != null) {
            id = existing;
        } else {
            id = dictSize++;
            if (id >= dict.length) {
                dict = Arrays.copyOf(dict, Math.max(8, dict.length * 2));
            }
            dict[id] = value;
            dictLookup.put(value, id);
        }
        codes[index] = id;
        nulls.clear(index);
    }

    public int[] rawCodes() {
        return codes;
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
        return 32L + (long) codes.length * Integer.BYTES
                + (long) dict.length * 32L + nulls.getEstimatedMemory();
    }
}

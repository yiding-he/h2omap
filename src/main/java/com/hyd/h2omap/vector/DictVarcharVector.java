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
 * <p>段内去重值预存为不可变 {@link Value} 字典数组，每行仅存 4 字节整数编码
 * （{@code -1} 表示 null）。读取时直接按编码返回缓存的 {@link Value} 引用，
 * 零字符串解码与包装对象分配。
 */
public final class DictVarcharVector implements ColumnVector {

    private final int[] codes;

    private Value[] dict;

    private int dictSize;

    private Map<String, Integer> dictLookup;

    private final NullBitmap nulls;

    private DictVarcharVector(int[] codes, Value[] dict, int dictSize,
            Map<String, Integer> dictLookup, NullBitmap nulls) {
        this.codes = codes;
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
        return new DictVarcharVector(
                codes, dictList.toArray(new Value[0]), dictList.size(), null, nulls);
    }

    @Override
    public ColumnVector copy() {
        return new DictVarcharVector(
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
        return 16L + (long) codes.length * Integer.BYTES + dictMem + nulls.getEstimatedMemory();
    }
}

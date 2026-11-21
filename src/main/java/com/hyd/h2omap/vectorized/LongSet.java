package com.hyd.h2omap.vectorized;

/**
 * 原始 long 开放寻址集合，供整数列的 IN 过滤避开 Value 装箱哈希。
 * 固定容量下满槽会让开放寻址死循环，因此按元素个数扩容。
 */
final class LongSet {
    private long[] keys;
    private boolean[] used;
    private int mask;
    private int size;

    LongSet(int expected) {
        int cap = Integer.highestOneBit(Math.max(8, expected * 2 - 1)) << 1;
        keys = new long[cap];
        used = new boolean[cap];
        mask = cap - 1;
    }

    void add(long key) {
        // 固定容量下满槽会让开放寻址死循环；按元素个数扩容，
        // 预估容量的调用方（InNode）永远不会触到阈值，只有逐步增长的调用方（SubqueryFold）会。
        if (size * 4 > mask * 3) {
            rehash();
        }
        int i = (int) (mix(key) & mask);
        while (used[i]) {
            if (keys[i] == key) {
                return;
            }
            i = (i + 1) & mask;
        }
        used[i] = true;
        keys[i] = key;
        size++;
    }

    boolean contains(long key) {
        int i = (int) (mix(key) & mask);
        while (used[i]) {
            if (keys[i] == key) {
                return true;
            }
            i = (i + 1) & mask;
        }
        return false;
    }

    private void rehash() {
        long[] oldKeys = keys;
        boolean[] oldUsed = used;
        int cap = (mask + 1) << 1;
        keys = new long[cap];
        used = new boolean[cap];
        mask = cap - 1;
        for (int i = 0; i < oldKeys.length; i++) {
            if (oldUsed[i]) {
                int j = (int) (mix(oldKeys[i]) & mask);
                while (used[j]) {
                    j = (j + 1) & mask;
                }
                used[j] = true;
                keys[j] = oldKeys[i];
            }
        }
    }

    private static long mix(long key) {
        long h = key * 0x9E3779B97F4A7C15L;
        return h ^ (h >>> 32);
    }

    /** 全部元素的有序快照，供签名使用（开放寻址的槽位顺序不稳定）。 */
    long[] values() {
        long[] out = new long[size];
        int p = 0;
        for (int i = 0; i < keys.length; i++) {
            if (used[i]) {
                out[p++] = keys[i];
            }
        }
        java.util.Arrays.sort(out);
        return out;
    }
}

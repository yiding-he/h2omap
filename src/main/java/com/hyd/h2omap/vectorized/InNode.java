package com.hyd.h2omap.vectorized;

import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.IntConsumer;

import org.h2.engine.SessionLocal;
import org.h2.value.CompareMode;
import org.h2.value.Value;
import org.h2.value.ValueNull;

import com.hyd.h2omap.storage.ColumnSegment;
import com.hyd.h2omap.vector.ColumnVector;
import com.hyd.h2omap.vector.DictValueVector;
import com.hyd.h2omap.vector.DictVarcharVector;
import com.hyd.h2omap.vector.IntVector;
import com.hyd.h2omap.vector.LongVector;

/**
 * {@code IN} / {@code NOT IN} 常量集合节点。整数族列走原始 {@code long} 开放寻址集合
 * （{@link LongSet}），字典列先在字典上算出允许码位图。
 */
final class InNode implements VectorizedFilter.Node {
    private final int col;
    private final boolean not;
    private final Set<Value> set;
    private final LongSet longSet;

    InNode(int col, boolean not, List<Value> values) {
        this(col, not, new HashSet<>(values), null);
    }

    InNode(int col, boolean not, LongSet longSet) {
        this(col, not, null, longSet);
    }

    private InNode(int col, boolean not, Set<Value> set, LongSet longSet) {
        this.col = col;
        this.not = not;
        this.set = set;
        this.longSet = longSet;
    }

    @Override
    public void collectColumns(IntConsumer sink) {
        sink.accept(col);
    }

    @Override
    public String sig() {
        if (longSet != null) {
            return "I" + col + ',' + not + ",long" + java.util.Arrays.toString(longSet.values());
        }
        String[] valueSigs = new String[set.size()];
        int p = 0;
        for (Value v : set) {
            valueSigs[p++] = VectorizedFilter.valueSig(v);
        }
        java.util.Arrays.sort(valueSigs);
        return "I" + col + ',' + not + ',' + java.util.Arrays.toString(valueSigs);
    }

    @Override
    public int filterSegment(ColumnSegment segment, int[] sel, int selCount, boolean fullRange,
            BitSet masked, SessionLocal session, CompareMode compareMode) {
        ColumnVector vec = segment.getColumn(col);
        if (vec instanceof DictVarcharVector) {
            DictVarcharVector dv = (DictVarcharVector) vec;
            return filterDict(dv.rawCodes(), dv.rawDict(), dv.dictSize(), sel, selCount, fullRange);
        }
        if (vec instanceof DictValueVector) {
            DictValueVector dv = (DictValueVector) vec;
            return filterDict(dv.rawCodes(), dv.rawDict(), dv.dictSize(), sel, selCount, fullRange);
        }
        if (longSet != null && vec instanceof IntVector) {
            return filterLongSet(((IntVector) vec).rawValues(), sel, selCount, fullRange);
        }
        if (longSet != null && vec instanceof LongVector) {
            return filterLongSet(((LongVector) vec).rawValues(), sel, selCount, fullRange);
        }
        int out = 0;
        if (fullRange) {
            for (int i = 0; i < selCount; i++) {
                Value v = vec.get(i);
                if (v != ValueNull.INSTANCE && contains(v)) {
                    sel[out++] = i;
                }
            }
        } else {
            for (int i = 0; i < selCount; i++) {
                int idx = sel[i];
                Value v = vec.get(idx);
                if (v != ValueNull.INSTANCE && contains(v)) {
                    sel[out++] = idx;
                }
            }
        }
        return out;
    }

    private boolean contains(Value v) {
        if (longSet != null) {
            return longSet.contains(v.getLong()) ^ not;
        }
        return set.contains(v) ^ not;
    }

    private int filterLongSet(int[] vals, int[] sel, int selCount, boolean fullRange) {
        int out = 0;
        if (fullRange) {
            for (int i = 0; i < selCount; i++) {
                if (longSet.contains(vals[i]) ^ not) {
                    sel[out++] = i;
                }
            }
        } else {
            for (int i = 0; i < selCount; i++) {
                int idx = sel[i];
                if (longSet.contains(vals[idx]) ^ not) {
                    sel[out++] = idx;
                }
            }
        }
        return out;
    }

    private int filterLongSet(long[] vals, int[] sel, int selCount, boolean fullRange) {
        int out = 0;
        if (fullRange) {
            for (int i = 0; i < selCount; i++) {
                if (longSet.contains(vals[i]) ^ not) {
                    sel[out++] = i;
                }
            }
        } else {
            for (int i = 0; i < selCount; i++) {
                int idx = sel[i];
                if (longSet.contains(vals[idx]) ^ not) {
                    sel[out++] = idx;
                }
            }
        }
        return out;
    }

    private int filterDict(int[] codes, Value[] dict, int dictSize,
            int[] sel, int selCount, boolean fullRange) {
        boolean[] allowed = new boolean[dictSize];
        boolean anyAllowed = false;
        for (int d = 0; d < dictSize; d++) {
            if ((longSet != null ? longSet.contains(dict[d].getLong()) : set.contains(dict[d])) ^ not) {
                allowed[d] = true;
                anyAllowed = true;
            }
        }
        if (!anyAllowed) {
            return 0;
        }
        int out = 0;
        if (fullRange) {
            for (int i = 0; i < selCount; i++) {
                int code = codes[i];
                if (code >= 0 && allowed[code]) {
                    sel[out++] = i;
                }
            }
        } else {
            for (int i = 0; i < selCount; i++) {
                int idx = sel[i];
                int code = codes[idx];
                if (code >= 0 && allowed[code]) {
                    sel[out++] = idx;
                }
            }
        }
        return out;
    }

    @Override
    public boolean evalRow(Value[] row, SessionLocal session, CompareMode compareMode) {
        Value v = row[col];
        return v != ValueNull.INSTANCE && contains(v);
    }
}

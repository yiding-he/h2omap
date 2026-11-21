package com.hyd.h2omap.vectorized;

import java.util.BitSet;
import java.util.function.IntConsumer;

import org.h2.engine.SessionLocal;
import org.h2.expression.condition.CompareLike;
import org.h2.value.CompareMode;
import org.h2.value.Value;
import org.h2.value.ValueNull;

import com.hyd.h2omap.storage.ColumnSegment;
import com.hyd.h2omap.vector.ColumnVector;
import com.hyd.h2omap.vector.DictVarcharVector;
import com.hyd.h2omap.vector.NullBitmap;
import com.hyd.h2omap.vector.VarcharVector;

/**
 * {@code LIKE} / {@code NOT LIKE} 节点。字典列先在字典上算出匹配位图（{@code CompareLike}
 * 非线程安全，字典匹配与逐行匹配都加同一把锁）；打包字节池列直接逐行取字符串比较；
 * 其余列逐值取字符串。{@code LIKE} 不参与段级剪枝，因此也不提供列值区间。
 */
final class LikeNode implements VectorizedFilter.Node {
    private final int col;
    private final CompareLike like;
    private final String pattern;
    private final char escapeChar;
    private final boolean negate;

    LikeNode(int col, CompareLike like, String pattern, char escapeChar, boolean negate) {
        this.col = col;
        this.like = like;
        this.pattern = pattern;
        this.escapeChar = escapeChar;
        this.negate = negate;
    }

    @Override
    public void collectColumns(IntConsumer sink) {
        sink.accept(col);
    }

    @Override
    public String sig() {
        return "W" + col + ',' + pattern + ',' + (int) escapeChar + ',' + negate;
    }

    private boolean matchString(String s) {
        return like.test(pattern, s, escapeChar) ^ negate;
    }

    @Override
    public int filterSegment(ColumnSegment segment, int[] sel, int selCount, boolean fullRange,
            BitSet masked, SessionLocal session, CompareMode compareMode) {
        ColumnVector vec = segment.getColumn(col);
        int out = 0;
        if (vec instanceof DictVarcharVector) {
            DictVarcharVector dv = (DictVarcharVector) vec;
            int dictSize = dv.dictSize();
            Value[] dict = dv.rawDict();
            boolean[] dictMatched = new boolean[dictSize];
            synchronized (like) {
                for (int d = 0; d < dictSize; d++) {
                    dictMatched[d] = matchString(dict[d].getString());
                }
            }
            int[] codes = dv.rawCodes();
            if (fullRange) {
                for (int i = 0; i < selCount; i++) {
                    int c = codes[i];
                    if (c >= 0 && dictMatched[c]) {
                        sel[out++] = i;
                    }
                }
            } else {
                for (int i = 0; i < selCount; i++) {
                    int idx = sel[i];
                    int c = codes[idx];
                    if (c >= 0 && dictMatched[c]) {
                        sel[out++] = idx;
                    }
                }
            }
            return out;
        }
        if (vec instanceof VarcharVector) {
            VarcharVector vv = (VarcharVector) vec;
            NullBitmap nulls = vv.nulls();
            boolean hasNull = nulls.hasNull();
            synchronized (like) {
                if (fullRange) {
                    for (int i = 0; i < selCount; i++) {
                        if ((!hasNull || !nulls.isNull(i)) && matchString(vv.getString(i))) {
                            sel[out++] = i;
                        }
                    }
                } else {
                    for (int i = 0; i < selCount; i++) {
                        int idx = sel[i];
                        if ((!hasNull || !nulls.isNull(idx)) && matchString(vv.getString(idx))) {
                            sel[out++] = idx;
                        }
                    }
                }
            }
            return out;
        }
        synchronized (like) {
            if (fullRange) {
                for (int i = 0; i < selCount; i++) {
                    Value v = vec.get(i);
                    if (v != ValueNull.INSTANCE && matchString(v.getString())) {
                        sel[out++] = i;
                    }
                }
            } else {
                for (int i = 0; i < selCount; i++) {
                    int idx = sel[i];
                    Value v = vec.get(idx);
                    if (v != ValueNull.INSTANCE && matchString(v.getString())) {
                        sel[out++] = idx;
                    }
                }
            }
        }
        return out;
    }

    @Override
    public boolean evalRow(Value[] row, SessionLocal session, CompareMode compareMode) {
        Value v = row[col];
        if (v == ValueNull.INSTANCE) {
            return false;
        }
        synchronized (like) {
            return matchString(v.getString());
        }
    }
}

package com.hyd.h2omap.vectorized;

import java.util.BitSet;
import java.util.function.IntConsumer;

import org.h2.engine.SessionLocal;
import org.h2.expression.condition.Comparison;
import org.h2.value.CompareMode;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueNull;

import com.hyd.h2omap.storage.ColumnSegment;
import com.hyd.h2omap.vector.ArrayVector;
import com.hyd.h2omap.vector.ColumnVector;

/**
 * {@code CARDINALITY(数组列) 与常量比较} 节点。数组列在段内是
 * 「前缀和下标 {@code starts} + 元素列」布局，基数退化为一次整数减法，
 * 不重建每行的 {@code ValueArray}；非数组向量回退逐值取数组长度。
 */
final class CardinalityCmpNode implements VectorizedFilter.Node {
    private final int col;
    private final int cmpType;
    private final int target;

    CardinalityCmpNode(int col, int cmpType, int target) {
        this.col = col;
        this.cmpType = cmpType;
        this.target = target;
    }

    @Override
    public void collectColumns(IntConsumer sink) {
        sink.accept(col);
    }

    @Override
    public String sig() {
        return "K" + col + ',' + cmpType + ',' + target;
    }

    @Override
    public int filterSegment(ColumnSegment segment, int[] sel, int selCount, boolean fullRange,
            BitSet masked, SessionLocal session, CompareMode compareMode) {
        ColumnVector vec = segment.getColumn(col);
        int out = 0;
        if (vec instanceof ArrayVector av) {
            // 数组列已经按行存了元素的起止下标，求元素个数不必重建 ValueArray。
            for (int i = 0; i < selCount; i++) {
                int idx = fullRange ? i : sel[i];
                int len = av.length(idx);
                if (len >= 0 && VectorizedFilter.matchCompare(Integer.compare(len, target), cmpType)) {
                    sel[out++] = idx;
                }
            }
            return out;
        }
        if (fullRange) {
            for (int i = 0; i < selCount; i++) {
                Value v = vec.get(i);
                if (v != ValueNull.INSTANCE && matchCard(v)) {
                    sel[out++] = i;
                }
            }
        } else {
            for (int i = 0; i < selCount; i++) {
                int idx = sel[i];
                Value v = vec.get(idx);
                if (v != ValueNull.INSTANCE && matchCard(v)) {
                    sel[out++] = idx;
                }
            }
        }
        return out;
    }

    private boolean matchCard(Value v) {
        if (v.getValueType() != Value.ARRAY) {
            return false;
        }
        int len = ((ValueArray) v).getList().length;
        return VectorizedFilter.matchCompare(Integer.compare(len, target), cmpType);
    }

    @Override
    public boolean evalRow(Value[] row, SessionLocal session, CompareMode compareMode) {
        Value v = row[col];
        return v != ValueNull.INSTANCE && matchCard(v);
    }
}

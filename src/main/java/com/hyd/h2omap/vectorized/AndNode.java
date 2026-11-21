package com.hyd.h2omap.vectorized;

import java.util.BitSet;
import java.util.function.IntConsumer;

import org.h2.engine.SessionLocal;
import org.h2.value.CompareMode;
import org.h2.value.Value;

import com.hyd.h2omap.storage.ColumnSegment;

/**
 * AND 合取节点：按序把子节点作用于选中行集，任一子节点清空即短路。
 * 区间收集与签名由全部子节点合成（任一子节点无签名则整体无签名）。
 */
final class AndNode implements VectorizedFilter.Node {
    private final VectorizedFilter.Node[] children;

    AndNode(VectorizedFilter.Node[] children) {
        this.children = children;
    }

    @Override
    public void collectColumns(IntConsumer sink) {
        for (VectorizedFilter.Node child : children) {
            child.collectColumns(sink);
        }
    }

    @Override
    public int filterSegment(ColumnSegment segment, int[] sel, int selCount, boolean fullRange,
            BitSet masked, SessionLocal session, CompareMode compareMode) {
        int count = selCount;
        boolean full = fullRange;
        for (VectorizedFilter.Node child : children) {
            count = child.filterSegment(segment, sel, count, full, null, session, compareMode);
            if (count == 0) {
                return 0;
            }
            full = false;
        }
        return count;
    }

    @Override
    public boolean evalRow(Value[] row, SessionLocal session, CompareMode compareMode) {
        for (VectorizedFilter.Node child : children) {
            if (!child.evalRow(row, session, compareMode)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void collectBounds(Value[] lower, Value[] upper) {
        for (VectorizedFilter.Node child : children) {
            child.collectBounds(lower, upper);
        }
    }

    @Override
    public String sig() {
        StringBuilder sb = new StringBuilder("A(");
        for (VectorizedFilter.Node child : children) {
            String childSig = child.sig();
            if (childSig == null) {
                return null;
            }
            sb.append(childSig).append(';');
        }
        return sb.append(')').toString();
    }
}

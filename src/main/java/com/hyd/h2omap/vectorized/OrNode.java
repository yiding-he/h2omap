package com.hyd.h2omap.vectorized;

import java.util.BitSet;
import java.util.function.IntConsumer;

import org.h2.engine.SessionLocal;
import org.h2.value.CompareMode;
import org.h2.value.Value;

import com.hyd.h2omap.storage.ColumnSegment;

/**
 * OR 析取节点：左子节点先过滤，未命中的行交给右子节点，命中位图合并后输出。
 * 任一子节点无签名则整体无签名。
 */
final class OrNode implements VectorizedFilter.Node {
    private final VectorizedFilter.Node left;
    private final VectorizedFilter.Node right;

    OrNode(VectorizedFilter.Node left, VectorizedFilter.Node right) {
        this.left = left;
        this.right = right;
    }

    @Override
    public void collectColumns(IntConsumer sink) {
        left.collectColumns(sink);
        right.collectColumns(sink);
    }

    @Override
    public int filterSegment(ColumnSegment segment, int[] sel, int selCount, boolean fullRange,
            BitSet masked, SessionLocal session, CompareMode compareMode) {
        int[] leftSel = new int[selCount];
        if (!fullRange) {
            System.arraycopy(sel, 0, leftSel, 0, selCount);
        }
        int leftCount = left.filterSegment(segment, leftSel, selCount, fullRange, null, session, compareMode);
        if (leftCount == selCount) {
            if (fullRange) {
                for (int i = 0; i < selCount; i++) {
                    sel[i] = i;
                }
            }
            return selCount;
        }
        BitSet hit = new BitSet(segment.getRowCount());
        for (int i = 0; i < leftCount; i++) {
            hit.set(leftSel[i]);
        }
        int[] rightInput = new int[selCount - leftCount];
        int rLen = 0;
        if (fullRange) {
            for (int i = 0; i < selCount; i++) {
                if (!hit.get(i)) {
                    rightInput[rLen++] = i;
                }
            }
        } else {
            for (int i = 0; i < selCount; i++) {
                int idx = sel[i];
                if (!hit.get(idx)) {
                    rightInput[rLen++] = idx;
                }
            }
        }
        int rightCount = right.filterSegment(segment, rightInput, rLen, false, null, session, compareMode);
        for (int i = 0; i < rightCount; i++) {
            hit.set(rightInput[i]);
        }
        int out = 0;
        if (fullRange) {
            for (int i = hit.nextSetBit(0); i >= 0; i = hit.nextSetBit(i + 1)) {
                sel[out++] = i;
            }
        } else {
            for (int i = 0; i < selCount; i++) {
                int idx = sel[i];
                if (hit.get(idx)) {
                    sel[out++] = idx;
                }
            }
        }
        return out;
    }

    @Override
    public boolean evalRow(Value[] row, SessionLocal session, CompareMode compareMode) {
        return left.evalRow(row, session, compareMode)
                || right.evalRow(row, session, compareMode);
    }

    @Override
    public String sig() {
        String leftSig = left.sig();
        String rightSig = right.sig();
        return leftSig == null || rightSig == null
                ? null
                : "O(" + leftSig + "|" + rightSig + ")";
    }
}

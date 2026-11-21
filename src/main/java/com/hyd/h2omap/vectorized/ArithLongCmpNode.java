package com.hyd.h2omap.vectorized;

import java.util.BitSet;
import java.util.function.IntConsumer;

import org.h2.engine.SessionLocal;
import org.h2.expression.BinaryOperation;
import org.h2.expression.condition.Comparison;
import org.h2.value.CompareMode;
import org.h2.value.Value;
import org.h2.value.ValueNull;

import com.hyd.h2omap.storage.ColumnSegment;
import com.hyd.h2omap.vector.ColumnVector;
import com.hyd.h2omap.vector.IntVector;
import com.hyd.h2omap.vector.LongVector;
import com.hyd.h2omap.vector.NullBitmap;

/**
 * 「列与常量做算术（+ - *）后再与常量比较」的节点，供 {@code col op const} 形态下推。
 * 整数与长整数列走原始数组，逐行只做一次算术与一次比较，不装箱。
 */
final class ArithLongCmpNode implements VectorizedFilter.Node {
    private final int col;

    @Override
    public void collectColumns(IntConsumer sink) {
        sink.accept(col);
    }
    private final BinaryOperation.OpType arithOp;
    private final long arithConst;
    private final int cmpType;
    private final long target;

    ArithLongCmpNode(int col, BinaryOperation.OpType arithOp, long arithConst,
            int cmpType, long target) {
        this.col = col;
        this.arithOp = arithOp;
        this.arithConst = arithConst;
        this.cmpType = cmpType;
        this.target = target;
    }

    @Override
    public String sig() {
        return "L" + col + ',' + arithOp + ',' + arithConst + ',' + cmpType + ',' + target;
    }

    @Override
    public int filterSegment(ColumnSegment segment, int[] sel, int selCount, boolean fullRange,
            BitSet masked, SessionLocal session, CompareMode compareMode) {
        ColumnVector vec = segment.getColumn(col);
        if (vec instanceof IntVector) {
            IntVector iv = (IntVector) vec;
            int[] vals = iv.rawValues();
            NullBitmap nulls = iv.nulls();
            boolean hasNull = nulls.hasNull();
            int out = 0;
            if (fullRange) {
                if (!hasNull) {
                    for (int i = 0; i < selCount; i++) {
                        if (matchLong(evalArith(vals[i]))) {
                            sel[out++] = i;
                        }
                    }
                } else {
                    for (int i = 0; i < selCount; i++) {
                        if (!nulls.isNull(i) && matchLong(evalArith(vals[i]))) {
                            sel[out++] = i;
                        }
                    }
                }
            } else {
                for (int i = 0; i < selCount; i++) {
                    int idx = sel[i];
                    if ((!hasNull || !nulls.isNull(idx)) && matchLong(evalArith(vals[idx]))) {
                        sel[out++] = idx;
                    }
                }
            }
            return out;
        }
        if (vec instanceof LongVector) {
            LongVector lv = (LongVector) vec;
            long[] vals = lv.rawValues();
            NullBitmap nulls = lv.nulls();
            boolean hasNull = nulls.hasNull();
            int out = 0;
            if (fullRange) {
                for (int i = 0; i < selCount; i++) {
                    if ((!hasNull || !nulls.isNull(i)) && matchLong(evalArith(vals[i]))) {
                        sel[out++] = i;
                    }
                }
            } else {
                for (int i = 0; i < selCount; i++) {
                    int idx = sel[i];
                    if ((!hasNull || !nulls.isNull(idx)) && matchLong(evalArith(vals[idx]))) {
                        sel[out++] = idx;
                    }
                }
            }
            return out;
        }
        int out = 0;
        if (fullRange) {
            for (int i = 0; i < selCount; i++) {
                Value v = vec.get(i);
                if (v != ValueNull.INSTANCE && matchLong(evalArith(v.getLong()))) {
                    sel[out++] = i;
                }
            }
        } else {
            for (int i = 0; i < selCount; i++) {
                int idx = sel[i];
                Value v = vec.get(idx);
                if (v != ValueNull.INSTANCE && matchLong(evalArith(v.getLong()))) {
                    sel[out++] = idx;
                }
            }
        }
        return out;
    }

    private long evalArith(long v) {
        switch (arithOp) {
        case PLUS:
            return v + arithConst;
        case MINUS:
            return v - arithConst;
        case MULTIPLY:
            return v * arithConst;
        default:
            throw new IllegalStateException("Unexpected op: " + arithOp);
        }
    }

    private boolean matchLong(long v) {
        switch (cmpType) {
        case Comparison.EQUAL:
            return v == target;
        case Comparison.NOT_EQUAL:
            return v != target;
        case Comparison.SMALLER:
            return v < target;
        case Comparison.BIGGER:
            return v > target;
        case Comparison.SMALLER_EQUAL:
            return v <= target;
        case Comparison.BIGGER_EQUAL:
            return v >= target;
        default:
            return false;
        }
    }

    @Override
    public boolean evalRow(Value[] row, SessionLocal session, CompareMode compareMode) {
        Value v = row[col];
        return v != ValueNull.INSTANCE && matchLong(evalArith(v.getLong()));
    }
}

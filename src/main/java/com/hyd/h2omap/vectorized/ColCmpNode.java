package com.hyd.h2omap.vectorized;

import java.util.function.IntUnaryOperator;
import java.util.BitSet;
import java.util.function.IntConsumer;

import org.h2.engine.SessionLocal;
import org.h2.expression.condition.Comparison;
import org.h2.value.CompareMode;
import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.ValueTimestamp;

import com.hyd.h2omap.stats.ZoneMap;
import com.hyd.h2omap.storage.ColumnSegment;
import com.hyd.h2omap.vector.ColumnVector;
import com.hyd.h2omap.vector.DictValueVector;
import com.hyd.h2omap.vector.DictVarcharVector;
import com.hyd.h2omap.vector.DoubleVector;
import com.hyd.h2omap.vector.IntVector;
import com.hyd.h2omap.vector.LongVector;
import com.hyd.h2omap.vector.NullBitmap;
import com.hyd.h2omap.vector.TimestampVector;

/**
 * 列与常量比较节点。段级先用 zone map 判定整段不相交即整段跳过；
 * 列值落在原始数组类型（INT/BIGINT/DOUBLE/TIMESTAMP）时走原始数组比较，
 * 字典列先在字典上算出允许码位图，其余类型逐值比较。
 */
final class ColCmpNode implements VectorizedFilter.Node {
    private final int col;
    private final int cmpType;
    private final Value constant;

    ColCmpNode(int col, int cmpType, Value constant) {
        this.col = col;
        this.cmpType = cmpType;
        this.constant = constant;
    }

    @Override
    public void collectColumns(IntConsumer sink) {
        sink.accept(col);
    }

    @Override
    public void collectBounds(Value[] lower, Value[] upper) {
        switch (cmpType) {
        case Comparison.EQUAL:
            VectorizedFilter.setBound(lower, col, constant);
            VectorizedFilter.setBound(upper, col, constant);
            break;
        case Comparison.BIGGER:
        case Comparison.BIGGER_EQUAL:
            VectorizedFilter.setBound(lower, col, constant);
            break;
        case Comparison.SMALLER:
        case Comparison.SMALLER_EQUAL:
            VectorizedFilter.setBound(upper, col, constant);
            break;
        default:
            break;
        }
    }

    @Override
    public int filterSegment(ColumnSegment segment, int[] sel, int selCount, boolean fullRange,
            BitSet masked, SessionLocal session, CompareMode compareMode) {
        ZoneMap zm = segment.getZoneMap(col);
        Value low = (cmpType == Comparison.BIGGER || cmpType == Comparison.BIGGER_EQUAL
                || cmpType == Comparison.EQUAL) ? constant : null;
        Value high = (cmpType == Comparison.SMALLER || cmpType == Comparison.SMALLER_EQUAL
                || cmpType == Comparison.EQUAL) ? constant : null;
        if ((low != null || high != null) && !zm.mayIntersect(low, high)) {
            return 0;
        }
        ColumnVector vec = segment.getColumn(col);
        if (vec instanceof IntVector) {
            return filterInt((IntVector) vec, sel, selCount, fullRange);
        }
        if (vec instanceof LongVector) {
            return filterLong((LongVector) vec, sel, selCount, fullRange);
        }
        if (vec instanceof DoubleVector) {
            return filterDouble((DoubleVector) vec, sel, selCount, fullRange);
        }
        if (vec instanceof TimestampVector) {
            return filterTimestamp((TimestampVector) vec, sel, selCount, fullRange);
        }
        if (vec instanceof DictVarcharVector) {
            DictVarcharVector dv = (DictVarcharVector) vec;
            return filterDict(dv::codeAt, dv.rawDict(), dv.dictSize(), sel, selCount, fullRange,
                    session, compareMode);
        }
        if (vec instanceof DictValueVector) {
            DictValueVector dv = (DictValueVector) vec;
            return filterDict(dv::codeAt, dv.rawDict(), dv.dictSize(), sel, selCount, fullRange,
                    session, compareMode);
        }
        return filterGeneric(vec, sel, selCount, fullRange, session, compareMode);
    }

    private int filterInt(IntVector vec, int[] sel, int selCount, boolean fullRange) {
        NullBitmap nulls = vec.nulls();
        boolean hasNull = nulls.hasNull();
        long target = constant.getLong();
        int out = 0;
        if (fullRange) {
            if (!hasNull) {
                for (int i = 0; i < selCount; i++) {
                    if (matchLong(vec.longAt(i), target)) {
                        sel[out++] = i;
                    }
                }
            } else {
                long[] bits = nulls.rawBits();
                for (int i = 0; i < selCount; i++) {
                    if ((bits[i >>> 6] & (1L << i)) == 0 && matchLong(vec.longAt(i), target)) {
                        sel[out++] = i;
                    }
                }
            }
        } else {
            if (!hasNull) {
                for (int i = 0; i < selCount; i++) {
                    int idx = sel[i];
                    if (matchLong(vec.longAt(idx), target)) {
                        sel[out++] = idx;
                    }
                }
            } else {
                long[] bits = nulls.rawBits();
                for (int i = 0; i < selCount; i++) {
                    int idx = sel[i];
                    if ((bits[idx >>> 6] & (1L << idx)) == 0 && matchLong(vec.longAt(idx), target)) {
                        sel[out++] = idx;
                    }
                }
            }
        }
        return out;
    }

    private int filterLong(LongVector vec, int[] sel, int selCount, boolean fullRange) {
        NullBitmap nulls = vec.nulls();
        boolean hasNull = nulls.hasNull();
        long target = constant.getLong();
        int out = 0;
        if (fullRange) {
            for (int i = 0; i < selCount; i++) {
                if ((!hasNull || !nulls.isNull(i)) && matchLong(vec.longAt(i), target)) {
                    sel[out++] = i;
                }
            }
        } else {
            for (int i = 0; i < selCount; i++) {
                int idx = sel[i];
                if ((!hasNull || !nulls.isNull(idx)) && matchLong(vec.longAt(idx), target)) {
                    sel[out++] = idx;
                }
            }
        }
        return out;
    }

    private int filterDouble(DoubleVector vec, int[] sel, int selCount, boolean fullRange) {
        double[] vals = vec.rawValues();
        NullBitmap nulls = vec.nulls();
        boolean hasNull = nulls.hasNull();
        double target = constant.getDouble();
        int out = 0;
        if (fullRange) {
            if (!hasNull) {
                for (int i = 0; i < selCount; i++) {
                    if (matchDouble(vals[i], target)) {
                        sel[out++] = i;
                    }
                }
            } else {
                for (int i = 0; i < selCount; i++) {
                    if (!nulls.isNull(i) && matchDouble(vals[i], target)) {
                        sel[out++] = i;
                    }
                }
            }
        } else {
            if (!hasNull) {
                for (int i = 0; i < selCount; i++) {
                    int idx = sel[i];
                    if (matchDouble(vals[idx], target)) {
                        sel[out++] = idx;
                    }
                }
            } else {
                for (int i = 0; i < selCount; i++) {
                    int idx = sel[i];
                    if (!nulls.isNull(idx) && matchDouble(vals[idx], target)) {
                        sel[out++] = idx;
                    }
                }
            }
        }
        return out;
    }

    private int filterDict(IntUnaryOperator codes, Value[] dict, int dictSize,
            int[] sel, int selCount, boolean fullRange, SessionLocal session, CompareMode compareMode) {
        boolean[] allowed = new boolean[dictSize];
        boolean anyAllowed = false;
        for (int d = 0; d < dictSize; d++) {
            int cmp = dict[d].compareTo(constant, session, compareMode);
            if (VectorizedFilter.matchCompare(cmp, cmpType)) {
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
                int code = codes.applyAsInt(i);
                if (code >= 0 && allowed[code]) {
                    sel[out++] = i;
                }
            }
        } else {
            for (int i = 0; i < selCount; i++) {
                int idx = sel[i];
                int code = codes.applyAsInt(idx);
                if (code >= 0 && allowed[code]) {
                    sel[out++] = idx;
                }
            }
        }
        return out;
    }

    private int filterGeneric(ColumnVector vec, int[] sel, int selCount, boolean fullRange,
            SessionLocal session, CompareMode compareMode) {
        int out = 0;
        if (fullRange) {
            for (int i = 0; i < selCount; i++) {
                Value v = vec.get(i);
                if (v != ValueNull.INSTANCE && VectorizedFilter.matchCompare(v.compareTo(constant, session, compareMode), cmpType)) {
                    sel[out++] = i;
                }
            }
        } else {
            for (int i = 0; i < selCount; i++) {
                int idx = sel[i];
                Value v = vec.get(idx);
                if (v != ValueNull.INSTANCE && VectorizedFilter.matchCompare(v.compareTo(constant, session, compareMode), cmpType)) {
                    sel[out++] = idx;
                }
            }
        }
        return out;
    }

    private boolean matchLong(long v, long target) {
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

    private boolean matchDouble(double v, double target) {
        int cmp = Double.compare(v, target);
        return VectorizedFilter.matchCompare(cmp, cmpType);
    }

    private int filterTimestamp(TimestampVector vec, int[] sel, int selCount, boolean fullRange) {
        if (!(constant instanceof ValueTimestamp)) {
            return filterGeneric(vec, sel, selCount, fullRange, null, null);
        }
        ValueTimestamp ts = (ValueTimestamp) constant;
        long targetDate = ts.getDateValue();
        long targetNanos = ts.getTimeNanos();
        NullBitmap nulls = vec.nulls();
        boolean hasNull = nulls.hasNull();
        int out = 0;
        if (fullRange) {
            if (!hasNull) {
                for (int i = 0; i < selCount; i++) {
                    if (matchTimestamp(vec.dateAt(i), vec.nanosAt(i), targetDate, targetNanos)) {
                        sel[out++] = i;
                    }
                }
            } else {
                long[] bits = nulls.rawBits();
                for (int i = 0; i < selCount; i++) {
                    if ((bits[i >>> 6] & (1L << i)) == 0
                            && matchTimestamp(vec.dateAt(i), vec.nanosAt(i), targetDate, targetNanos)) {
                        sel[out++] = i;
                    }
                }
            }
        } else {
            if (!hasNull) {
                for (int i = 0; i < selCount; i++) {
                    int idx = sel[i];
                    if (matchTimestamp(vec.dateAt(idx), vec.nanosAt(idx), targetDate, targetNanos)) {
                        sel[out++] = idx;
                    }
                }
            } else {
                long[] bits = nulls.rawBits();
                for (int i = 0; i < selCount; i++) {
                    int idx = sel[i];
                    if ((bits[idx >>> 6] & (1L << idx)) == 0
                            && matchTimestamp(vec.dateAt(idx), vec.nanosAt(idx), targetDate, targetNanos)) {
                        sel[out++] = idx;
                    }
                }
            }
        }
        return out;
    }

    private boolean matchTimestamp(long d, long n, long targetDate, long targetNanos) {
        int cmp = Long.compare(d, targetDate);
        if (cmp == 0) {
            cmp = Long.compare(n, targetNanos);
        }
        return VectorizedFilter.matchCompare(cmp, cmpType);
    }

    @Override
    public boolean evalRow(Value[] row, SessionLocal session, CompareMode compareMode) {
        Value v = row[col];
        return v != ValueNull.INSTANCE
                && VectorizedFilter.matchCompare(v.compareTo(constant, session, compareMode), cmpType);
    }

    @Override
    public String sig() {
        return "C" + col + ',' + cmpType + ',' + VectorizedFilter.valueSig(constant);
    }
}

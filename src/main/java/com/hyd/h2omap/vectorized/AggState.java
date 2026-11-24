package com.hyd.h2omap.vectorized;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashSet;
import java.util.Set;

import org.h2.engine.SessionLocal;
import org.h2.expression.aggregate.AggregateType;
import org.h2.value.CompareMode;
import org.h2.value.Value;
import org.h2.value.ValueBigint;
import org.h2.value.ValueDouble;
import org.h2.value.ValueNull;
import org.h2.value.ValueNumeric;

import com.hyd.h2omap.storage.ColumnSegment;
import com.hyd.h2omap.vector.ColumnVector;
import com.hyd.h2omap.vector.DictValueVector;
import com.hyd.h2omap.vector.DictVarcharVector;
import com.hyd.h2omap.vector.DoubleVector;
import com.hyd.h2omap.vector.IntVector;
import com.hyd.h2omap.vector.NullBitmap;

/**
 * 单个聚合口径的累加器状态，按 {@link AggSpec} 累加段内列（原始数组路径，不逐行装箱）
 * 与 {@code extraRows} 行（Value 路径），并可与其他状态合并（段级部分聚合缓存依赖此）。
 * 合并时绝不与缓存对象按引用共享，见 {@code SegmentAggCache} 的约束。
 */
final class AggState {
    final AggSpec spec;
    long count;
    long longSum;
    double doubleSum;
    BigDecimal decimalSum;
    boolean hasValue;
    Value bestVal;
    Set<Value> distinctSet;

    AggState(AggSpec spec) {
        this.spec = spec;
    }

    void accumulateSegmentFull(ColumnSegment seg, int segCount,
            SessionLocal session, CompareMode compareMode) {
        if (spec.type == AggregateType.COUNT_ALL) {
            count += segCount;
            return;
        }
        if (spec.distinct) {
            accumulateDistinctSegment(seg, null, segCount, true);
            return;
        }
        if (spec.arithOp != null) {
            accumulateArithSegment(seg, null, segCount, true, session, compareMode);
            return;
        }
        ColumnVector vec = seg.getColumn(spec.col);
        if (spec.type == AggregateType.COUNT) {
            NullBitmap nulls = vec.nulls();
            if (!nulls.hasNull()) {
                count += segCount;
            } else {
                for (int i = 0; i < segCount; i++) {
                    if (!nulls.isNull(i)) {
                        count++;
                    }
                }
            }
            return;
        }
        if ((spec.type == AggregateType.SUM || spec.type == AggregateType.AVG)
                && vec instanceof IntVector) {
            IntVector iv = (IntVector) vec;
            NullBitmap nulls = iv.nulls();
            if (!nulls.hasNull()) {
                long s = 0;
                for (int i = 0; i < segCount; i++) {
                    s += iv.longAt(i);
                }
                longSum += s;
                count += segCount;
                hasValue = true;
            } else {
                long s = 0;
                long c = 0;
                for (int i = 0; i < segCount; i++) {
                    if (!nulls.isNull(i)) {
                        s += iv.longAt(i);
                        c++;
                    }
                }
                longSum += s;
                count += c;
                if (c > 0) {
                    hasValue = true;
                }
            }
            return;
        }
        if ((spec.type == AggregateType.SUM || spec.type == AggregateType.AVG)
                && vec instanceof DoubleVector) {
            DoubleVector dv = (DoubleVector) vec;
            double[] vals = dv.rawValues();
            NullBitmap nulls = dv.nulls();
            if (!nulls.hasNull()) {
                double s = 0;
                for (int i = 0; i < segCount; i++) {
                    s += vals[i];
                }
                doubleSum += s;
                count += segCount;
                hasValue = true;
            } else {
                double s = 0;
                long c = 0;
                for (int i = 0; i < segCount; i++) {
                    if (!nulls.isNull(i)) {
                        s += vals[i];
                        c++;
                    }
                }
                doubleSum += s;
                count += c;
                if (c > 0) {
                    hasValue = true;
                }
            }
            return;
        }
        for (int i = 0; i < segCount; i++) {
            accumulateValue(vec.get(i), session, compareMode);
        }
    }

    void accumulateSegmentSel(ColumnSegment seg, int[] sel, int selCount,
            SessionLocal session, CompareMode compareMode) {
        if (spec.type == AggregateType.COUNT_ALL) {
            count += selCount;
            return;
        }
        if (spec.distinct) {
            accumulateDistinctSegment(seg, sel, selCount, false);
            return;
        }
        if (spec.arithOp != null) {
            accumulateArithSegment(seg, sel, selCount, false, session, compareMode);
            return;
        }
        ColumnVector vec = seg.getColumn(spec.col);
        if (spec.type == AggregateType.COUNT) {
            NullBitmap nulls = vec.nulls();
            if (!nulls.hasNull()) {
                count += selCount;
            } else {
                for (int i = 0; i < selCount; i++) {
                    if (!nulls.isNull(sel[i])) {
                        count++;
                    }
                }
            }
            return;
        }
        if ((spec.type == AggregateType.SUM || spec.type == AggregateType.AVG)
                && vec instanceof IntVector) {
            IntVector iv = (IntVector) vec;
            NullBitmap nulls = iv.nulls();
            if (!nulls.hasNull()) {
                long s = 0;
                for (int i = 0; i < selCount; i++) {
                    s += iv.longAt(sel[i]);
                }
                longSum += s;
                count += selCount;
                hasValue = true;
            } else {
                long s = 0;
                long c = 0;
                for (int i = 0; i < selCount; i++) {
                    int idx = sel[i];
                    if (!nulls.isNull(idx)) {
                        s += iv.longAt(idx);
                        c++;
                    }
                }
                longSum += s;
                count += c;
                if (c > 0) {
                    hasValue = true;
                }
            }
            return;
        }
        if ((spec.type == AggregateType.SUM || spec.type == AggregateType.AVG)
                && vec instanceof DoubleVector) {
            DoubleVector dv = (DoubleVector) vec;
            double[] vals = dv.rawValues();
            NullBitmap nulls = dv.nulls();
            if (!nulls.hasNull()) {
                double s = 0;
                for (int i = 0; i < selCount; i++) {
                    s += vals[sel[i]];
                }
                doubleSum += s;
                count += selCount;
                hasValue = true;
            } else {
                double s = 0;
                long c = 0;
                for (int i = 0; i < selCount; i++) {
                    int idx = sel[i];
                    if (!nulls.isNull(idx)) {
                        s += vals[idx];
                        c++;
                    }
                }
                doubleSum += s;
                count += c;
                if (c > 0) {
                    hasValue = true;
                }
            }
            return;
        }
        for (int i = 0; i < selCount; i++) {
            accumulateValue(vec.get(sel[i]), session, compareMode);
        }
    }

    private void accumulateDistinctSegment(ColumnSegment seg, int[] sel, int selCount, boolean full) {
        if (distinctSet == null) {
            distinctSet = new HashSet<>();
        }
        ColumnVector vec = seg.getColumn(spec.col);
        if (vec instanceof DictVarcharVector) {
            DictVarcharVector dv = (DictVarcharVector) vec;
            int dictSize = dv.dictSize();
            boolean[] seen = new boolean[dictSize];
            if (full) {
                for (int i = 0; i < selCount; i++) {
                    int c = dv.codeAt(i);
                    if (c >= 0) {
                        seen[c] = true;
                    }
                }
            } else {
                for (int i = 0; i < selCount; i++) {
                    int c = dv.codeAt(sel[i]);
                    if (c >= 0) {
                        seen[c] = true;
                    }
                }
            }
            Value[] dict = dv.rawDict();
            for (int d = 0; d < dictSize; d++) {
                if (seen[d]) {
                    distinctSet.add(dict[d]);
                }
            }
            return;
        }
        if (vec instanceof DictValueVector) {
            DictValueVector dv = (DictValueVector) vec;
            int dictSize = dv.dictSize();
            boolean[] seen = new boolean[dictSize];
            if (full) {
                for (int i = 0; i < selCount; i++) {
                    int c = dv.codeAt(i);
                    if (c >= 0) {
                        seen[c] = true;
                    }
                }
            } else {
                for (int i = 0; i < selCount; i++) {
                    int c = dv.codeAt(sel[i]);
                    if (c >= 0) {
                        seen[c] = true;
                    }
                }
            }
            Value[] dict = dv.rawDict();
            for (int d = 0; d < dictSize; d++) {
                if (seen[d]) {
                    distinctSet.add(dict[d]);
                }
            }
            return;
        }
        NullBitmap nulls = vec.nulls();
        boolean hasNull = nulls.hasNull();
        if (full) {
            for (int i = 0; i < selCount; i++) {
                if (!hasNull || !nulls.isNull(i)) {
                    distinctSet.add(vec.get(i));
                }
            }
        } else {
            for (int i = 0; i < selCount; i++) {
                int idx = sel[i];
                if (!hasNull || !nulls.isNull(idx)) {
                    distinctSet.add(vec.get(idx));
                }
            }
        }
    }

    private void accumulateArithSegment(ColumnSegment seg, int[] sel, int selCount, boolean full,
            SessionLocal session, CompareMode compareMode) {
        ColumnVector leftVec = seg.getColumn(spec.col);
        ColumnVector rightVec = spec.rightCol >= 0 ? seg.getColumn(spec.rightCol) : null;
        int rt = spec.resultType.getValueType();
        if ((spec.type == AggregateType.SUM || spec.type == AggregateType.AVG)
                && rightVec == null && rt == Value.BIGINT && leftVec instanceof IntVector) {
            IntVector iv = (IntVector) leftVec;
            NullBitmap nulls = iv.nulls();
            boolean hasNull = nulls.hasNull();
            long c = spec.arithConst.getLong();
            long s = 0;
            long cnt = 0;
            for (int i = 0; i < selCount; i++) {
                int idx = full ? i : sel[i];
                if (!hasNull || !nulls.isNull(idx)) {
                    s += evalLongOp(iv.longAt(idx), c);
                    cnt++;
                }
            }
            longSum += s;
            count += cnt;
            if (cnt > 0) {
                hasValue = true;
            }
            return;
        }
        if ((spec.type == AggregateType.SUM || spec.type == AggregateType.AVG)
                && rightVec == null && rt == Value.DOUBLE && leftVec instanceof DoubleVector) {
            DoubleVector dv = (DoubleVector) leftVec;
            double[] vals = dv.rawValues();
            NullBitmap nulls = dv.nulls();
            boolean hasNull = nulls.hasNull();
            double c = spec.arithConst.getDouble();
            double s = 0;
            long cnt = 0;
            for (int i = 0; i < selCount; i++) {
                int idx = full ? i : sel[i];
                if (!hasNull || !nulls.isNull(idx)) {
                    s += evalDoubleOp(vals[idx], c);
                    cnt++;
                }
            }
            doubleSum += s;
            count += cnt;
            if (cnt > 0) {
                hasValue = true;
            }
            return;
        }
        for (int i = 0; i < selCount; i++) {
            int idx = full ? i : sel[i];
            Value lv = leftVec.get(idx);
            if (lv == ValueNull.INSTANCE) {
                continue;
            }
            Value rv = rightVec != null ? rightVec.get(idx) : spec.arithConst;
            if (rv == ValueNull.INSTANCE) {
                continue;
            }
            accumulateValue(evalArithValue(lv, rv), session, compareMode);
        }
    }

    private long evalLongOp(long colVal, long constVal) {
        long l = spec.colOnLeft ? colVal : constVal;
        long r = spec.colOnLeft ? constVal : colVal;
        switch (spec.arithOp) {
        case PLUS:
            return l + r;
        case MINUS:
            return l - r;
        case MULTIPLY:
            return l * r;
        default:
            return 0L;
        }
    }

    private double evalDoubleOp(double colVal, double constVal) {
        double l = spec.colOnLeft ? colVal : constVal;
        double r = spec.colOnLeft ? constVal : colVal;
        switch (spec.arithOp) {
        case PLUS:
            return l + r;
        case MINUS:
            return l - r;
        case MULTIPLY:
            return l * r;
        default:
            return 0.0d;
        }
    }

    private Value evalArithValue(Value colVal, Value otherVal) {
        Value l = spec.colOnLeft ? colVal : otherVal;
        Value r = spec.colOnLeft ? otherVal : colVal;
        int rt = spec.resultType.getValueType();
        if (rt == Value.DOUBLE) {
            return ValueDouble.get(evalDoubleOpRaw(l.getDouble(), r.getDouble()));
        }
        if (rt == Value.NUMERIC) {
            BigDecimal ld = l.getBigDecimal();
            BigDecimal rd = r.getBigDecimal();
            switch (spec.arithOp) {
            case PLUS:
                return ValueNumeric.get(ld.add(rd));
            case MINUS:
                return ValueNumeric.get(ld.subtract(rd));
            case MULTIPLY:
                return ValueNumeric.get(ld.multiply(rd));
            default:
                return ValueNull.INSTANCE;
            }
        }
        long lv = l.getLong();
        long rv = r.getLong();
        switch (spec.arithOp) {
        case PLUS:
            return ValueBigint.get(lv + rv);
        case MINUS:
            return ValueBigint.get(lv - rv);
        case MULTIPLY:
            return ValueBigint.get(lv * rv);
        default:
            return ValueNull.INSTANCE;
        }
    }

    private double evalDoubleOpRaw(double l, double r) {
        switch (spec.arithOp) {
        case PLUS:
            return l + r;
        case MINUS:
            return l - r;
        case MULTIPLY:
            return l * r;
        default:
            return 0.0d;
        }
    }

    void accumulateAt(ColumnSegment seg, int idx, SessionLocal session, CompareMode compareMode) {
        if (spec.type == AggregateType.COUNT_ALL) {
            count++;
            return;
        }
        if (spec.arithOp != null) {
            Value lv = seg.getColumn(spec.col).get(idx);
            if (lv == ValueNull.INSTANCE) {
                return;
            }
            Value rv = spec.rightCol >= 0 ? seg.getColumn(spec.rightCol).get(idx) : spec.arithConst;
            if (rv == ValueNull.INSTANCE) {
                return;
            }
            accumulateValue(evalArithValue(lv, rv), session, compareMode);
            return;
        }
        accumulateValue(seg.getColumn(spec.col).get(idx), session, compareMode);
    }

    void accumulateRow(Value[] row, SessionLocal session, CompareMode compareMode) {
        if (spec.type == AggregateType.COUNT_ALL) {
            count++;
            return;
        }
        if (spec.arithOp != null) {
            Value lv = row[spec.col];
            if (lv == ValueNull.INSTANCE) {
                return;
            }
            Value rv = spec.rightCol >= 0 ? row[spec.rightCol] : spec.arithConst;
            if (rv == ValueNull.INSTANCE) {
                return;
            }
            accumulateValue(evalArithValue(lv, rv), session, compareMode);
            return;
        }
        accumulateValue(row[spec.col], session, compareMode);
    }

    private void accumulateValue(Value v, SessionLocal session, CompareMode compareMode) {
        if (v == ValueNull.INSTANCE) {
            return;
        }
        if (spec.distinct) {
            if (distinctSet == null) {
                distinctSet = new HashSet<>();
            }
            distinctSet.add(v);
            return;
        }
        count++;
        hasValue = true;
        switch (spec.type) {
        case COUNT:
            break;
        case SUM:
        case AVG:
            int rt = spec.resultType.getValueType();
            if (rt == Value.BIGINT) {
                longSum += v.getLong();
            } else if (rt == Value.DOUBLE) {
                doubleSum += v.getDouble();
            } else {
                BigDecimal d = v.getBigDecimal();
                decimalSum = decimalSum == null ? d : decimalSum.add(d);
            }
            break;
        case MIN:
            if (bestVal == null || v.compareTo(bestVal, session, compareMode) < 0) {
                bestVal = v;
            }
            break;
        case MAX:
            if (bestVal == null || v.compareTo(bestVal, session, compareMode) > 0) {
                bestVal = v;
            }
            break;
        default:
            break;
        }
    }

    void merge(AggState other, SessionLocal session, CompareMode compareMode) {
        if (spec.distinct) {
            if (other.distinctSet != null && !other.distinctSet.isEmpty()) {
                if (distinctSet == null) {
                    distinctSet = new HashSet<>();
                }
                distinctSet.addAll(other.distinctSet);
            }
            return;
        }
        count += other.count;
        if (!other.hasValue) {
            return;
        }
        hasValue = true;
        switch (spec.type) {
        case SUM:
        case AVG:
            longSum += other.longSum;
            doubleSum += other.doubleSum;
            if (other.decimalSum != null) {
                decimalSum = decimalSum == null ? other.decimalSum : decimalSum.add(other.decimalSum);
            }
            break;
        case MIN:
            if (bestVal == null || (other.bestVal != null && other.bestVal.compareTo(bestVal, session, compareMode) < 0)) {
                bestVal = other.bestVal;
            }
            break;
        case MAX:
            if (bestVal == null || (other.bestVal != null && other.bestVal.compareTo(bestVal, session, compareMode) > 0)) {
                bestVal = other.bestVal;
            }
            break;
        default:
            break;
        }
    }

    Value result(SessionLocal session) {
        if (spec.distinct) {
            return ValueBigint.get(distinctSet != null ? distinctSet.size() : 0L);
        }
        if (spec.type == AggregateType.COUNT_ALL || spec.type == AggregateType.COUNT) {
            return ValueBigint.get(count);
        }
        if (!hasValue) {
            return ValueNull.INSTANCE;
        }
        int rt = spec.resultType.getValueType();
        switch (spec.type) {
        case SUM:
            if (rt == Value.BIGINT) {
                return ValueBigint.get(longSum);
            }
            if (rt == Value.DOUBLE) {
                return ValueDouble.get(doubleSum);
            }
            return ValueNumeric.get(decimalSum != null ? decimalSum : BigDecimal.valueOf(longSum));
        case AVG:
            if (rt == Value.DOUBLE) {
                return ValueDouble.get((doubleSum + longSum) / count);
            }
            BigDecimal sum = decimalSum != null ? decimalSum : BigDecimal.valueOf(longSum);
            return ValueNumeric.get(sum.divide(
                    BigDecimal.valueOf(count), spec.resultType.getScale(), RoundingMode.HALF_DOWN));
        case MIN:
        case MAX:
            return bestVal;
        default:
            return ValueNull.INSTANCE;
        }
    }
}

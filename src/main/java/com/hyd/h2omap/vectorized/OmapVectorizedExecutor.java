/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.vectorized;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.aggregate.Aggregate;
import org.h2.expression.aggregate.AggregateType;
import org.h2.expression.aggregate.H2AggregateAccess;
import org.h2.expression.condition.Comparison;
import org.h2.expression.condition.ConditionAndOr;
import org.h2.expression.condition.ConditionAndOrN;
import org.h2.expression.condition.H2ConditionAccess;
import org.h2.result.LocalResult;
import org.h2.table.TableFilter;
import org.h2.value.CompareMode;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueBigint;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueDouble;
import org.h2.value.ValueNull;
import org.h2.value.ValueNumeric;

import com.hyd.h2omap.storage.ColumnSegment;
import com.hyd.h2omap.storage.ColumnarStore;
import com.hyd.h2omap.table.OmapTable;
import com.hyd.h2omap.vector.ColumnVector;
import com.hyd.h2omap.vector.DictValueVector;
import com.hyd.h2omap.vector.DictVarcharVector;
import com.hyd.h2omap.vector.DoubleVector;
import com.hyd.h2omap.vector.IntVector;
import com.hyd.h2omap.vector.LongVector;
import com.hyd.h2omap.vector.NullBitmap;

/**
 * 单表过滤与聚合的向量化执行器。
 *
 * <p>直接读取 {@link ColumnarStore.VectorizedSnapshot} 中的不可变列段原始数组，
 * 在完全无锁的情况下完成 Selection Vector 过滤与无装箱聚合。遇到不支持的表达式时返回
 * {@code false}，交由 H2 原 {@code queryGroup} 无缝兜底。
 */
public final class OmapVectorizedExecutor {

    private OmapVectorizedExecutor() {
    }

    public static boolean tryExecuteGroup(
            SessionLocal session,
            TableFilter tableFilter,
            Expression condition,
            Expression having,
            int havingIndex,
            List<Expression> expressions,
            int resultColumnCount,
            int[] groupIndex,
            boolean[] groupByExpression,
            int[] groupByCopies,
            LocalResult result) {
        if (!(tableFilter.getTable() instanceof OmapTable)) {
            return false;
        }

        if (qualifyIndexCheck(expressions, resultColumnCount, havingIndex)) {
            return false;
        }

        VectorizedFilter filter = null;
        if (condition != null) {
            filter = VectorizedFilter.compile(condition, tableFilter, session);
            if (filter == null) {
                return false;
            }
        }

        int groupCount = groupIndex != null ? groupIndex.length : 0;
        int[] groupKeyCols = new int[groupCount];
        int[] groupResultIdxs = new int[groupCount];
        for (int g = 0; g < groupCount; g++) {
            int rIdx = groupIndex[g];
            groupResultIdxs[g] = rIdx;
            Expression gExpr = expressions.get(rIdx).getNonAliasExpression();
            if (!(gExpr instanceof ExpressionColumn)) {
                return false;
            }
            ExpressionColumn ec = (ExpressionColumn) gExpr;
            if (ec.getTableFilter() != tableFilter) {
                return false;
            }
            groupKeyCols[g] = ec.getColumn().getColumnId();
        }

        List<AggSpec> aggList = new ArrayList<>(resultColumnCount);
        for (int i = 0; i < resultColumnCount; i++) {
            if (groupByExpression != null && groupByExpression[i]) {
                continue;
            }
            if (groupByCopies != null && groupByCopies[i] >= 0) {
                continue;
            }
            Expression expr = expressions.get(i).getNonAliasExpression();
            if (!(expr instanceof Aggregate)) {
                return false;
            }
            Aggregate agg = (Aggregate) expr;
            if (agg.isDistinct()) {
                return false;
            }
            AggSpec spec = parseAggregate(agg, tableFilter, i);
            if (spec == null) {
                return false;
            }
            aggList.add(spec);
        }

        Expression havingExpr = having != null ? having
                : (havingIndex >= 0 && havingIndex < expressions.size() ? expressions.get(havingIndex) : null);
        HavingPredicate havingPred = null;
        if (havingExpr != null) {
            havingPred = compileHaving(havingExpr, tableFilter, groupKeyCols, aggList, session);
            if (havingPred == null) {
                return false;
            }
        }

        AggSpec[] aggSpecs = aggList.toArray(new AggSpec[0]);
        int aggCount = aggSpecs.length;

        OmapTable table = (OmapTable) tableFilter.getTable();
        ColumnarStore store = table.getStore();
        CompareMode compareMode = table.getCompareMode();
        ColumnarStore.VectorizedSnapshot snap = store.takeVectorizedSnapshot();

        if (groupCount == 0) {
            executeGlobal(snap, filter, havingPred, aggSpecs, aggCount, resultColumnCount,
                    result, session, compareMode);
        } else if (groupCount == 1) {
            executeSingleGroup(snap, filter, havingPred, groupKeyCols[0], groupResultIdxs[0],
                    groupByCopies, aggSpecs, aggCount, resultColumnCount, result, session, compareMode);
        } else {
            executeMultiGroup(snap, filter, havingPred, groupKeyCols, groupResultIdxs,
                    groupByCopies, aggSpecs, aggCount, resultColumnCount, result, session, compareMode);
        }
        return true;
    }

    private static boolean qualifyIndexCheck(List<Expression> expressions, int resultColumnCount, int havingIndex) {
        int expectedExtra = havingIndex >= 0 ? 1 : 0;
        return expressions.size() > resultColumnCount + expectedExtra;
    }

    private static AggSpec parseAggregate(Aggregate agg, TableFilter filter, int outIdx) {
        AggregateType type = agg.getAggregateType();
        Expression[] args = H2AggregateAccess.getArgs(agg);
        switch (type) {
        case COUNT_ALL:
            return new AggSpec(type, -1, outIdx, agg.getType());
        case AVG: {
            int rt = agg.getType().getValueType();
            if (rt != Value.DOUBLE && rt != Value.NUMERIC) {
                return null;
            }
            // fall through
        }
        case COUNT:
        case SUM:
        case MIN:
        case MAX:
            if (args.length != 1) {
                return null;
            }
            Expression arg = args[0].getNonAliasExpression();
            if (!(arg instanceof ExpressionColumn)) {
                return null;
            }
            ExpressionColumn ec = (ExpressionColumn) arg;
            if (ec.getTableFilter() != filter) {
                return null;
            }
            return new AggSpec(type, ec.getColumn().getColumnId(), outIdx, agg.getType());
        default:
            return null;
        }
    }

    private static void executeGlobal(
            ColumnarStore.VectorizedSnapshot snap,
            VectorizedFilter filter,
            HavingPredicate havingPred,
            AggSpec[] specs,
            int aggCount,
            int resultColumnCount,
            LocalResult result,
            SessionLocal session,
            CompareMode compareMode) {
        AggState[] states = new AggState[aggCount];
        for (int a = 0; a < aggCount; a++) {
            states[a] = new AggState(specs[a]);
        }

        int[] sel = null;
        for (ColumnSegment seg : snap.segments) {
            int segCount = seg.getRowCount();
            if (segCount == 0) {
                continue;
            }
            BitSet masked = snap.maskedBySegment.get(seg.getSegmentId());
            if (filter == null && (masked == null || masked.isEmpty())) {
                for (int a = 0; a < aggCount; a++) {
                    states[a].accumulateSegmentFull(seg, segCount, session, compareMode);
                }
                continue;
            }
            if (sel == null || sel.length < segCount) {
                sel = new int[segCount];
            }
            int selCount;
            if (filter != null) {
                selCount = filter.filterSegment(seg, sel, masked, session, compareMode);
            } else {
                selCount = 0;
                for (int i = 0; i < segCount; i++) {
                    if (!masked.get(i)) {
                        sel[selCount++] = i;
                    }
                }
            }
            if (selCount == 0) {
                continue;
            }
            for (int a = 0; a < aggCount; a++) {
                states[a].accumulateSegmentSel(seg, sel, selCount, session, compareMode);
            }
        }

        for (Value[] row : snap.extraRows) {
            if (filter == null || filter.evalRow(row, session, compareMode)) {
                for (int a = 0; a < aggCount; a++) {
                    states[a].accumulateRow(row, session, compareMode);
                }
            }
        }

        Value[] aggVals = new Value[aggCount];
        for (int a = 0; a < aggCount; a++) {
            aggVals[a] = states[a].result(session);
        }
        if (havingPred != null && !havingPred.eval(null, aggVals, session, compareMode)) {
            return;
        }
        Value[] out = new Value[resultColumnCount];
        for (int a = 0; a < aggCount; a++) {
            int outIdx = specs[a].outIdx;
            if (outIdx >= 0 && outIdx < resultColumnCount) {
                out[outIdx] = aggVals[a];
            }
        }
        result.addRow(out);
    }

    private static void executeSingleGroup(
            ColumnarStore.VectorizedSnapshot snap,
            VectorizedFilter filter,
            HavingPredicate havingPred,
            int groupKeyCol,
            int groupResultIdx,
            int[] groupByCopies,
            AggSpec[] specs,
            int aggCount,
            int resultColumnCount,
            LocalResult result,
            SessionLocal session,
            CompareMode compareMode) {
        Map<Value, AggState[]> groups = new HashMap<>();
        int[] sel = null;

        for (ColumnSegment seg : snap.segments) {
            int segCount = seg.getRowCount();
            if (segCount == 0) {
                continue;
            }
            BitSet masked = snap.maskedBySegment.get(seg.getSegmentId());
            boolean full = filter == null && (masked == null || masked.isEmpty());
            int selCount;
            if (full) {
                selCount = segCount;
            } else {
                if (sel == null || sel.length < segCount) {
                    sel = new int[segCount];
                }
                if (filter != null) {
                    selCount = filter.filterSegment(seg, sel, masked, session, compareMode);
                } else {
                    selCount = 0;
                    for (int i = 0; i < segCount; i++) {
                        if (!masked.get(i)) {
                            sel[selCount++] = i;
                        }
                    }
                }
            }
            if (selCount == 0) {
                continue;
            }

            ColumnVector keyVec = seg.getColumn(groupKeyCol);
            if (keyVec instanceof DictVarcharVector) {
                DictVarcharVector dv = (DictVarcharVector) keyVec;
                groupByDict(seg, dv.rawCodes(), dv.rawDict(), dv.dictSize(),
                        sel, selCount, full, specs, aggCount, groups, session, compareMode);
            } else if (keyVec instanceof DictValueVector) {
                DictValueVector dv = (DictValueVector) keyVec;
                groupByDict(seg, dv.rawCodes(), dv.rawDict(), dv.dictSize(),
                        sel, selCount, full, specs, aggCount, groups, session, compareMode);
            } else {
                for (int i = 0; i < selCount; i++) {
                    int idx = full ? i : sel[i];
                    Value key = keyVec.get(idx);
                    AggState[] states = groups.get(key);
                    if (states == null) {
                        states = newStates(specs, aggCount);
                        groups.put(key, states);
                    }
                    for (int a = 0; a < aggCount; a++) {
                        states[a].accumulateAt(seg, idx, session, compareMode);
                    }
                }
            }
        }

        for (Value[] row : snap.extraRows) {
            if (filter == null || filter.evalRow(row, session, compareMode)) {
                Value key = row[groupKeyCol];
                AggState[] states = groups.get(key);
                if (states == null) {
                    states = newStates(specs, aggCount);
                    groups.put(key, states);
                }
                for (int a = 0; a < aggCount; a++) {
                    states[a].accumulateRow(row, session, compareMode);
                }
            }
        }

        Value[] singleKeyHolder = havingPred != null ? new Value[1] : null;
        Value[] aggVals = new Value[aggCount];
        for (Map.Entry<Value, AggState[]> entry : groups.entrySet()) {
            AggState[] states = entry.getValue();
            for (int a = 0; a < aggCount; a++) {
                aggVals[a] = states[a].result(session);
            }
            if (havingPred != null) {
                singleKeyHolder[0] = entry.getKey();
                if (!havingPred.eval(singleKeyHolder, aggVals, session, compareMode)) {
                    continue;
                }
            }
            Value[] out = new Value[resultColumnCount];
            if (groupResultIdx >= 0 && groupResultIdx < resultColumnCount) {
                out[groupResultIdx] = entry.getKey();
            }
            if (groupByCopies != null) {
                for (int i = 0; i < resultColumnCount && i < groupByCopies.length; i++) {
                    if (groupByCopies[i] >= 0) {
                        out[i] = out[groupByCopies[i]];
                    }
                }
            }
            for (int a = 0; a < aggCount; a++) {
                int outIdx = specs[a].outIdx;
                if (outIdx >= 0 && outIdx < resultColumnCount) {
                    out[outIdx] = aggVals[a];
                }
            }
            result.addRow(out);
        }
    }

    private static void executeMultiGroup(
            ColumnarStore.VectorizedSnapshot snap,
            VectorizedFilter filter,
            HavingPredicate havingPred,
            int[] groupKeyCols,
            int[] groupResultIdxs,
            int[] groupByCopies,
            AggSpec[] specs,
            int aggCount,
            int resultColumnCount,
            LocalResult result,
            SessionLocal session,
            CompareMode compareMode) {
        int groupCount = groupKeyCols.length;
        Map<MultiGroupKey, AggState[]> groups = new HashMap<>();
        int[] sel = null;
        ColumnVector[] keyVecs = new ColumnVector[groupCount];

        for (ColumnSegment seg : snap.segments) {
            int segCount = seg.getRowCount();
            if (segCount == 0) {
                continue;
            }
            BitSet masked = snap.maskedBySegment.get(seg.getSegmentId());
            boolean full = filter == null && (masked == null || masked.isEmpty());
            int selCount;
            if (full) {
                selCount = segCount;
            } else {
                if (sel == null || sel.length < segCount) {
                    sel = new int[segCount];
                }
                if (filter != null) {
                    selCount = filter.filterSegment(seg, sel, masked, session, compareMode);
                } else {
                    selCount = 0;
                    for (int i = 0; i < segCount; i++) {
                        if (!masked.get(i)) {
                            sel[selCount++] = i;
                        }
                    }
                }
            }
            if (selCount == 0) {
                continue;
            }

            for (int g = 0; g < groupCount; g++) {
                keyVecs[g] = seg.getColumn(groupKeyCols[g]);
            }
            MultiGroupKey probe = new MultiGroupKey(new Value[groupCount]);
            for (int i = 0; i < selCount; i++) {
                int idx = full ? i : sel[i];
                for (int g = 0; g < groupCount; g++) {
                    probe.values[g] = keyVecs[g].get(idx);
                }
                AggState[] states = groups.get(probe);
                if (states == null) {
                    states = newStates(specs, aggCount);
                    groups.put(new MultiGroupKey(probe.values.clone()), states);
                }
                for (int a = 0; a < aggCount; a++) {
                    states[a].accumulateAt(seg, idx, session, compareMode);
                }
            }
        }

        MultiGroupKey probe = new MultiGroupKey(new Value[groupCount]);
        for (Value[] row : snap.extraRows) {
            if (filter == null || filter.evalRow(row, session, compareMode)) {
                for (int g = 0; g < groupCount; g++) {
                    probe.values[g] = row[groupKeyCols[g]];
                }
                AggState[] states = groups.get(probe);
                if (states == null) {
                    states = newStates(specs, aggCount);
                    groups.put(new MultiGroupKey(probe.values.clone()), states);
                }
                for (int a = 0; a < aggCount; a++) {
                    states[a].accumulateRow(row, session, compareMode);
                }
            }
        }

        Value[] aggVals = new Value[aggCount];
        for (Map.Entry<MultiGroupKey, AggState[]> entry : groups.entrySet()) {
            Value[] keys = entry.getKey().values;
            AggState[] states = entry.getValue();
            for (int a = 0; a < aggCount; a++) {
                aggVals[a] = states[a].result(session);
            }
            if (havingPred != null && !havingPred.eval(keys, aggVals, session, compareMode)) {
                continue;
            }
            Value[] out = new Value[resultColumnCount];
            for (int g = 0; g < groupCount; g++) {
                int rIdx = groupResultIdxs[g];
                if (rIdx >= 0 && rIdx < resultColumnCount) {
                    out[rIdx] = keys[g];
                }
            }
            if (groupByCopies != null) {
                for (int i = 0; i < resultColumnCount && i < groupByCopies.length; i++) {
                    if (groupByCopies[i] >= 0) {
                        out[i] = out[groupByCopies[i]];
                    }
                }
            }
            for (int a = 0; a < aggCount; a++) {
                int outIdx = specs[a].outIdx;
                if (outIdx >= 0 && outIdx < resultColumnCount) {
                    out[outIdx] = aggVals[a];
                }
            }
            result.addRow(out);
        }
    }

    private static final class MultiGroupKey {
        final Value[] values;

        MultiGroupKey(Value[] values) {
            this.values = values;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof MultiGroupKey)) {
                return false;
            }
            return Arrays.equals(values, ((MultiGroupKey) o).values);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(values);
        }
    }

    private static void groupByDict(
            ColumnSegment seg,
            int[] codes,
            Value[] dict,
            int dictSize,
            int[] sel,
            int selCount,
            boolean full,
            AggSpec[] specs,
            int aggCount,
            Map<Value, AggState[]> globalGroups,
            SessionLocal session,
            CompareMode compareMode) {
        // 槽位 dictSize 留给 NULL（code == -1）
        AggState[][] local = new AggState[dictSize + 1][];
        for (int i = 0; i < selCount; i++) {
            int idx = full ? i : sel[i];
            int code = codes[idx];
            int slot = code >= 0 ? code : dictSize;
            AggState[] states = local[slot];
            if (states == null) {
                states = newStates(specs, aggCount);
                local[slot] = states;
            }
            for (int a = 0; a < aggCount; a++) {
                states[a].accumulateAt(seg, idx, session, compareMode);
            }
        }
        for (int slot = 0; slot <= dictSize; slot++) {
            AggState[] localStates = local[slot];
            if (localStates == null) {
                continue;
            }
            Value key = slot < dictSize ? dict[slot] : ValueNull.INSTANCE;
            AggState[] target = globalGroups.get(key);
            if (target == null) {
                globalGroups.put(key, localStates);
            } else {
                for (int a = 0; a < aggCount; a++) {
                    target[a].merge(localStates[a], session, compareMode);
                }
            }
        }
    }

    private static AggState[] newStates(AggSpec[] specs, int aggCount) {
        AggState[] states = new AggState[aggCount];
        for (int a = 0; a < aggCount; a++) {
            states[a] = new AggState(specs[a]);
        }
        return states;
    }

    private static final class AggSpec {
        final AggregateType type;
        final int col;
        final int outIdx;
        final TypeInfo resultType;

        AggSpec(AggregateType type, int col, int outIdx, TypeInfo resultType) {
            this.type = type;
            this.col = col;
            this.outIdx = outIdx;
            this.resultType = resultType;
        }
    }

    private static final class AggState {
        final AggSpec spec;
        long count;
        long longSum;
        double doubleSum;
        BigDecimal decimalSum;
        boolean hasValue;
        Value bestVal;

        AggState(AggSpec spec) {
            this.spec = spec;
        }

        void accumulateSegmentFull(ColumnSegment seg, int segCount,
                SessionLocal session, CompareMode compareMode) {
            if (spec.type == AggregateType.COUNT_ALL) {
                count += segCount;
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
                int[] vals = iv.rawValues();
                NullBitmap nulls = iv.nulls();
                if (!nulls.hasNull()) {
                    long s = 0;
                    for (int i = 0; i < segCount; i++) {
                        s += vals[i];
                    }
                    longSum += s;
                    count += segCount;
                    hasValue = true;
                } else {
                    long s = 0;
                    long c = 0;
                    for (int i = 0; i < segCount; i++) {
                        if (!nulls.isNull(i)) {
                            s += vals[i];
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
                int[] vals = iv.rawValues();
                NullBitmap nulls = iv.nulls();
                if (!nulls.hasNull()) {
                    long s = 0;
                    for (int i = 0; i < selCount; i++) {
                        s += vals[sel[i]];
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
                            s += vals[idx];
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

        void accumulateAt(ColumnSegment seg, int idx, SessionLocal session, CompareMode compareMode) {
            if (spec.type == AggregateType.COUNT_ALL) {
                count++;
                return;
            }
            accumulateValue(seg.getColumn(spec.col).get(idx), session, compareMode);
        }

        void accumulateRow(Value[] row, SessionLocal session, CompareMode compareMode) {
            if (spec.type == AggregateType.COUNT_ALL) {
                count++;
                return;
            }
            accumulateValue(row[spec.col], session, compareMode);
        }

        private void accumulateValue(Value v, SessionLocal session, CompareMode compareMode) {
            if (v == ValueNull.INSTANCE) {
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

    // ---- HAVING 谓词编译与求值 ----

    private static HavingPredicate compileHaving(
            Expression expr,
            TableFilter tableFilter,
            int[] groupKeyCols,
            List<AggSpec> aggList,
            SessionLocal session) {
        Expression e = expr.getNonAliasExpression();
        if (e instanceof ConditionAndOr) {
            ConditionAndOr cao = (ConditionAndOr) e;
            int op = H2ConditionAccess.getAndOrType(cao);
            HavingPredicate left = compileHaving(cao.getSubexpression(0), tableFilter, groupKeyCols, aggList, session);
            HavingPredicate right = compileHaving(cao.getSubexpression(1), tableFilter, groupKeyCols, aggList, session);
            if (left == null || right == null) {
                return null;
            }
            return new HavingAndOr(op == ConditionAndOr.AND, new HavingPredicate[] { left, right });
        }
        if (e instanceof ConditionAndOrN) {
            ConditionAndOrN caon = (ConditionAndOrN) e;
            int op = H2ConditionAccess.getAndOrType(caon);
            int subCount = caon.getSubexpressionCount();
            HavingPredicate[] compiled = new HavingPredicate[subCount];
            for (int i = 0; i < subCount; i++) {
                compiled[i] = compileHaving(caon.getSubexpression(i), tableFilter, groupKeyCols, aggList, session);
                if (compiled[i] == null) {
                    return null;
                }
            }
            return new HavingAndOr(op == ConditionAndOr.AND, compiled);
        }
        if (e instanceof Comparison) {
            Comparison cmp = (Comparison) e;
            int cmpType = cmp.getCompareType();
            if (cmpType != Comparison.EQUAL
                    && cmpType != Comparison.NOT_EQUAL
                    && cmpType != Comparison.BIGGER
                    && cmpType != Comparison.BIGGER_EQUAL
                    && cmpType != Comparison.SMALLER
                    && cmpType != Comparison.SMALLER_EQUAL) {
                return null;
            }
            Expression left = cmp.getSubexpression(0).getNonAliasExpression();
            Expression right = cmp.getSubexpression(1).getNonAliasExpression();
            if (right.isConstant()) {
                return buildHavingComparison(left, cmpType, right.getValue(session),
                        tableFilter, groupKeyCols, aggList);
            }
            if (left.isConstant()) {
                return buildHavingComparison(right, flipCmp(cmpType), left.getValue(session),
                        tableFilter, groupKeyCols, aggList);
            }
            return null;
        }
        return null;
    }

    private static HavingPredicate buildHavingComparison(
            Expression target,
            int cmpType,
            Value constant,
            TableFilter tableFilter,
            int[] groupKeyCols,
            List<AggSpec> aggList) {
        if (constant == ValueNull.INSTANCE) {
            return (groupKeys, aggVals, session, compareMode) -> false;
        }
        if (target instanceof Aggregate) {
            Aggregate agg = (Aggregate) target;
            if (agg.isDistinct()) {
                return null;
            }
            AggSpec spec = parseAggregate(agg, tableFilter, -1);
            if (spec == null) {
                return null;
            }
            int aggIdx = -1;
            for (int i = 0; i < aggList.size(); i++) {
                AggSpec existing = aggList.get(i);
                if (existing.type == spec.type && existing.col == spec.col
                        && existing.resultType.getValueType() == spec.resultType.getValueType()) {
                    aggIdx = i;
                    break;
                }
            }
            if (aggIdx < 0) {
                aggIdx = aggList.size();
                aggList.add(spec);
            }
            return new HavingComparison(-1, aggIdx, cmpType, constant);
        }
        if (target instanceof ExpressionColumn) {
            ExpressionColumn ec = (ExpressionColumn) target;
            if (ec.getTableFilter() != tableFilter) {
                return null;
            }
            int colId = ec.getColumn().getColumnId();
            for (int g = 0; g < groupKeyCols.length; g++) {
                if (groupKeyCols[g] == colId) {
                    return new HavingComparison(g, -1, cmpType, constant);
                }
            }
            return null;
        }
        return null;
    }

    private static int flipCmp(int cmpType) {
        switch (cmpType) {
        case Comparison.BIGGER:
            return Comparison.SMALLER;
        case Comparison.BIGGER_EQUAL:
            return Comparison.SMALLER_EQUAL;
        case Comparison.SMALLER:
            return Comparison.BIGGER;
        case Comparison.SMALLER_EQUAL:
            return Comparison.BIGGER_EQUAL;
        default:
            return cmpType;
        }
    }

    private interface HavingPredicate {
        boolean eval(Value[] groupKeys, Value[] aggVals, SessionLocal session, CompareMode compareMode);
    }

    private static final class HavingAndOr implements HavingPredicate {
        final boolean isAnd;
        final HavingPredicate[] children;

        HavingAndOr(boolean isAnd, HavingPredicate[] children) {
            this.isAnd = isAnd;
            this.children = children;
        }

        @Override
        public boolean eval(Value[] groupKeys, Value[] aggVals, SessionLocal session, CompareMode compareMode) {
            if (isAnd) {
                for (HavingPredicate c : children) {
                    if (!c.eval(groupKeys, aggVals, session, compareMode)) {
                        return false;
                    }
                }
                return true;
            }
            for (HavingPredicate c : children) {
                if (c.eval(groupKeys, aggVals, session, compareMode)) {
                    return true;
                }
            }
            return false;
        }
    }

    private static final class HavingComparison implements HavingPredicate {
        final int groupPos;
        final int aggIdx;
        final int cmpType;
        final Value constant;

        HavingComparison(int groupPos, int aggIdx, int cmpType, Value constant) {
            this.groupPos = groupPos;
            this.aggIdx = aggIdx;
            this.cmpType = cmpType;
            this.constant = constant;
        }

        @Override
        public boolean eval(Value[] groupKeys, Value[] aggVals, SessionLocal session, CompareMode compareMode) {
            Value v = aggIdx >= 0 ? aggVals[aggIdx] : groupKeys[groupPos];
            if (v == ValueNull.INSTANCE) {
                return false;
            }
            int c = v.compareTo(constant, session, compareMode);
            switch (cmpType) {
            case Comparison.EQUAL:
                return c == 0;
            case Comparison.NOT_EQUAL:
                return c != 0;
            case Comparison.BIGGER:
                return c > 0;
            case Comparison.BIGGER_EQUAL:
                return c >= 0;
            case Comparison.SMALLER:
                return c < 0;
            case Comparison.SMALLER_EQUAL:
                return c <= 0;
            default:
                return false;
            }
        }
    }
}

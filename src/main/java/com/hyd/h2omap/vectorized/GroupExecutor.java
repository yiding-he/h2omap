package com.hyd.h2omap.vectorized;

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
import org.h2.result.LocalResult;
import org.h2.table.TableFilter;
import org.h2.value.CompareMode;
import org.h2.value.Value;
import org.h2.value.ValueNull;

import com.hyd.h2omap.storage.ColumnSegment;
import com.hyd.h2omap.storage.ColumnarStore;
import com.hyd.h2omap.storage.ExtremeValue;
import com.hyd.h2omap.storage.VectorizedSnapshot;
import com.hyd.h2omap.table.OmapTable;
import com.hyd.h2omap.vector.ColumnVector;
import com.hyd.h2omap.vector.DictValueVector;
import com.hyd.h2omap.vector.DictVarcharVector;

/**
 * 单表聚合的下推执行：全局聚合、单列分组（字典列走码槽位快路径）与多列分组，
 * 可带 {@code HAVING}（{@link HavingPredicates}）；含段级部分聚合缓存
 * （{@link SegmentAggCache}）与裸 MIN/MAX 定界。分组键限单表列引用。
 * 执行期无存储锁，段内容来自向量化快照。
 */
final class GroupExecutor {

    private GroupExecutor() {
    }

    static boolean execute(
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
            AggSpec spec = AggSpec.parseAggregate(agg, tableFilter, i, session);
            if (spec == null) {
                return false;
            }
            aggList.add(spec);
        }

        Expression havingExpr = having != null ? having
                : (havingIndex >= 0 && havingIndex < expressions.size() ? expressions.get(havingIndex) : null);
        HavingPredicate havingPred = null;
        if (havingExpr != null) {
            havingPred = HavingPredicates.compileHaving(havingExpr, tableFilter, groupKeyCols, aggList, session);
            if (havingPred == null) {
                return false;
            }
        }

        AggSpec[] aggSpecs = aggList.toArray(new AggSpec[0]);
        int aggCount = aggSpecs.length;

        OmapTable table = (OmapTable) tableFilter.getTable();
        CompareMode compareMode = table.getCompareMode();

        // 段级部分聚合缓存：多列分组暂不参与；含 DISTINCT 聚合或无签名形态时不缓存。
        String cacheSig = groupCount <= 1
                ? buildCacheSignature(filter, groupKeyCols, groupCount, aggSpecs)
                : null;
        SegmentAggCache aggCache = cacheSig != null ? table.getStore().getAggCache() : null;

        // 裸 MIN/MAX 单列无谓词：骨架 zone map 定界求值，只扫可能持有最优值的段，
        // 逐出段仅读目标列；列类型或瞬态不支持时交回下方常规向量化路径。
        if (filter == null && havingPred == null && groupCount == 0 && aggCount == 1) {
            AggSpec spec = aggSpecs[0];
            if (!spec.distinct && spec.arithOp == null
                    && (spec.type == AggregateType.MIN || spec.type == AggregateType.MAX)) {
                ExtremeValue extreme = table.getStore()
                        .extremeColumn(spec.col, spec.type == AggregateType.MIN, session);
                if (extreme.supported) {
                    Value[] out = new Value[resultColumnCount];
                    if (spec.outIdx >= 0 && spec.outIdx < resultColumnCount) {
                        out[spec.outIdx] = extreme.value != null
                                ? extreme.value : ValueNull.INSTANCE;
                    }
                    result.addRow(out);
                    return true;
                }
            }
        }

        // 部分列读：逐出段只读分组键、聚合参数列与谓词列。
        int[] aggColumns = new int[aggCount * 2];
        for (int a = 0; a < aggCount; a++) {
            aggColumns[a * 2] = aggSpecs[a].col;
            aggColumns[a * 2 + 1] = aggSpecs[a].rightCol;
        }
        int[] snapshotColumns = OmapVectorizedExecutor.unionColumns(groupKeyCols, aggColumns,
                filter != null ? filter.referencedColumns() : null);
        VectorizedSnapshot snap = OmapVectorizedExecutor.takeSnapshot(table, filter, tableFilter, snapshotColumns);

        if (groupCount == 0) {
            executeGlobal(snap, filter, havingPred, aggSpecs, aggCount, resultColumnCount,
                    result, session, compareMode, aggCache, cacheSig);
        } else if (groupCount == 1) {
            executeSingleGroup(snap, filter, havingPred, groupKeyCols[0], groupResultIdxs[0],
                    groupByCopies, aggSpecs, aggCount, resultColumnCount, result, session, compareMode,
                    aggCache, cacheSig);
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

    private static void executeGlobal(
            VectorizedSnapshot snap,
            VectorizedFilter filter,
            HavingPredicate havingPred,
            AggSpec[] specs,
            int aggCount,
            int resultColumnCount,
            LocalResult result,
            SessionLocal session,
            CompareMode compareMode,
            SegmentAggCache aggCache,
            String cacheSig) {
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
            int segId = seg.getSegmentId();
            long segVer = snap.deltaVersions.getOrDefault(segId, 0L);
            AggState[] partial = null;
            if (aggCache != null) {
                SegmentAggCache.Entry cached = aggCache.lookup(cacheSig, segId,
                        seg.getGeneration(), segVer);
                if (cached != null) {
                    partial = (AggState[]) cached.partial;
                }
            }
            if (partial == null) {
                partial = newStates(specs, aggCount);
                BitSet masked = snap.maskedBySegment.get(segId);
                if (filter == null && (masked == null || masked.isEmpty())) {
                    for (int a = 0; a < aggCount; a++) {
                        partial[a].accumulateSegmentFull(seg, segCount, session, compareMode);
                    }
                } else {
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
                    if (selCount > 0) {
                        for (int a = 0; a < aggCount; a++) {
                            partial[a].accumulateSegmentSel(seg, sel, selCount, session, compareMode);
                        }
                    }
                }
                if (aggCache != null) {
                    aggCache.store(cacheSig, segId, seg.getGeneration(), segVer, partial);
                }
            }
            for (int a = 0; a < aggCount; a++) {
                states[a].merge(partial[a], session, compareMode);
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
            VectorizedSnapshot snap,
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
            CompareMode compareMode,
            SegmentAggCache aggCache,
            String cacheSig) {
        Map<Value, AggState[]> groups = new HashMap<>();
        int[] sel = null;

        for (ColumnSegment seg : snap.segments) {
            int segCount = seg.getRowCount();
            if (segCount == 0) {
                continue;
            }
            int segId = seg.getSegmentId();
            long segVer = snap.deltaVersions.getOrDefault(segId, 0L);
            Map<Value, AggState[]> partial = null;
            if (aggCache != null) {
                SegmentAggCache.Entry cached = aggCache.lookup(cacheSig, segId,
                        seg.getGeneration(), segVer);
                if (cached != null) {
                    partial = castGroupPartial(cached.partial);
                }
            }
            if (partial == null) {
                partial = new HashMap<>();
                BitSet masked = snap.maskedBySegment.get(segId);
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
                if (selCount > 0) {
                    ColumnVector keyVec = seg.getColumn(groupKeyCol);
                    if (keyVec instanceof DictVarcharVector) {
                        DictVarcharVector dv = (DictVarcharVector) keyVec;
                        groupByDict(seg, dv.rawCodes(), dv.rawDict(), dv.dictSize(),
                                sel, selCount, full, specs, aggCount, partial, session, compareMode);
                    } else if (keyVec instanceof DictValueVector) {
                        DictValueVector dv = (DictValueVector) keyVec;
                        groupByDict(seg, dv.rawCodes(), dv.rawDict(), dv.dictSize(),
                                sel, selCount, full, specs, aggCount, partial, session, compareMode);
                    } else {
                        for (int i = 0; i < selCount; i++) {
                            int idx = full ? i : sel[i];
                            Value key = keyVec.get(idx);
                            AggState[] states = partial.get(key);
                            if (states == null) {
                                states = newStates(specs, aggCount);
                                partial.put(key, states);
                            }
                            for (int a = 0; a < aggCount; a++) {
                                states[a].accumulateAt(seg, idx, session, compareMode);
                            }
                        }
                    }
                }
                // 高基数分组的段级部分结果本身就太大，不缓存。
                if (aggCache != null && partial.size() <= SegmentAggCache.MAX_GROUP_KEYS) {
                    aggCache.store(cacheSig, segId, seg.getGeneration(), segVer, partial);
                }
            }
            mergeGroupPartials(groups, partial, specs, aggCount, session, compareMode);
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

    @SuppressWarnings("unchecked")
    private static Map<Value, AggState[]> castGroupPartial(Object partial) {
        return (Map<Value, AggState[]>) partial;
    }

    private static void mergeGroupPartials(Map<Value, AggState[]> groups,
            Map<Value, AggState[]> partial, AggSpec[] specs, int aggCount,
            SessionLocal session, CompareMode compareMode) {
        for (Map.Entry<Value, AggState[]> entry : partial.entrySet()) {
            AggState[] target = groups.get(entry.getKey());
            if (target == null) {
                target = newStates(specs, aggCount);
                groups.put(entry.getKey(), target);
            }
            // 不按引用共享：partial 可能是缓存里的对象，groups 的状态随后还会被
            // extraRows 累加，共享引用会污染缓存的部分结果。
            AggState[] source = entry.getValue();
            for (int a = 0; a < target.length; a++) {
                target[a].merge(source[a], session, compareMode);
            }
        }
    }

    /**
     * 构造「过滤 × 分组键 × 聚合口径」的查询签名，作为段级部分聚合缓存的键。
     *
     * <p>签名必须完整覆盖语义：过滤树的列、比较与常量，分组键列，每个聚合的
     * 类型、列、算术口径与结果类型。含 DISTINCT 聚合或无签名过滤形态
     * （子查询预计算等）时返回 {@code null}，该查询不参与缓存。
     */
    private static String buildCacheSignature(VectorizedFilter filter, int[] groupKeyCols,
            int groupCount, AggSpec[] specs) {
        String filterSig = filter == null ? "" : filter.signature();
        if (filterSig == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder(filterSig).append("|g");
        for (int g = 0; g < groupCount; g++) {
            sb.append(groupKeyCols[g]).append(',');
        }
        sb.append('|');
        for (AggSpec spec : specs) {
            if (spec.distinct) {
                return null;
            }
            sb.append(spec.type.ordinal()).append(',')
                    .append(spec.col).append(',')
                    .append(spec.rightCol).append(',')
                    .append(spec.arithOp).append(',')
                    .append(spec.colOnLeft).append(',')
                    .append(spec.resultType.getValueType()).append(',');
            if (spec.arithConst != null) {
                sb.append(spec.arithConst.getValueType()).append(':')
                        .append(spec.arithConst).append(';');
            } else {
                sb.append(';');
            }
        }
        return sb.toString();
    }

    private static void executeMultiGroup(
            VectorizedSnapshot snap,
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
}

package com.hyd.h2omap.vectorized;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.h2.command.query.Query;
import org.h2.command.query.Select;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.Subquery;
import org.h2.expression.aggregate.AggregateType;
import org.h2.expression.condition.Comparison;
import org.h2.table.TableFilter;
import org.h2.value.CompareMode;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueBigint;
import org.h2.value.ValueDouble;
import org.h2.value.ValueInteger;
import org.h2.value.ValueNull;
import org.h2.value.ValueNumeric;

import com.hyd.h2omap.storage.ColumnSegment;
import com.hyd.h2omap.storage.VectorizedSnapshot;
import com.hyd.h2omap.table.OmapTable;
import com.hyd.h2omap.vector.ColumnVector;
import com.hyd.h2omap.vector.DictValueVector;
import com.hyd.h2omap.vector.DictVarcharVector;
import com.hyd.h2omap.vector.DoubleVector;
import com.hyd.h2omap.vector.IntVector;
import com.hyd.h2omap.vector.LongVector;

/**
 * S3：单列等值关联的标量聚合子查询折叠。
 *
 * <p>编译「外层列与关联标量聚合子查询比较」：对内层表一次向量化快照预计算
 * 「归一化关联键 → 聚合结果」，空组取 {@code COUNT} 为 0、其余聚合为 NULL 的空值语义；
 * 外层每行按键查表比较，整数比较列走边界快路径。聚合口径对齐 H2 的
 * {@code AggregateDataDefault} / {@code AggregateDataAvg}。全部方法在
 * {@code VectorizedFilter.compile} 编译期调用，不持有存储锁。
 */
final class CorrelatedAggFold {

    private CorrelatedAggFold() {
    }

    /**
     * 编译「外层列与关联标量聚合子查询比较」。
     *
     * @param left    比较中属于外层表的一侧
     * @param cmpType 以 {@code left} 为左侧的比较类型
     */
    static VectorizedFilter.Node compile(Expression left, int cmpType, Subquery subq,
            TableFilter filter, SessionLocal session) {
        int cmpCol = VectorizedFilter.resolveColumn(left, filter);
        if (cmpCol < 0) {
            return null;
        }
        Query query = subq.getQuery();
        if (!(query instanceof Select)) {
            return null;
        }
        SubqueryInner.InnerQuery inner = SubqueryInner.parseInner((Select) query, filter, session, true);
        if (inner == null) {
            return null;
        }
        Map<Object, Value> results = precomputeAggregate(inner, session);
        if (results == null) {
            return null;
        }
        return new CorrAggCmpNode(inner.outerKeyCol, cmpCol, cmpType, results,
                emptyAggregateResult(inner.aggregate.getAggregateType()), inner.outerKeyType,
                filter.getTable().getColumn(cmpCol).getType());
    }

    /** 对内层表做一次分组聚合；键规范化到外层键列类型。失败返回 {@code null}。 */
    private static Map<Object, Value> precomputeAggregate(SubqueryInner.InnerQuery inner,
            SessionLocal session) {
        OmapTable table = (OmapTable) inner.innerFilter.getTable();
        CompareMode compareMode = table.getCompareMode();
        AggregateType type = inner.aggregate.getAggregateType();
        TypeInfo resultType = inner.aggregate.getType();
        // 组用列表按槽位保存，槽位由键值解析而来；字典列的槽位可以按字典下标一次算好，
        // 内层循环里只剩数组读，不做 Value 哈希。
        Map<Value, Integer> slotOf = new HashMap<>();
        List<GroupAgg> slotGroups = new ArrayList<>();
        List<Value> slotKeys = new ArrayList<>();
        VectorizedSnapshot snapshot = table.getStore().takeVectorizedSnapshot();
        try {
            int[] count = new int[1];
            for (ColumnSegment segment : snapshot.segments) {
                int[] sel = SubqueryInner.innerSelection(inner, segment, snapshot, session, compareMode, count);
                int selCount = count[0];
                if (selCount == 0) {
                    continue;
                }
                ColumnVector keyVec = segment.getColumn(inner.innerKeyCol);
                ColumnVector aggVec = inner.aggCol >= 0 ? segment.getColumn(inner.aggCol) : null;
                int[] dictCodes = SubqueryInner.dictCodes(keyVec);
                if (dictCodes != null) {
                    Value[] dict = SubqueryInner.rawDict(keyVec);
                    int[] codeSlot = new int[dict.length];
                    for (int c = 0; c < dict.length; c++) {
                        codeSlot[c] = resolveSlot(dict[c], slotOf, slotGroups, slotKeys,
                                type, resultType, session, compareMode);
                    }
                    for (int i = 0; i < selCount; i++) {
                        int idx = sel == null ? i : sel[i];
                        int code = dictCodes[idx];
                        if (code < 0) {
                            continue;
                        }
                        GroupAgg group = slotGroups.get(codeSlot[code]);
                        if (aggVec != null) {
                            group.addColumn(aggVec, idx);
                        } else {
                            group.count++;
                        }
                    }
                    continue;
                }
                for (int i = 0; i < selCount; i++) {
                    int idx = sel == null ? i : sel[i];
                    Value k = keyVec.get(idx);
                    if (k == ValueNull.INSTANCE) {
                        continue;
                    }
                    GroupAgg group = slotGroups.get(resolveSlot(k, slotOf, slotGroups, slotKeys,
                            type, resultType, session, compareMode));
                    if (aggVec != null) {
                        group.addColumn(aggVec, idx);
                    } else {
                        group.count++;
                    }
                }
            }
            for (Value[] row : snapshot.extraRows) {
                if (inner.constFilter != null
                        && !inner.constFilter.evalRow(row, session, compareMode)) {
                    continue;
                }
                Value k = row[inner.innerKeyCol];
                if (k == ValueNull.INSTANCE) {
                    continue;
                }
                GroupAgg group = slotGroups.get(resolveSlot(k, slotOf, slotGroups, slotKeys,
                        type, resultType, session, compareMode));
                if (inner.aggCol >= 0) {
                    group.addValue(row[inner.aggCol]);
                } else {
                    group.count++;
                }
            }
        } catch (RuntimeException e) {
            return null;
        }
        Map<Object, Value> results = new HashMap<>(slotGroups.size() * 2);
        try {
            for (int s = 0; s < slotGroups.size(); s++) {
                results.put(SubqueryInner.normalizeKey(slotKeys.get(s), inner.outerKeyType, session),
                        slotGroups.get(s).result());
            }
        } catch (RuntimeException e) {
            return null;
        }
        return results;
    }

    /** 解析键值对应的组槽位；{@code ValueNull} 返回 {@code -1}。 */
    private static int resolveSlot(Value key, Map<Value, Integer> slotOf, List<GroupAgg> slotGroups,
            List<Value> slotKeys, AggregateType type, TypeInfo resultType,
            SessionLocal session, CompareMode compareMode) {
        if (key == ValueNull.INSTANCE) {
            return -1;
        }
        Integer slot = slotOf.get(key);
        if (slot != null) {
            return slot;
        }
        int s = slotGroups.size();
        slotGroups.add(new GroupAgg(type, resultType, session, compareMode));
        slotKeys.add(key);
        slotOf.put(key, s);
        return s;
    }

    /** 关联键无匹配组时的聚合结果：COUNT 为 0，其余为 NULL。 */
    private static Value emptyAggregateResult(AggregateType type) {
        return type == AggregateType.COUNT || type == AggregateType.COUNT_ALL
                ? ValueBigint.get(0)
                : ValueNull.INSTANCE;
    }

    /**
     * 单组聚合累加器，口径对齐 H2 的 {@code AggregateDataDefault} / {@code AggregateDataAvg} /
     * {@code AggregateDataCount}：SUM 溢出即失败回退；AVG 按结果类型走 double 或
     * {@code BigDecimal} 除法（HALF_DOWN）；MIN/MAX 按数值次序比较。
     *
     * <p>段内列走原始数组，不逐行装箱；{@code extraRows} 走 {@link Value} 路径。
     * MIN/MAX 因此有两个累加器，取值时按类型合并。
     */
    static final class GroupAgg {
        private final AggregateType type;
        private final TypeInfo resultType;
        private long count;
        private long longSum;
        private double doubleSum;
        private BigDecimal decimalSum = BigDecimal.ZERO;
        /** 段内列（原始数组路径）的 MIN/MAX 累加器：Integer / Long / Double。 */
        private Object extremeRaw;
        /** {@code extraRows} 路径的 MIN/MAX 累加器。 */
        private Value extreme;

        private final SessionLocal session;
        private final CompareMode compareMode;

        GroupAgg(AggregateType type, TypeInfo resultType, SessionLocal session, CompareMode compareMode) {
            this.type = type;
            this.resultType = resultType;
            this.session = session;
            this.compareMode = compareMode;
        }

        void addColumn(ColumnVector vec, int idx) {
            switch (type) {
            case COUNT_ALL:
                count++;
                return;
            case COUNT:
                if (!vec.nulls().isNull(idx)) {
                    count++;
                }
                return;
            case SUM:
            case AVG:
                if (vec.nulls().isNull(idx)) {
                    return;
                }
                count++;
                if (!accumulateRaw(vec, idx)) {
                    addSumValue(vec.get(idx));
                }
                return;
            case MIN:
            case MAX:
                if (vec.nulls().isNull(idx) || addExtremeRaw(vec, idx)) {
                    return;
                }
                addExtreme(vec.get(idx));
                return;
            default:
                throw new AssertionError();
            }
        }

        /** 按原始数组累加；不支持的向量类型返回 {@code false}。 */
        private boolean accumulateRaw(ColumnVector vec, int idx) {
            int resultValueType = resultType.getValueType();
            boolean asDouble = resultValueType == Value.DOUBLE;
            boolean asLong = resultValueType == Value.BIGINT;
            if (vec instanceof IntVector) {
                int v = ((IntVector) vec).rawValues()[idx];
                if (asDouble) {
                    doubleSum += v;
                } else if (asLong) {
                    longSum = Math.addExact(longSum, v);
                } else {
                    decimalSum = decimalSum.add(BigDecimal.valueOf(v));
                }
                return true;
            }
            if (vec instanceof LongVector) {
                long v = ((LongVector) vec).rawValues()[idx];
                if (asDouble) {
                    doubleSum += v;
                } else if (asLong) {
                    longSum = Math.addExact(longSum, v);
                } else {
                    decimalSum = decimalSum.add(BigDecimal.valueOf(v));
                }
                return true;
            }
            if (vec instanceof DoubleVector) {
                double v = ((DoubleVector) vec).rawValues()[idx];
                if (asDouble) {
                    doubleSum += v;
                } else {
                    decimalSum = decimalSum.add(BigDecimal.valueOf(v));
                }
                return true;
            }
            return false;
        }

        /** 按原始数组比较 MIN/MAX；不支持的向量类型返回 {@code false}。 */
        private boolean addExtremeRaw(ColumnVector vec, int idx) {
            if (vec instanceof IntVector) {
                int v = ((IntVector) vec).rawValues()[idx];
                Integer cur = (Integer) extremeRaw;
                if (cur == null || (type == AggregateType.MIN ? v < cur : v > cur)) {
                    extremeRaw = v;
                }
                return true;
            }
            if (vec instanceof LongVector) {
                long v = ((LongVector) vec).rawValues()[idx];
                Long cur = (Long) extremeRaw;
                if (cur == null || (type == AggregateType.MIN ? v < cur : v > cur)) {
                    extremeRaw = v;
                }
                return true;
            }
            if (vec instanceof DoubleVector) {
                double v = ((DoubleVector) vec).rawValues()[idx];
                Double cur = (Double) extremeRaw;
                if (cur == null || (type == AggregateType.MIN ? v < cur : v > cur)) {
                    extremeRaw = v;
                }
                return true;
            }
            return false;
        }

        void addValue(Value v) {
            switch (type) {
            case COUNT_ALL:
                count++;
                return;
            case COUNT:
                if (v != ValueNull.INSTANCE) {
                    count++;
                }
                return;
            case SUM:
            case AVG:
                if (v == ValueNull.INSTANCE) {
                    return;
                }
                count++;
                addSumValue(v);
                return;
            case MIN:
            case MAX:
                addExtreme(v);
                return;
            default:
                throw new AssertionError();
            }
        }

        private void addExtreme(Value v) {
            if (v == ValueNull.INSTANCE) {
                return;
            }
            if (extreme == null) {
                extreme = v;
                return;
            }
            int cmp = v.compareTo(extreme, session, compareMode);
            if ((type == AggregateType.MIN && cmp < 0) || (type == AggregateType.MAX && cmp > 0)) {
                extreme = v;
            }
        }

        private void addSumValue(Value v) {
            switch (resultType.getValueType()) {
            case Value.BIGINT:
                longSum = Math.addExact(longSum, v.getLong());
                break;
            case Value.DOUBLE:
                doubleSum += v.getDouble();
                break;
            default:
                decimalSum = decimalSum.add(v.getBigDecimal());
                break;
            }
        }

        Value result() {
            switch (type) {
            case COUNT_ALL:
            case COUNT:
                return ValueBigint.get(count);
            case MIN:
            case MAX:
                return extremeValue();
            case SUM:
                return count == 0 ? ValueNull.INSTANCE : sumValue();
            case AVG:
                if (count == 0) {
                    return ValueNull.INSTANCE;
                }
                if (resultType.getValueType() == Value.DOUBLE) {
                    return ValueDouble.get(doubleSum / count);
                }
                return ValueNumeric.get(decimalSum.divide(
                        BigDecimal.valueOf(count), resultType.getScale(), RoundingMode.HALF_DOWN));
            default:
                throw new AssertionError();
            }
        }

        /** 合并原始数组与 {@code Value} 两个 MIN/MAX 累加器。 */
        private Value extremeValue() {
            Value raw = null;
            if (extremeRaw instanceof Integer) {
                raw = ValueInteger.get((Integer) extremeRaw);
            } else if (extremeRaw instanceof Long) {
                raw = ValueBigint.get((Long) extremeRaw);
            } else if (extremeRaw instanceof Double) {
                raw = ValueDouble.get((Double) extremeRaw);
            }
            if (extreme == null) {
                return raw == null ? ValueNull.INSTANCE : raw;
            }
            if (raw == null) {
                return extreme;
            }
            int cmp = raw.compareTo(extreme, session, compareMode);
            return (type == AggregateType.MIN ? cmp <= 0 : cmp >= 0) ? raw : extreme;
        }

        private Value sumValue() {
            switch (resultType.getValueType()) {
            case Value.BIGINT:
                return ValueBigint.get(longSum);
            case Value.DOUBLE:
                return ValueDouble.get(doubleSum);
            default:
                return ValueNumeric.get(decimalSum);
            }
        }
    }

    /**
     * 关联聚合比较：外层比较列的值与「按关联键分组的聚合结果」比较。
     * 整数比较列走整数边界快路径（复用 {@code integerBound} 的收敛规则），
     * 其余逐行取 Value 比较。空组取 {@code emptyDefault}。
     */
    static final class CorrAggCmpNode implements VectorizedFilter.Node {
        private static final int OP_ALWAYS = 100;
        private static final int OP_NEVER = 101;

        private final int keyCol;
        private final int cmpCol;
        private final int cmpType;
        private final Map<Object, Value> results;
        private final Value emptyDefault;
        private final TypeInfo outerKeyType;
        private final Map<Object, FastCmp> fast;
        private final FastCmp defaultCmp;

        CorrAggCmpNode(int keyCol, int cmpCol, int cmpType, Map<Object, Value> results,
                Value emptyDefault, TypeInfo outerKeyType, TypeInfo cmpColType) {
            this.keyCol = keyCol;
            this.cmpCol = cmpCol;
            this.cmpType = cmpType;
            this.results = results;
            this.emptyDefault = emptyDefault;
            this.outerKeyType = outerKeyType;
            this.fast = buildFast(results, cmpColType, cmpType);
            this.defaultCmp = emptyDefault == ValueNull.INSTANCE
                    ? new FastCmp(0, OP_NEVER)
                    : new FastCmp(emptyDefault.getLong(), cmpType);
        }

        @Override
        public void collectColumns(java.util.function.IntConsumer sink) {
            sink.accept(keyCol);
            sink.accept(cmpCol);
        }

        private static Map<Object, FastCmp> buildFast(Map<Object, Value> results, TypeInfo cmpColType,
                int cmpType) {
            int type = cmpColType.getValueType();
            if (type != Value.TINYINT && type != Value.SMALLINT
                    && type != Value.INTEGER && type != Value.BIGINT) {
                return null;
            }
            try {
                Map<Object, FastCmp> map = new HashMap<>(results.size() * 2);
                for (Map.Entry<Object, Value> entry : results.entrySet()) {
                    Value v = entry.getValue();
                    FastCmp cmp;
                    if (v == ValueNull.INSTANCE) {
                        cmp = new FastCmp(0, OP_NEVER);
                    } else {
                        int[] holder = { cmpType };
                        Value bound = VectorizedFilter.integerBound(v, cmpColType, holder);
                        if (bound == null) {
                            // EQUAL/NOT_EQUAL 且带小数：整数列不可能等于该值
                            cmp = new FastCmp(0, cmpType == Comparison.NOT_EQUAL ? OP_ALWAYS : OP_NEVER);
                        } else {
                            cmp = new FastCmp(bound.getLong(), holder[0]);
                        }
                    }
                    map.put(entry.getKey(), cmp);
                }
                return map;
            } catch (RuntimeException e) {
                return null;
            }
        }

        @Override
        public int filterSegment(ColumnSegment segment, int[] sel, int selCount, boolean fullRange,
                BitSet masked, SessionLocal session, CompareMode compareMode) {
            ColumnVector keyVec = segment.getColumn(keyCol);
            ColumnVector cmpVec = segment.getColumn(cmpCol);
            if (fast != null && cmpVec instanceof IntVector
                    && (keyVec instanceof IntVector || SubqueryInner.isDict(keyVec))) {
                return filterIntFast(keyVec, (IntVector) cmpVec, sel, selCount, fullRange, session);
            }
            if (fast != null && cmpVec instanceof LongVector
                    && (keyVec instanceof LongVector || SubqueryInner.isDict(keyVec))) {
                return filterLongFast(keyVec, (LongVector) cmpVec, sel, selCount, fullRange, session);
            }
            int out = 0;
            if (fullRange) {
                for (int i = 0; i < selCount; i++) {
                    if (matchGeneric(keyVec, cmpVec, i, session, compareMode)) {
                        sel[out++] = i;
                    }
                }
            } else {
                for (int i = 0; i < selCount; i++) {
                    int idx = sel[i];
                    if (matchGeneric(keyVec, cmpVec, idx, session, compareMode)) {
                        sel[out++] = idx;
                    }
                }
            }
            return out;
        }

        /** 整数比较列的快路径；键列是 IntVector 或字典向量。 */
        private int filterIntFast(ColumnVector keyVec, IntVector cmpVec, int[] sel, int selCount,
                boolean fullRange, SessionLocal session) {
            int[] cmpVals = cmpVec.rawValues();
            long[] cmpBits = SubqueryInner.nullableBits(cmpVec);
            int[] keyVals = keyVec instanceof IntVector ? ((IntVector) keyVec).rawValues() : null;
            int[] keyCodes = keyVals == null ? SubqueryInner.dictCodes(keyVec) : null;
            FastCmp[] dictKeys = keyCodes != null ? dictCmps(keyVec, session) : null;
            long[] keyBits = SubqueryInner.nullableBits(keyVec);
            int out = 0;
            if (fullRange) {
                for (int i = 0; i < selCount; i++) {
                    if (matchIntRow(i, cmpVals, cmpBits, keyVals, keyCodes, dictKeys, keyBits)) {
                        sel[out++] = i;
                    }
                }
            } else {
                for (int i = 0; i < selCount; i++) {
                    int idx = sel[i];
                    if (matchIntRow(idx, cmpVals, cmpBits, keyVals, keyCodes, dictKeys, keyBits)) {
                        sel[out++] = idx;
                    }
                }
            }
            return out;
        }

        private boolean matchIntRow(int idx, int[] cmpVals, long[] cmpBits, int[] keyVals,
                int[] keyCodes, FastCmp[] dictKeys, long[] keyBits) {
            if (cmpBits != null && (cmpBits[idx >>> 6] & (1L << idx)) != 0) {
                return false;
            }
            FastCmp c;
            if (keyVals != null) {
                c = keyBits != null && (keyBits[idx >>> 6] & (1L << idx)) != 0
                        ? defaultCmp
                        : fast.get(keyVals[idx]);
            } else {
                int code = keyCodes[idx];
                c = code < 0 ? defaultCmp : dictKeys[code];
            }
            if (c == null) {
                c = defaultCmp;
            }
            return matchFast(cmpVals[idx], c);
        }

        /** 长整数比较列的快路径；键列是 LongVector 或字典向量。 */
        private int filterLongFast(ColumnVector keyVec, LongVector cmpVec, int[] sel, int selCount,
                boolean fullRange, SessionLocal session) {
            long[] cmpVals = cmpVec.rawValues();
            long[] cmpBits = SubqueryInner.nullableBits(cmpVec);
            long[] keyVals = keyVec instanceof LongVector ? ((LongVector) keyVec).rawValues() : null;
            int[] keyCodes = keyVals == null ? SubqueryInner.dictCodes(keyVec) : null;
            FastCmp[] dictKeys = keyCodes != null ? dictCmps(keyVec, session) : null;
            long[] keyBits = SubqueryInner.nullableBits(keyVec);
            int out = 0;
            if (fullRange) {
                for (int i = 0; i < selCount; i++) {
                    if (matchLongRow(i, cmpVals, cmpBits, keyVals, keyCodes, dictKeys, keyBits)) {
                        sel[out++] = i;
                    }
                }
            } else {
                for (int i = 0; i < selCount; i++) {
                    int idx = sel[i];
                    if (matchLongRow(idx, cmpVals, cmpBits, keyVals, keyCodes, dictKeys, keyBits)) {
                        sel[out++] = idx;
                    }
                }
            }
            return out;
        }

        private boolean matchLongRow(int idx, long[] cmpVals, long[] cmpBits, long[] keyVals,
                int[] keyCodes, FastCmp[] dictKeys, long[] keyBits) {
            if (cmpBits != null && (cmpBits[idx >>> 6] & (1L << idx)) != 0) {
                return false;
            }
            FastCmp c;
            if (keyVals != null) {
                c = keyBits != null && (keyBits[idx >>> 6] & (1L << idx)) != 0
                        ? defaultCmp
                        : fast.get(keyVals[idx]);
            } else {
                int code = keyCodes[idx];
                c = code < 0 ? defaultCmp : dictKeys[code];
            }
            if (c == null) {
                c = defaultCmp;
            }
            return matchFast(cmpVals[idx], c);
        }

        /** 按字典下标预解析每个码对应的比较目标；缺失的码留 {@code null}，由行级匹配回退到默认值。 */
        private FastCmp[] dictCmps(ColumnVector vec, SessionLocal session) {
            Value[] dict = vec instanceof DictValueVector
                    ? ((DictValueVector) vec).rawDict()
                    : ((DictVarcharVector) vec).rawDict();
            FastCmp[] cmps = new FastCmp[dict.length];
            for (int i = 0; i < dict.length; i++) {
                Value v = dict[i];
                cmps[i] = v == ValueNull.INSTANCE
                        ? null
                        : fast.get(SubqueryInner.normalizeKey(v, outerKeyType, session));
            }
            return cmps;
        }

        private static boolean matchFast(long v, FastCmp c) {
            switch (c.op) {
            case OP_ALWAYS:
                return true;
            case OP_NEVER:
                return false;
            case Comparison.EQUAL:
                return v == c.bound;
            case Comparison.NOT_EQUAL:
                return v != c.bound;
            case Comparison.SMALLER:
                return v < c.bound;
            case Comparison.BIGGER:
                return v > c.bound;
            case Comparison.SMALLER_EQUAL:
                return v <= c.bound;
            case Comparison.BIGGER_EQUAL:
                return v >= c.bound;
            default:
                return false;
            }
        }

        private boolean matchGeneric(ColumnVector keyVec, ColumnVector cmpVec, int idx,
                SessionLocal session, CompareMode compareMode) {
            Value v = cmpVec.get(idx);
            if (v == ValueNull.INSTANCE) {
                return false;
            }
            Value k = keyVec.get(idx);
            Value target;
            if (k == ValueNull.INSTANCE) {
                target = emptyDefault;
            } else {
                target = results.get(SubqueryInner.normalizeKey(k, outerKeyType, session));
                if (target == null) {
                    target = emptyDefault;
                }
            }
            if (target == ValueNull.INSTANCE) {
                return false;
            }
            return VectorizedFilter.matchCompare(v.compareTo(target, session, compareMode), cmpType);
        }

        @Override
        public boolean evalRow(Value[] row, SessionLocal session, CompareMode compareMode) {
            Value v = row[cmpCol];
            if (v == ValueNull.INSTANCE) {
                return false;
            }
            Value k = row[keyCol];
            Value target;
            if (k == ValueNull.INSTANCE) {
                target = emptyDefault;
            } else {
                target = results.get(SubqueryInner.normalizeKey(k, outerKeyType, session));
                if (target == null) {
                    target = emptyDefault;
                }
            }
            if (target == ValueNull.INSTANCE) {
                return false;
            }
            return VectorizedFilter.matchCompare(v.compareTo(target, session, compareMode), cmpType);
        }

        private static final class FastCmp {
            final long bound;
            final int op;

            FastCmp(long bound, int op) {
                this.bound = bound;
                this.op = op;
            }
        }
    }
}

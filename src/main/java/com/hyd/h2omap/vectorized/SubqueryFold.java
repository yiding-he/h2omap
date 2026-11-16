package com.hyd.h2omap.vectorized;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.h2.command.query.Query;
import org.h2.command.query.Select;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.Subquery;
import org.h2.expression.aggregate.Aggregate;
import org.h2.expression.aggregate.AggregateType;
import org.h2.expression.aggregate.H2AggregateAccess;
import org.h2.expression.condition.ConditionAndOr;
import org.h2.expression.condition.ConditionAndOrN;
import org.h2.expression.condition.ConditionInQuery;
import org.h2.expression.condition.ConditionNot;
import org.h2.expression.condition.Comparison;
import org.h2.expression.condition.ExistsPredicate;
import org.h2.expression.condition.H2ConditionAccess;
import org.h2.result.ResultInterface;
import org.h2.table.TableFilter;
import org.h2.value.CompareMode;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueBigint;
import org.h2.value.ValueDouble;
import org.h2.value.ValueNull;
import org.h2.value.ValueNumeric;

import com.hyd.h2omap.storage.ColumnSegment;
import com.hyd.h2omap.storage.ColumnarStore.VectorizedSnapshot;
import com.hyd.h2omap.table.OmapTable;
import com.hyd.h2omap.vector.ColumnVector;
import com.hyd.h2omap.vector.DictValueVector;
import com.hyd.h2omap.vector.IntVector;
import com.hyd.h2omap.vector.LongVector;
import com.hyd.h2omap.vector.NullBitmap;

/**
 * 把 {@code WHERE} 里的子查询谓词编译成向量化过滤节点：
 *
 * <ul>
 * <li>不相关 {@code IN (SELECT ...)}：子查询一次求值，折成集合节点；
 * <li>单列等值关联的标量聚合子查询：对内层表预计算一次分组聚合，外层按关联键查表比较；
 * <li>关联 {@code EXISTS} / {@code NOT EXISTS}：预计算内层关联键集合，折成半连接节点。
 * </ul>
 *
 * 不识别的形态返回 {@code null}，由上层回退 H2 原路径。语义上等价于
 * 「内层查询按关联列分组、空组取聚合的空值」。
 */
final class SubqueryFold {

    private SubqueryFold() {
    }

    // ------------------------------------------------------------------
    // 编译入口
    // ------------------------------------------------------------------

    /** 编译 {@code IN (SELECT)}、{@code EXISTS}、{@code NOT EXISTS} 谓词。 */
    static VectorizedFilter.Node compileNode(Expression expr, TableFilter filter, SessionLocal session) {
        if (expr instanceof ConditionInQuery) {
            return compileInQuery((ConditionInQuery) expr, filter, session);
        }
        if (expr instanceof ConditionNot) {
            Expression sub = expr.getSubexpression(0);
            if (sub instanceof ExistsPredicate) {
                return compileExists((ExistsPredicate) sub, true, filter, session);
            }
            return null;
        }
        if (expr instanceof ExistsPredicate) {
            return compileExists((ExistsPredicate) expr, false, filter, session);
        }
        return null;
    }

    /**
     * 编译「外层列与关联标量聚合子查询比较」。
     *
     * @param left    比较中属于外层表的一侧
     * @param cmpType 以 {@code left} 为左侧的比较类型
     */
    static VectorizedFilter.Node compileCorrelatedCmp(Expression left, int cmpType, Subquery subq,
            TableFilter filter, SessionLocal session) {
        int cmpCol = VectorizedFilter.resolveColumn(left, filter);
        if (cmpCol < 0) {
            return null;
        }
        Query query = subq.getQuery();
        if (!(query instanceof Select)) {
            return null;
        }
        Inner inner = parseInner((Select) query, filter, session, true);
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

    // ------------------------------------------------------------------
    // IN (SELECT ...)
    // ------------------------------------------------------------------

    private static VectorizedFilter.Node compileInQuery(ConditionInQuery in, TableFilter filter,
            SessionLocal session) {
        if (in.isWhenConditionOperand() || in.isAll() || in.getCompareType() != Comparison.EQUAL) {
            return null;
        }
        boolean not = in.isNot();
        Query query = H2ConditionAccess.getInQueryQuery(in);
        if (VectorizedFilter.referencesFilter(query, filter)) {
            // 关联 IN 不在本轮范围
            return null;
        }
        if (query.getColumnCount() != 1) {
            return null;
        }
        int col = VectorizedFilter.resolveColumn(in.getLeft(), filter);
        if (col < 0) {
            return null;
        }
        List<Value> values = new ArrayList<>();
        boolean hasNull = false;
        query.setSession(session);
        try (ResultInterface result = query.query(0)) {
            while (result.next()) {
                Value v = result.currentRow()[0];
                if (v == ValueNull.INSTANCE) {
                    hasNull = true;
                } else {
                    values.add(v);
                }
            }
        } catch (RuntimeException e) {
            return null;
        }
        if (values.isEmpty()) {
            // 空集：IN 恒 false，NOT IN 恒 true（左侧为 NULL 的行同样如此）
            return new ConstNode(not);
        }
        if (not && hasNull) {
            // 子查询结果含 NULL 时，NOT IN 对任何左值都是 UNKNOWN
            return new ConstNode(false);
        }
        TypeInfo colType = filter.getTable().getColumn(col).getType();
        try {
            for (int i = 0; i < values.size(); i++) {
                values.set(i, values.get(i).convertTo(colType, session));
            }
        } catch (RuntimeException e) {
            return null;
        }
        // 整数列优先用原始 long 集合，避免百万级 Value 装箱哈希
        int valueType = colType.getValueType();
        if (valueType == Value.TINYINT || valueType == Value.SMALLINT
                || valueType == Value.INTEGER || valueType == Value.BIGINT) {
            try {
                return new VectorizedFilter.InNode(col, not, toLongSet(values));
            } catch (RuntimeException e) {
                return null;
            }
        }
        return new VectorizedFilter.InNode(col, not, values);
    }

    private static VectorizedFilter.LongSet toLongSet(List<Value> values) {
        VectorizedFilter.LongSet set = new VectorizedFilter.LongSet(values.size());
        for (Value v : values) {
            set.add(v.getLong());
        }
        return set;
    }

    // ------------------------------------------------------------------
    // EXISTS / NOT EXISTS
    // ------------------------------------------------------------------

    private static VectorizedFilter.Node compileExists(ExistsPredicate exists, boolean not,
            TableFilter filter, SessionLocal session) {
        Query query = H2ConditionAccess.getExistsQuery(exists);
        if (!(query instanceof Select)) {
            return null;
        }
        if (!VectorizedFilter.referencesFilter(query, filter)) {
            query.setSession(session);
            try {
                return new ConstNode(query.exists() ^ not);
            } catch (RuntimeException e) {
                return null;
            }
        }
        Inner inner = parseInner((Select) query, filter, session, false);
        if (inner == null) {
            return null;
        }
        Set<Object> keys = precomputeKeySet(inner, session);
        if (keys == null) {
            return null;
        }
        return new SemiJoinNode(inner.outerKeyCol, keys, not, inner.outerKeyType);
    }

    // ------------------------------------------------------------------
    // 内层形态解析（关联聚合与关联 EXISTS 共用）
    // ------------------------------------------------------------------

    /** 内层查询的解析结果。 */
    private static final class Inner {
        TableFilter innerFilter;
        int innerKeyCol;
        int outerKeyCol;
        TypeInfo outerKeyType;
        /** 内层常量谓词，编译在内层表的列上；无条件时为 null。 */
        VectorizedFilter constFilter;
        /** 仅关联聚合使用。 */
        Aggregate aggregate;
        /** 聚合参数列；COUNT(*) 为 -1。 */
        int aggCol;
    }

    /**
     * 识别「单表内层查询 + 恰好一个等值关联 + 其余条件只引用内层表」的形态。
     *
     * <p>接受的形态：内层是 OMAP 单表查询，无 ORDER BY / LIMIT / OFFSET / 窗口；
     * 关联聚合额外要求内层是单聚合（COUNT/COUNT(*)/SUM/MIN/MAX/AVG），
     * SUM/AVG 的参数列必须是整数列；关联 EXISTS 要求内层非分组查询。
     */
    private static Inner parseInner(Select select, TableFilter outerFilter, SessionLocal session,
            boolean aggregate) {
        if (select.getSortOrder() != null || select.isWindowQuery()
                || select.getFetch() != null || select.getOffset() != null) {
            return null;
        }
        List<TableFilter> innerFilters = select.getTableFilters();
        if (innerFilters.size() != 1) {
            return null;
        }
        TableFilter innerFilter = innerFilters.get(0);
        if (!(innerFilter.getTable() instanceof OmapTable)) {
            return null;
        }
        Inner inner = new Inner();
        inner.innerFilter = innerFilter;
        if (aggregate) {
            if (!select.isGroupQuery()) {
                return null;
            }
            List<Expression> expressions = select.getExpressions();
            if (expressions.size() != 1 || !(expressions.get(0) instanceof Aggregate)) {
                return null;
            }
            inner.aggregate = (Aggregate) expressions.get(0);
            if (inner.aggregate.isDistinct()) {
                return null;
            }
            AggregateType aggType = inner.aggregate.getAggregateType();
            if (aggType != AggregateType.COUNT_ALL && aggType != AggregateType.COUNT
                    && aggType != AggregateType.SUM && aggType != AggregateType.MIN
                    && aggType != AggregateType.MAX && aggType != AggregateType.AVG) {
                return null;
            }
            inner.aggCol = -1;
            if (aggType != AggregateType.COUNT_ALL) {
                Expression[] args = H2AggregateAccess.getArgs(inner.aggregate);
                if (args == null || args.length != 1) {
                    return null;
                }
                inner.aggCol = VectorizedFilter.resolveColumn(args[0].getNonAliasExpression(), innerFilter);
                if (inner.aggCol < 0) {
                    return null;
                }
                if (aggType == AggregateType.SUM || aggType == AggregateType.AVG) {
                    // 浮点与十进制列的求和口径交给 H2，这里只整数列
                    int t = innerFilter.getTable().getColumn(inner.aggCol).getType().getValueType();
                    if (t != Value.TINYINT && t != Value.SMALLINT && t != Value.INTEGER && t != Value.BIGINT) {
                        return null;
                    }
                }
            }
        } else {
            if (select.isGroupQuery()) {
                return null;
            }
        }
        // 条件拆解：恰好一个等值关联，其余合取项只引用内层表
        List<Expression> conjuncts = new ArrayList<>();
        flattenAnd(select.getCondition(), conjuncts);
        List<Expression> constConjuncts = new ArrayList<>();
        Expression outerKeyExpr = null;
        int innerKeyCol = -1;
        for (Expression conjunct : conjuncts) {
            if (!VectorizedFilter.referencesFilter(conjunct, outerFilter)) {
                constConjuncts.add(conjunct);
                continue;
            }
            if (outerKeyExpr != null || !(conjunct instanceof Comparison)
                    || ((Comparison) conjunct).getCompareType() != Comparison.EQUAL) {
                return null;
            }
            Expression left = conjunct.getSubexpression(0);
            Expression right = conjunct.getSubexpression(1);
            int lInner = VectorizedFilter.resolveColumn(left, innerFilter);
            int rInner = VectorizedFilter.resolveColumn(right, innerFilter);
            int lOuter = VectorizedFilter.resolveColumn(left, outerFilter);
            int rOuter = VectorizedFilter.resolveColumn(right, outerFilter);
            if (lInner >= 0 && rOuter >= 0) {
                innerKeyCol = lInner;
                outerKeyExpr = right;
            } else if (rInner >= 0 && lOuter >= 0) {
                innerKeyCol = rInner;
                outerKeyExpr = left;
            } else {
                // 关联列不是两侧的普通列引用
                return null;
            }
        }
        if (outerKeyExpr == null) {
            // 没有关联，不在此处处理
            return null;
        }
        inner.innerKeyCol = innerKeyCol;
        inner.outerKeyCol = VectorizedFilter.resolveColumn(outerKeyExpr, outerFilter);
        if (inner.outerKeyCol < 0) {
            return null;
        }
        inner.outerKeyType = outerFilter.getTable().getColumn(inner.outerKeyCol).getType();
        if (!constConjuncts.isEmpty()) {
            Expression tree = constConjuncts.get(0);
            for (int i = 1; i < constConjuncts.size(); i++) {
                tree = new ConditionAndOr(ConditionAndOr.AND, tree, constConjuncts.get(i));
            }
            inner.constFilter = VectorizedFilter.compile(tree, innerFilter, session);
            if (inner.constFilter == null) {
                return null;
            }
        }
        return inner;
    }

    private static void flattenAnd(Expression expr, List<Expression> out) {
        if (expr instanceof ConditionAndOr
                && H2ConditionAccess.getAndOrType((ConditionAndOr) expr) == ConditionAndOr.AND) {
            flattenAnd(expr.getSubexpression(0), out);
            flattenAnd(expr.getSubexpression(1), out);
            return;
        }
        if (expr instanceof ConditionAndOrN
                && H2ConditionAccess.getAndOrType((ConditionAndOrN) expr) == ConditionAndOr.AND) {
            int count = expr.getSubexpressionCount();
            for (int i = 0; i < count; i++) {
                flattenAnd(expr.getSubexpression(i), out);
            }
            return;
        }
        out.add(expr);
    }

    // ------------------------------------------------------------------
    // 预计算
    // ------------------------------------------------------------------

    /** 对内层表做一次分组聚合；键规范化到外层键列类型。失败返回 {@code null}。 */
    private static Map<Object, Value> precomputeAggregate(Inner inner, SessionLocal session) {
        OmapTable table = (OmapTable) inner.innerFilter.getTable();
        CompareMode compareMode = table.getCompareMode();
        AggregateType type = inner.aggregate.getAggregateType();
        TypeInfo resultType = inner.aggregate.getType();
        Map<Object, GroupAgg> groups = new HashMap<>();
        VectorizedSnapshot snapshot = table.getStore().takeVectorizedSnapshot();
        try {
            for (ColumnSegment segment : snapshot.segments) {
                int[] sel = null;
                int selCount;
                if (inner.constFilter != null) {
                    sel = new int[segment.getRowCount()];
                    selCount = inner.constFilter.filterSegment(segment, sel,
                            snapshot.maskedBySegment.get(segment.getSegmentId()), session, compareMode);
                } else {
                    BitSet masked = snapshot.maskedBySegment.get(segment.getSegmentId());
                    if (masked != null && !masked.isEmpty()) {
                        sel = new int[segment.getRowCount()];
                        int valid = 0;
                        for (int r = 0; r < segment.getRowCount(); r++) {
                            if (!masked.get(r)) {
                                sel[valid++] = r;
                            }
                        }
                        selCount = valid;
                    } else {
                        selCount = segment.getRowCount();
                    }
                }
                if (selCount == 0) {
                    continue;
                }
                ColumnVector keyVec = segment.getColumn(inner.innerKeyCol);
                ColumnVector aggVec = inner.aggCol >= 0 ? segment.getColumn(inner.aggCol) : null;
                for (int i = 0; i < selCount; i++) {
                    int idx = sel == null ? i : sel[i];
                    Value k = keyVec.get(idx);
                    if (k == ValueNull.INSTANCE) {
                        continue;
                    }
                    GroupAgg group = groups.get(k);
                    if (group == null) {
                        group = new GroupAgg(type, resultType);
                        groups.put(k, group);
                    }
                    if (aggVec != null) {
                        group.addColumn(aggVec, idx, session);
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
                GroupAgg group = groups.get(k);
                if (group == null) {
                    group = new GroupAgg(type, resultType);
                    groups.put(k, group);
                }
                if (inner.aggCol >= 0) {
                    group.addValue(row[inner.aggCol], session);
                } else {
                    group.count++;
                }
            }
        } catch (RuntimeException e) {
            return null;
        }
        Map<Object, Value> results = new HashMap<>(groups.size() * 2);
        try {
            for (Map.Entry<Object, GroupAgg> entry : groups.entrySet()) {
                results.put(normalizeKey((Value) entry.getKey(), inner.outerKeyType, session),
                        entry.getValue().result());
            }
        } catch (RuntimeException e) {
            return null;
        }
        return results;
    }

    /** 预计算内层关联键集合；键规范化到外层键列类型。失败返回 {@code null}。 */
    private static Set<Object> precomputeKeySet(Inner inner, SessionLocal session) {
        OmapTable table = (OmapTable) inner.innerFilter.getTable();
        CompareMode compareMode = table.getCompareMode();
        TypeInfo keyType = inner.outerKeyType;
        boolean intKeys = keyType.getValueType() == Value.INTEGER;
        boolean longKeys = keyType.getValueType() == Value.BIGINT;
        Set<Object> keys = new HashSet<>();
        VectorizedSnapshot snapshot = table.getStore().takeVectorizedSnapshot();
        try {
            for (ColumnSegment segment : snapshot.segments) {
                int[] sel = null;
                int selCount;
                if (inner.constFilter != null) {
                    sel = new int[segment.getRowCount()];
                    selCount = inner.constFilter.filterSegment(segment, sel,
                            snapshot.maskedBySegment.get(segment.getSegmentId()), session, compareMode);
                } else {
                    BitSet masked = snapshot.maskedBySegment.get(segment.getSegmentId());
                    if (masked != null && !masked.isEmpty()) {
                        sel = new int[segment.getRowCount()];
                        int valid = 0;
                        for (int r = 0; r < segment.getRowCount(); r++) {
                            if (!masked.get(r)) {
                                sel[valid++] = r;
                            }
                        }
                        selCount = valid;
                    } else {
                        selCount = segment.getRowCount();
                    }
                }
                if (selCount == 0) {
                    continue;
                }
                ColumnVector keyVec = segment.getColumn(inner.innerKeyCol);
                if (intKeys && keyVec instanceof IntVector) {
                    int[] raw = ((IntVector) keyVec).rawValues();
                    NullBitmap nulls = keyVec.nulls();
                    for (int i = 0; i < selCount; i++) {
                        int idx = sel == null ? i : sel[i];
                        if (!nulls.isNull(idx)) {
                            keys.add(raw[idx]);
                        }
                    }
                } else if (longKeys && keyVec instanceof LongVector) {
                    long[] raw = ((LongVector) keyVec).rawValues();
                    NullBitmap nulls = keyVec.nulls();
                    for (int i = 0; i < selCount; i++) {
                        int idx = sel == null ? i : sel[i];
                        if (!nulls.isNull(idx)) {
                            keys.add(raw[idx]);
                        }
                    }
                } else {
                    for (int i = 0; i < selCount; i++) {
                        int idx = sel == null ? i : sel[i];
                        Value k = keyVec.get(idx);
                        if (k != ValueNull.INSTANCE) {
                            keys.add(normalizeKey(k, keyType, session));
                        }
                    }
                }
            }
            for (Value[] row : snapshot.extraRows) {
                if (inner.constFilter != null
                        && !inner.constFilter.evalRow(row, session, compareMode)) {
                    continue;
                }
                Value k = row[inner.innerKeyCol];
                if (k != ValueNull.INSTANCE) {
                    keys.add(normalizeKey(k, keyType, session));
                }
            }
        } catch (RuntimeException e) {
            return null;
        }
        return keys;
    }

    /** 把内层键值转换成外层键列类型，并按需装箱成 Integer/Long 以便快速查找。 */
    private static Object normalizeKey(Value key, TypeInfo outerKeyType, SessionLocal session) {
        if (key.getValueType() != outerKeyType.getValueType()) {
            key = key.convertTo(outerKeyType, session);
        }
        switch (outerKeyType.getValueType()) {
        case Value.INTEGER:
            return key.getInt();
        case Value.BIGINT:
            return key.getLong();
        default:
            return key;
        }
    }

    /** 关联键无匹配组时的聚合结果：COUNT 为 0，其余为 NULL。 */
    private static Value emptyAggregateResult(AggregateType type) {
        return type == AggregateType.COUNT || type == AggregateType.COUNT_ALL
                ? ValueBigint.get(0)
                : ValueNull.INSTANCE;
    }

    // ------------------------------------------------------------------
    // 分组聚合累加器
    // ------------------------------------------------------------------

    /**
     * 单组聚合累加器，口径对齐 H2 的 {@code AggregateDataDefault} / {@code AggregateDataAvg} /
     * {@code AggregateDataCount}：SUM 溢出即失败回退；AVG 按结果类型走 double 或
     * {@code BigDecimal} 除法（HALF_DOWN）；MIN/MAX 用 {@code session.compare}。
     */
    private static final class GroupAgg {
        private final AggregateType type;
        private final TypeInfo resultType;
        private long count;
        private long longSum;
        private double doubleSum;
        private BigDecimal decimalSum = BigDecimal.ZERO;
        private Value extreme;

        GroupAgg(AggregateType type, TypeInfo resultType) {
            this.type = type;
            this.resultType = resultType;
        }

        void addColumn(ColumnVector vec, int idx, SessionLocal session) {
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
                // 累加器按结果类型分派（AVG(INT) 的结果是 DOUBLE，不能写进 longSum）
                addSumValue(vec.get(idx));
                return;
            case MIN:
            case MAX:
                addExtreme(vec.get(idx), session);
                return;
            default:
                throw new AssertionError();
            }
        }

        void addValue(Value v, SessionLocal session) {
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
                addExtreme(v, session);
                return;
            default:
                throw new AssertionError();
            }
        }

        private void addExtreme(Value v, SessionLocal session) {
            if (v == ValueNull.INSTANCE) {
                return;
            }
            if (extreme == null) {
                extreme = v;
                return;
            }
            int cmp = session.compare(v, extreme);
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
                return extreme == null ? ValueNull.INSTANCE : extreme;
            case SUM:
                if (count == 0) {
                    return ValueNull.INSTANCE;
                }
                return sumValue();
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

    // ------------------------------------------------------------------
    // 节点实现
    // ------------------------------------------------------------------

    /** 恒真或恒假。 */
    private static final class ConstNode implements VectorizedFilter.Node {
        private final boolean value;

        ConstNode(boolean value) {
            this.value = value;
        }

        @Override
        public int filterSegment(ColumnSegment segment, int[] sel, int selCount, boolean fullRange,
                BitSet masked, SessionLocal session, CompareMode compareMode) {
            if (!value) {
                return 0;
            }
            if (fullRange) {
                for (int i = 0; i < selCount; i++) {
                    sel[i] = i;
                }
            }
            return selCount;
        }

        @Override
        public boolean evalRow(Value[] row, SessionLocal session, CompareMode compareMode) {
            return value;
        }
    }

    /** 半连接：外层键命中预计算键集合则通过。{@code NOT EXISTS} 时取反；NULL 键视为未命中。 */
    private static final class SemiJoinNode implements VectorizedFilter.Node {
        private final int keyCol;
        private final Set<Object> keys;
        private final boolean not;
        private final TypeInfo outerKeyType;

        SemiJoinNode(int keyCol, Set<Object> keys, boolean not, TypeInfo outerKeyType) {
            this.keyCol = keyCol;
            this.keys = keys;
            this.not = not;
            this.outerKeyType = outerKeyType;
        }

        @Override
        public int filterSegment(ColumnSegment segment, int[] sel, int selCount, boolean fullRange,
                BitSet masked, SessionLocal session, CompareMode compareMode) {
            ColumnVector keyVec = segment.getColumn(keyCol);
            int out = 0;
            if (keyVec instanceof IntVector) {
                int[] raw = ((IntVector) keyVec).rawValues();
                NullBitmap nulls = keyVec.nulls();
                if (fullRange) {
                    for (int i = 0; i < selCount; i++) {
                        if (match(nulls.isNull(i) ? null : raw[i])) {
                            sel[out++] = i;
                        }
                    }
                } else {
                    for (int i = 0; i < selCount; i++) {
                        int idx = sel[i];
                        if (match(nulls.isNull(idx) ? null : raw[idx])) {
                            sel[out++] = idx;
                        }
                    }
                }
                return out;
            }
            if (keyVec instanceof LongVector) {
                long[] raw = ((LongVector) keyVec).rawValues();
                NullBitmap nulls = keyVec.nulls();
                if (fullRange) {
                    for (int i = 0; i < selCount; i++) {
                        if (match(nulls.isNull(i) ? null : raw[i])) {
                            sel[out++] = i;
                        }
                    }
                } else {
                    for (int i = 0; i < selCount; i++) {
                        int idx = sel[i];
                        if (match(nulls.isNull(idx) ? null : raw[idx])) {
                            sel[out++] = idx;
                        }
                    }
                }
                return out;
            }
            if (fullRange) {
                for (int i = 0; i < selCount; i++) {
                    Value k = keyVec.get(i);
                    if (match(k == ValueNull.INSTANCE ? null : normalizeKey(k, outerKeyType, session))) {
                        sel[out++] = i;
                    }
                }
            } else {
                for (int i = 0; i < selCount; i++) {
                    int idx = sel[i];
                    Value k = keyVec.get(idx);
                    if (match(k == ValueNull.INSTANCE ? null : normalizeKey(k, outerKeyType, session))) {
                        sel[out++] = idx;
                    }
                }
            }
            return out;
        }

        private boolean match(Object key) {
            return (key != null && keys.contains(key)) ^ not;
        }

        @Override
        public boolean evalRow(Value[] row, SessionLocal session, CompareMode compareMode) {
            Value k = row[keyCol];
            return match(k == ValueNull.INSTANCE ? null : normalizeKey(k, outerKeyType, session));
        }
    }

    /**
     * 关联聚合比较：外层比较列的值与「按关联键分组的聚合结果」比较。
     * 整数比较列走整数边界快路径（复用 {@code integerBound} 的收敛规则），
     * 其余逐行取 Value 比较。空组取 {@code emptyDefault}。
     */
    private static final class CorrAggCmpNode implements VectorizedFilter.Node {
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
                    && (keyVec instanceof IntVector || keyVec instanceof DictValueVector)) {
                return filterIntFast(keyVec, (IntVector) cmpVec, sel, selCount, fullRange);
            }
            if (fast != null && cmpVec instanceof LongVector
                    && (keyVec instanceof LongVector || keyVec instanceof DictValueVector)) {
                return filterLongFast(keyVec, (LongVector) cmpVec, sel, selCount, fullRange);
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

        /** 整数比较列的快路径；键列是 IntVector 或数值字典。 */
        private int filterIntFast(ColumnVector keyVec, IntVector cmpVec, int[] sel, int selCount,
                boolean fullRange) {
            int[] cmpVals = cmpVec.rawValues();
            long[] cmpBits = nullableBits(cmpVec);
            int[] keyVals = keyVec instanceof IntVector ? ((IntVector) keyVec).rawValues() : null;
            int[] keyCodes = keyVec instanceof DictValueVector ? ((DictValueVector) keyVec).rawCodes() : null;
            int[] dictKeys = keyCodes != null ? dictKeys((DictValueVector) keyVec) : null;
            long[] keyBits = nullableBits(keyVec);
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
                int[] keyCodes, int[] dictKeys, long[] keyBits) {
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
                c = code < 0 ? defaultCmp : fast.get(dictKeys[code]);
            }
            if (c == null) {
                c = defaultCmp;
            }
            return matchFast(cmpVals[idx], c);
        }

        /** 长整数比较列的快路径；键列是 LongVector 或数值字典。 */
        private int filterLongFast(ColumnVector keyVec, LongVector cmpVec, int[] sel, int selCount,
                boolean fullRange) {
            long[] cmpVals = cmpVec.rawValues();
            long[] cmpBits = nullableBits(cmpVec);
            long[] keyVals = keyVec instanceof LongVector ? ((LongVector) keyVec).rawValues() : null;
            int[] keyCodes = keyVec instanceof DictValueVector ? ((DictValueVector) keyVec).rawCodes() : null;
            long[] dictKeys = keyCodes != null ? dictLongKeys((DictValueVector) keyVec) : null;
            long[] keyBits = nullableBits(keyVec);
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
                int[] keyCodes, long[] dictKeys, long[] keyBits) {
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
                c = code < 0 ? defaultCmp : fast.get(dictKeys[code]);
            }
            if (c == null) {
                c = defaultCmp;
            }
            return matchFast(cmpVals[idx], c);
        }

        private static long[] nullableBits(ColumnVector vec) {
            NullBitmap nulls = vec.nulls();
            return nulls.hasNull() ? nulls.rawBits() : null;
        }

        private static int[] dictKeys(DictValueVector dv) {
            Value[] dict = dv.rawDict();
            int size = dv.dictSize();
            int[] keys = new int[size];
            for (int i = 0; i < size; i++) {
                keys[i] = dict[i].getInt();
            }
            return keys;
        }

        private static long[] dictLongKeys(DictValueVector dv) {
            Value[] dict = dv.rawDict();
            int size = dv.dictSize();
            long[] keys = new long[size];
            for (int i = 0; i < size; i++) {
                keys[i] = dict[i].getLong();
            }
            return keys;
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
                target = results.get(normalizeKey(k, outerKeyType, session));
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
                target = results.get(normalizeKey(k, outerKeyType, session));
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

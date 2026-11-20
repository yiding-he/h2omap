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
import org.h2.expression.ExpressionColumn;
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
import org.h2.value.ValueInteger;
import org.h2.value.ValueNull;
import org.h2.value.ValueNumeric;

import com.hyd.h2omap.storage.ColumnSegment;
import com.hyd.h2omap.storage.ColumnarStore.VectorizedSnapshot;
import com.hyd.h2omap.table.OmapTable;
import com.hyd.h2omap.vector.ColumnVector;
import com.hyd.h2omap.vector.DictValueVector;
import com.hyd.h2omap.vector.DictVarcharVector;
import com.hyd.h2omap.vector.DoubleVector;
import com.hyd.h2omap.vector.IntVector;
import com.hyd.h2omap.vector.LongVector;
import com.hyd.h2omap.vector.NullBitmap;

/**
 * 把 {@code WHERE} 里的子查询谓词编译成向量化过滤节点：
 *
 * <ul>
 * <li>不相关 {@code IN (SELECT ...)}：子查询一次求值，折成集合节点；
 * <li>关联 {@code IN (SELECT ...)}：预计算「关联键 → 内层值集合」，外层按键做成员判定；
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
        if (query.getColumnCount() != 1) {
            return null;
        }
        int col = VectorizedFilter.resolveColumn(in.getLeft(), filter);
        if (col < 0) {
            return null;
        }
        if (VectorizedFilter.referencesFilter(query, filter)) {
            return compileCorrelatedIn(not, col, query, filter, session);
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

    /**
     * 编译关联 {@code IN (SELECT y FROM 内层表 WHERE 内层.k = 外层.K [AND 内层常量谓词])}。
     * 内层投影必须是内层表的列引用；预计算「归一化关联键 → 内层值集合」，
     * 外层每行按键取集合做成员判定。不识别返回 {@code null} 交回 H2。
     */
    private static VectorizedFilter.Node compileCorrelatedIn(boolean not, int col, Query query,
            TableFilter filter, SessionLocal session) {
        if (!(query instanceof Select)) {
            return null;
        }
        Select select = (Select) query;
        Inner inner = parseInner(select, filter, session, false);
        if (inner == null) {
            return null;
        }
        List<Expression> exprs = select.getExpressions();
        if (exprs.size() != 1) {
            return null;
        }
        Expression proj = exprs.get(0).getNonAliasExpression();
        if (!(proj instanceof ExpressionColumn)) {
            return null;
        }
        ExpressionColumn projCol = (ExpressionColumn) proj;
        if (projCol.getTableFilter() != inner.innerFilter) {
            return null;
        }
        int innerValCol = projCol.getColumn().getColumnId();
        TypeInfo outerValType = filter.getTable().getColumn(col).getType();
        Map<Object, KeyMembership> sets = precomputeKeyedSets(inner, innerValCol, outerValType, session);
        if (sets == null) {
            return null;
        }
        if (sets.isEmpty()) {
            // 内层没有任何非 NULL 关联键：IN 恒 false，NOT IN 恒 true
            return new ConstNode(not);
        }
        return new InSemiJoinNode(inner.outerKeyCol, col, not, sets, inner.outerKeyType,
                isIntFamily(outerValType.getValueType()));
    }

    private static boolean isIntFamily(int valueType) {
        return valueType == Value.TINYINT || valueType == Value.SMALLINT
                || valueType == Value.INTEGER || valueType == Value.BIGINT;
    }

    /** 预计算「归一化关联键 → 内层值集合」；值转换到外层值列类型，失败返回 {@code null}。 */
    private static Map<Object, KeyMembership> precomputeKeyedSets(Inner inner, int innerValCol,
            TypeInfo outerValType, SessionLocal session) {
        OmapTable table = (OmapTable) inner.innerFilter.getTable();
        CompareMode compareMode = table.getCompareMode();
        TypeInfo keyType = inner.outerKeyType;
        boolean intKeys = keyType.getValueType() == Value.INTEGER;
        boolean longKeys = keyType.getValueType() == Value.BIGINT;
        boolean longValues = isIntFamily(outerValType.getValueType());
        Map<Object, KeyMembership> sets = new HashMap<>();
        VectorizedSnapshot snapshot = table.getStore().takeVectorizedSnapshot();
        try {
            int[] count = new int[1];
            for (ColumnSegment segment : snapshot.segments) {
                int[] sel = innerSelection(inner, segment, snapshot, session, compareMode, count);
                int selCount = count[0];
                if (selCount == 0) {
                    continue;
                }
                ColumnVector keyVec = segment.getColumn(inner.innerKeyCol);
                ColumnVector valVec = segment.getColumn(innerValCol);
                if (intKeys && keyVec instanceof IntVector) {
                    int[] raw = ((IntVector) keyVec).rawValues();
                    NullBitmap nulls = keyVec.nulls();
                    for (int i = 0; i < selCount; i++) {
                        int idx = sel == null ? i : sel[i];
                        if (!nulls.isNull(idx)) {
                            membership(sets, raw[idx], longValues)
                                    .add(valVec.get(idx), outerValType, session);
                        }
                    }
                } else if (longKeys && keyVec instanceof LongVector) {
                    long[] raw = ((LongVector) keyVec).rawValues();
                    NullBitmap nulls = keyVec.nulls();
                    for (int i = 0; i < selCount; i++) {
                        int idx = sel == null ? i : sel[i];
                        if (!nulls.isNull(idx)) {
                            membership(sets, raw[idx], longValues)
                                    .add(valVec.get(idx), outerValType, session);
                        }
                    }
                } else {
                    for (int i = 0; i < selCount; i++) {
                        int idx = sel == null ? i : sel[i];
                        Value k = keyVec.get(idx);
                        if (k == ValueNull.INSTANCE) {
                            continue;
                        }
                        membership(sets, normalizeKey(k, keyType, session), longValues)
                                .add(valVec.get(idx), outerValType, session);
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
                membership(sets, normalizeKey(k, keyType, session), longValues)
                        .add(row[innerValCol], outerValType, session);
            }
        } catch (RuntimeException e) {
            return null;
        }
        return sets;
    }

    private static KeyMembership membership(Map<Object, KeyMembership> sets, Object key,
            boolean longValues) {
        KeyMembership m = sets.get(key);
        if (m == null) {
            m = new KeyMembership(longValues);
            sets.put(key, m);
        }
        return m;
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
        // 组用列表按槽位保存，槽位由键值解析而来；字典列的槽位可以按字典下标一次算好，
        // 内层循环里只剩数组读，不做 Value 哈希。
        Map<Value, Integer> slotOf = new HashMap<>();
        List<GroupAgg> slotGroups = new ArrayList<>();
        List<Value> slotKeys = new ArrayList<>();
        VectorizedSnapshot snapshot = table.getStore().takeVectorizedSnapshot();
        try {
            int[] count = new int[1];
            for (ColumnSegment segment : snapshot.segments) {
                int[] sel = innerSelection(inner, segment, snapshot, session, compareMode, count);
                int selCount = count[0];
                if (selCount == 0) {
                    continue;
                }
                // 组用列表按槽位保存，槽位由键值解析而来；字典列的槽位可以按字典下标一次算好，
                // 内层循环里只剩数组读，不做 Value 哈希。
                ColumnVector keyVec = segment.getColumn(inner.innerKeyCol);
                ColumnVector aggVec = inner.aggCol >= 0 ? segment.getColumn(inner.aggCol) : null;
                int[] dictCodes = dictCodes(keyVec);
                if (dictCodes != null) {
                    Value[] dict = rawDict(keyVec);
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
                results.put(normalizeKey(slotKeys.get(s), inner.outerKeyType, session),
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

    /** 字典向量的行内编码数组；非字典向量返回 {@code null}。 */
    private static int[] dictCodes(ColumnVector vec) {
        if (vec instanceof DictValueVector) {
            return ((DictValueVector) vec).rawCodes();
        }
        if (vec instanceof DictVarcharVector) {
            return ((DictVarcharVector) vec).rawCodes();
        }
        return null;
    }

    /** 字典向量的字典项数组；非字典向量返回 {@code null}。 */
    private static Value[] rawDict(ColumnVector vec) {
        if (vec instanceof DictValueVector) {
            return ((DictValueVector) vec).rawDict();
        }
        if (vec instanceof DictVarcharVector) {
            return ((DictVarcharVector) vec).rawDict();
        }
        return null;
    }



    /** 预计算前共用的段内行选择：应用内层常量谓词与 delta 屏蔽。返回 {@code null} 表示整段可见（此时 {@code countOut[0]} 为段行数），否则返回行下标数组，{@code countOut[0]} 为有效行数。 */
    private static int[] innerSelection(Inner inner, ColumnSegment segment, VectorizedSnapshot snapshot,
            SessionLocal session, CompareMode compareMode, int[] countOut) {
        if (inner.constFilter != null) {
            int[] sel = new int[segment.getRowCount()];
            countOut[0] = inner.constFilter.filterSegment(segment, sel,
                    snapshot.maskedBySegment.get(segment.getSegmentId()), session, compareMode);
            return sel;
        }
        BitSet masked = snapshot.maskedBySegment.get(segment.getSegmentId());
        if (masked != null && !masked.isEmpty()) {
            int[] sel = new int[segment.getRowCount()];
            int valid = 0;
            for (int r = 0; r < segment.getRowCount(); r++) {
                if (!masked.get(r)) {
                    sel[valid++] = r;
                }
            }
            countOut[0] = valid;
            return sel;
        }
        countOut[0] = segment.getRowCount();
        return null;
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
            int[] count = new int[1];
            for (ColumnSegment segment : snapshot.segments) {
                int[] sel = innerSelection(inner, segment, snapshot, session, compareMode, count);
                int selCount = count[0];
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

    private static long[] nullableBits(ColumnVector vec) {
        NullBitmap nulls = vec.nulls();
        return nulls.hasNull() ? nulls.rawBits() : null;
    }

    private static boolean isDict(ColumnVector vec) {
        return vec instanceof DictValueVector || vec instanceof DictVarcharVector;
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
     * {@code BigDecimal} 除法（HALF_DOWN）；MIN/MAX 按数值次序比较。
     *
     * <p>段内列走原始数组，不逐行装箱；{@code extraRows} 走 {@link Value} 路径。
     * MIN/MAX 因此有两个累加器，取值时按类型合并。
     */
    private static final class GroupAgg {
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
        public void collectColumns(java.util.function.IntConsumer sink) {
            sink.accept(keyCol);
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

    /** 单个关联键的内层值集合；{@code hasNull} 记录内层投影里的 NULL（NOT IN 的三值语义需要它）。 */
    private static final class KeyMembership {
        /** 外层值列为整数族时使用，其余为 {@code null}。 */
        final VectorizedFilter.LongSet longs;
        final Set<Value> values;
        boolean hasNull;

        KeyMembership(boolean longValues) {
            this.longs = longValues ? new VectorizedFilter.LongSet(4) : null;
            this.values = longValues ? null : new HashSet<>();
        }

        void add(Value v, TypeInfo outerValType, SessionLocal session) {
            if (v == ValueNull.INSTANCE) {
                hasNull = true;
                return;
            }
            if (v.getValueType() != outerValType.getValueType()) {
                v = v.convertTo(outerValType, session);
            }
            if (longs != null) {
                longs.add(v.getLong());
            } else {
                values.add(v);
            }
        }

        boolean contains(Value v) {
            return longs != null ? longs.contains(v.getLong()) : values.contains(v);
        }
    }

    /**
     * 关联 IN / NOT IN：外层按归一化关联键取内层值集合做成员判定。
     * NULL 键与未命中键都按「内层空集」处理（IN 不命中、NOT IN 命中）；
     * 外层值为 NULL 时两者都不命中；集合含 NULL 且外层值不在集合中时 NOT IN 为
     * UNKNOWN，同样不命中。整数族值列走原始数组快路径，键列支持 IntVector、
     * LongVector 与字典向量（按码预解析集合引用）。
     */
    private static final class InSemiJoinNode implements VectorizedFilter.Node {
        private final int keyCol;
        private final int valCol;
        private final boolean not;
        private final Map<Object, KeyMembership> sets;
        private final TypeInfo outerKeyType;
        private final boolean longValues;

        InSemiJoinNode(int keyCol, int valCol, boolean not, Map<Object, KeyMembership> sets,
                TypeInfo outerKeyType, boolean longValues) {
            this.keyCol = keyCol;
            this.valCol = valCol;
            this.not = not;
            this.sets = sets;
            this.outerKeyType = outerKeyType;
            this.longValues = longValues;
        }

        @Override
        public void collectColumns(java.util.function.IntConsumer sink) {
            sink.accept(keyCol);
            sink.accept(valCol);
        }

        @Override
        public int filterSegment(ColumnSegment segment, int[] sel, int selCount, boolean fullRange,
                BitSet masked, SessionLocal session, CompareMode compareMode) {
            ColumnVector keyVec = segment.getColumn(keyCol);
            ColumnVector valVec = segment.getColumn(valCol);
            boolean fastKey = keyVec instanceof IntVector || keyVec instanceof LongVector
                    || isDict(keyVec);
            if (longValues && fastKey && valVec instanceof IntVector) {
                return filterInt(keyVec, (IntVector) valVec, sel, selCount, fullRange, session);
            }
            if (longValues && fastKey && valVec instanceof LongVector) {
                return filterLong(keyVec, (LongVector) valVec, sel, selCount, fullRange, session);
            }
            int[] keyCodes = dictCodes(keyVec);
            KeyMembership[] dictKm = keyCodes != null ? dictSets(keyVec, session) : null;
            int out = 0;
            if (fullRange) {
                for (int i = 0; i < selCount; i++) {
                    if (matchValue(keyVec, valVec, i, keyCodes, dictKm, session)) {
                        sel[out++] = i;
                    }
                }
            } else {
                for (int i = 0; i < selCount; i++) {
                    int idx = sel[i];
                    if (matchValue(keyVec, valVec, idx, keyCodes, dictKm, session)) {
                        sel[out++] = idx;
                    }
                }
            }
            return out;
        }

        /** 整数值列的快路径；键列是 IntVector、LongVector 或字典向量。 */
        private int filterInt(ColumnVector keyVec, IntVector valVec, int[] sel, int selCount,
                boolean fullRange, SessionLocal session) {
            int[] vals = valVec.rawValues();
            long[] valBits = nullableBits(valVec);
            int[] keyVals = keyVec instanceof IntVector ? ((IntVector) keyVec).rawValues() : null;
            long[] keyValsL = keyVec instanceof LongVector ? ((LongVector) keyVec).rawValues() : null;
            int[] keyCodes = keyVals == null && keyValsL == null ? dictCodes(keyVec) : null;
            KeyMembership[] dictKm = keyCodes != null ? dictSets(keyVec, session) : null;
            long[] keyBits = nullableBits(keyVec);
            int out = 0;
            if (fullRange) {
                for (int i = 0; i < selCount; i++) {
                    if (matchInt(i, vals, valBits, keyVals, keyValsL, keyCodes, dictKm, keyBits)) {
                        sel[out++] = i;
                    }
                }
            } else {
                for (int i = 0; i < selCount; i++) {
                    int idx = sel[i];
                    if (matchInt(idx, vals, valBits, keyVals, keyValsL, keyCodes, dictKm, keyBits)) {
                        sel[out++] = idx;
                    }
                }
            }
            return out;
        }

        private boolean matchInt(int idx, int[] vals, long[] valBits, int[] keyVals, long[] keyValsL,
                int[] keyCodes, KeyMembership[] dictKm, long[] keyBits) {
            if (valBits != null && (valBits[idx >>> 6] & (1L << (idx & 63))) != 0) {
                return false;
            }
            KeyMembership km = membershipAt(idx, keyVals, keyValsL, keyCodes, dictKm, keyBits);
            return matchLong(km, vals[idx]);
        }

        private int filterLong(ColumnVector keyVec, LongVector valVec, int[] sel, int selCount,
                boolean fullRange, SessionLocal session) {
            long[] vals = valVec.rawValues();
            long[] valBits = nullableBits(valVec);
            int[] keyVals = keyVec instanceof IntVector ? ((IntVector) keyVec).rawValues() : null;
            long[] keyValsL = keyVec instanceof LongVector ? ((LongVector) keyVec).rawValues() : null;
            int[] keyCodes = keyVals == null && keyValsL == null ? dictCodes(keyVec) : null;
            KeyMembership[] dictKm = keyCodes != null ? dictSets(keyVec, session) : null;
            long[] keyBits = nullableBits(keyVec);
            int out = 0;
            if (fullRange) {
                for (int i = 0; i < selCount; i++) {
                    if (matchLongRow(i, vals, valBits, keyVals, keyValsL, keyCodes, dictKm, keyBits)) {
                        sel[out++] = i;
                    }
                }
            } else {
                for (int i = 0; i < selCount; i++) {
                    int idx = sel[i];
                    if (matchLongRow(idx, vals, valBits, keyVals, keyValsL, keyCodes, dictKm, keyBits)) {
                        sel[out++] = idx;
                    }
                }
            }
            return out;
        }

        private boolean matchLongRow(int idx, long[] vals, long[] valBits, int[] keyVals,
                long[] keyValsL, int[] keyCodes, KeyMembership[] dictKm, long[] keyBits) {
            if (valBits != null && (valBits[idx >>> 6] & (1L << (idx & 63))) != 0) {
                return false;
            }
            KeyMembership km = membershipAt(idx, keyVals, keyValsL, keyCodes, dictKm, keyBits);
            return matchLong(km, vals[idx]);
        }

        /** NOT IN 的成员判定：见类注释的三值语义。 */
        private boolean matchLong(KeyMembership km, long val) {
            if (not) {
                if (km == null) {
                    return true;
                }
                return !km.longs.contains(val) && !km.hasNull;
            }
            if (km == null) {
                return false;
            }
            return km.longs.contains(val);
        }

        /** 键列是原始数组或字典时的键查找；键行为 NULL 时返回 {@code null}。 */
        private KeyMembership membershipAt(int idx, int[] keyVals, long[] keyValsL,
                int[] keyCodes, KeyMembership[] dictKm, long[] keyBits) {
            if (keyVals != null) {
                if (keyBits != null && (keyBits[idx >>> 6] & (1L << (idx & 63))) != 0) {
                    return null;
                }
                return sets.get(keyVals[idx]);
            }
            if (keyValsL != null) {
                if (keyBits != null && (keyBits[idx >>> 6] & (1L << (idx & 63))) != 0) {
                    return null;
                }
                return sets.get(keyValsL[idx]);
            }
            int code = keyCodes[idx];
            return code < 0 ? null : dictKm[code];
        }

        /** 按字典下标预解析每个码对应的值集合；缺失的码留 {@code null}，按未命中处理。 */
        private KeyMembership[] dictSets(ColumnVector vec, SessionLocal session) {
            Value[] dict = vec instanceof DictValueVector
                    ? ((DictValueVector) vec).rawDict()
                    : ((DictVarcharVector) vec).rawDict();
            KeyMembership[] result = new KeyMembership[dict.length];
            for (int i = 0; i < dict.length; i++) {
                Value v = dict[i];
                result[i] = v == ValueNull.INSTANCE
                        ? null
                        : sets.get(normalizeKey(v, outerKeyType, session));
            }
            return result;
        }

        private boolean matchValue(ColumnVector keyVec, ColumnVector valVec, int idx,
                int[] keyCodes, KeyMembership[] dictKm, SessionLocal session) {
            KeyMembership km;
            if (keyCodes != null) {
                int code = keyCodes[idx];
                km = code < 0 ? null : dictKm[code];
            } else {
                Value k = keyVec.get(idx);
                km = k == ValueNull.INSTANCE ? null
                        : sets.get(normalizeKey(k, outerKeyType, session));
            }
            return matchMembership(km, valVec.get(idx));
        }

        /** IN / NOT IN 的成员判定；三值语义的 UNKNOWN 都按不命中处理。 */
        private boolean matchMembership(KeyMembership km, Value v) {
            if (not) {
                if (km == null) {
                    return true;
                }
                if (v == ValueNull.INSTANCE || km.contains(v)) {
                    return false;
                }
                return !km.hasNull;
            }
            if (km == null || v == ValueNull.INSTANCE) {
                return false;
            }
            return km.contains(v);
        }

        @Override
        public boolean evalRow(Value[] row, SessionLocal session, CompareMode compareMode) {
            Value k = row[keyCol];
            KeyMembership km = k == ValueNull.INSTANCE ? null
                    : sets.get(normalizeKey(k, outerKeyType, session));
            return matchMembership(km, row[valCol]);
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
                    && (keyVec instanceof IntVector || isDict(keyVec))) {
                return filterIntFast(keyVec, (IntVector) cmpVec, sel, selCount, fullRange, session);
            }
            if (fast != null && cmpVec instanceof LongVector
                    && (keyVec instanceof LongVector || isDict(keyVec))) {
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
            long[] cmpBits = nullableBits(cmpVec);
            int[] keyVals = keyVec instanceof IntVector ? ((IntVector) keyVec).rawValues() : null;
            int[] keyCodes = keyVals == null ? dictCodes(keyVec) : null;
            FastCmp[] dictKeys = keyCodes != null ? dictCmps(keyVec, session) : null;
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
            long[] cmpBits = nullableBits(cmpVec);
            long[] keyVals = keyVec instanceof LongVector ? ((LongVector) keyVec).rawValues() : null;
            int[] keyCodes = keyVals == null ? dictCodes(keyVec) : null;
            FastCmp[] dictKeys = keyCodes != null ? dictCmps(keyVec, session) : null;
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
                        : fast.get(normalizeKey(v, outerKeyType, session));
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

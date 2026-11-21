package com.hyd.h2omap.vectorized;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.h2.command.query.Select;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.aggregate.Aggregate;
import org.h2.expression.aggregate.AggregateType;
import org.h2.expression.aggregate.H2AggregateAccess;
import org.h2.expression.condition.ConditionAndOr;
import org.h2.expression.condition.ConditionAndOrN;
import org.h2.expression.condition.Comparison;
import org.h2.expression.condition.H2ConditionAccess;
import org.h2.table.TableFilter;
import org.h2.value.CompareMode;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueNull;

import com.hyd.h2omap.storage.ColumnSegment;
import com.hyd.h2omap.storage.ColumnarStore.VectorizedSnapshot;
import com.hyd.h2omap.table.OmapTable;
import com.hyd.h2omap.vector.ColumnVector;
import com.hyd.h2omap.vector.DictValueVector;
import com.hyd.h2omap.vector.DictVarcharVector;
import com.hyd.h2omap.vector.NullBitmap;

/**
 * 子查询内层形态的解析与各折叠形态共用的读键原语，供 {@link SubqueryFold}、
 * {@link CorrelatedAggFold}、{@link CorrelatedInFold}、{@link CorrelatedExistsFold} 使用。
 *
 * <p>解析接受「单表内层查询 + 恰好一个等值关联 + 其余条件只引用内层表」；共享原语包括
 * 段内行选择（内层常量谓词与 delta 屏蔽）、关联键归一化到外层键列类型、字典向量原始访问。
 * 全部方法在 {@code VectorizedFilter.compile} 编译期调用，不持有存储锁。
 */
final class SubqueryInner {

    private SubqueryInner() {
    }

    /** 内层查询的解析结果。 */
    static final class InnerQuery {
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
    static InnerQuery parseInner(Select select, TableFilter outerFilter, SessionLocal session,
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
        InnerQuery inner = new InnerQuery();
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

    /** 预计算前共用的段内行选择：应用内层常量谓词与 delta 屏蔽。返回 {@code null} 表示整段可见（此时 {@code countOut[0]} 为段行数），否则返回行下标数组，{@code countOut[0]} 为有效行数。 */
    static int[] innerSelection(InnerQuery inner, ColumnSegment segment, VectorizedSnapshot snapshot,
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

    /** 把内层键值转换成外层键列类型，并按需装箱成 Integer/Long 以便快速查找。 */
    static Object normalizeKey(Value key, TypeInfo outerKeyType, SessionLocal session) {
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

    /** 字典向量的行内编码数组；非字典向量返回 {@code null}。 */
    static int[] dictCodes(ColumnVector vec) {
        if (vec instanceof DictValueVector) {
            return ((DictValueVector) vec).rawCodes();
        }
        if (vec instanceof DictVarcharVector) {
            return ((DictVarcharVector) vec).rawCodes();
        }
        return null;
    }

    /** 字典向量的字典项数组；非字典向量返回 {@code null}。 */
    static Value[] rawDict(ColumnVector vec) {
        if (vec instanceof DictValueVector) {
            return ((DictValueVector) vec).rawDict();
        }
        if (vec instanceof DictVarcharVector) {
            return ((DictVarcharVector) vec).rawDict();
        }
        return null;
    }

    /** null 位图的原始 long 数组；列无 null 时返回 {@code null}。 */
    static long[] nullableBits(ColumnVector vec) {
        NullBitmap nulls = vec.nulls();
        return nulls.hasNull() ? nulls.rawBits() : null;
    }

    /** 是否字典向量（数值或字符串）。 */
    static boolean isDict(ColumnVector vec) {
        return vec instanceof DictValueVector || vec instanceof DictVarcharVector;
    }
}

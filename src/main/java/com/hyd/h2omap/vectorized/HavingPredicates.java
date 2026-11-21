package com.hyd.h2omap.vectorized;

import java.util.List;

import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.aggregate.Aggregate;
import org.h2.expression.condition.Comparison;
import org.h2.expression.condition.ConditionAndOr;
import org.h2.expression.condition.ConditionAndOrN;
import org.h2.expression.condition.H2ConditionAccess;
import org.h2.table.TableFilter;
import org.h2.value.CompareMode;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * {@code HAVING} 谓词的编译与求值：把分组键或聚合对常量的比较（及 {@code AND}/{@code OR}
 * 组合）编译成 {@link HavingPredicate}，在分组结果上求值。引用了 SELECT 列表之外的聚合时，
 * 编译期把对应 {@link AggSpec} 追加进聚合列表。只在编译期调用，不持有存储锁。
 */
final class HavingPredicates {

    private HavingPredicates() {
    }

    static HavingPredicate compileHaving(
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
                        tableFilter, groupKeyCols, aggList, session);
            }
            if (left.isConstant()) {
                return buildHavingComparison(right, flipCmp(cmpType), left.getValue(session),
                        tableFilter, groupKeyCols, aggList, session);
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
            List<AggSpec> aggList,
            SessionLocal compileSession) {
        if (constant == ValueNull.INSTANCE) {
            return (groupKeys, aggVals, session, compareMode) -> false;
        }
        if (target instanceof Aggregate) {
            Aggregate agg = (Aggregate) target;
            AggSpec spec = AggSpec.parseAggregate(agg, tableFilter, -1, compileSession);
            if (spec == null) {
                return null;
            }
            int aggIdx = -1;
            for (int i = 0; i < aggList.size(); i++) {
                AggSpec existing = aggList.get(i);
                if (existing.sameKey(spec)) {
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
}

/** 分组结果上的谓词：对「分组键值 + 聚合值」求值。 */
interface HavingPredicate {
    boolean eval(Value[] groupKeys, Value[] aggVals, SessionLocal session, CompareMode compareMode);
}

final class HavingAndOr implements HavingPredicate {
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

final class HavingComparison implements HavingPredicate {
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

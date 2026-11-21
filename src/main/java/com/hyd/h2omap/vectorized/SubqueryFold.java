package com.hyd.h2omap.vectorized;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.h2.command.query.Query;
import org.h2.command.query.Select;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.Subquery;
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
import org.h2.value.ValueNull;

import com.hyd.h2omap.storage.ColumnSegment;

/**
 * 把 {@code WHERE} 里的子查询谓词编译成向量化过滤节点的入口。
 *
 * <ul>
 * <li>不相关 {@code IN (SELECT ...)}：子查询一次求值，折成集合节点（本类）；
 * <li>关联 {@code IN (SELECT ...)}：预计算「关联键 → 内层值集合」（{@link CorrelatedInFold}）；
 * <li>单列等值关联的标量聚合子查询：预计算一次分组聚合（{@link CorrelatedAggFold}）；
 * <li>关联 {@code EXISTS} / {@code NOT EXISTS}：预计算内层关联键集合（{@link CorrelatedExistsFold}）。
 * </ul>
 *
 * 内层形态解析与共享读键原语在 {@link SubqueryInner}。不识别的形态返回 {@code null}，
 * 由上层回退 H2 原路径。语义上等价于「内层查询按关联列分组、空组取聚合的空值」。
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
        return CorrelatedAggFold.compile(left, cmpType, subq, filter, session);
    }

    // ------------------------------------------------------------------
    // IN (SELECT ...)（不相关形态；关联形态交 CorrelatedInFold）
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
            return CorrelatedInFold.compileCorrelatedIn(not, col, query, filter, session);
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
                return new InNode(col, not, toLongSet(values));
            } catch (RuntimeException e) {
                return null;
            }
        }
        return new InNode(col, not, values);
    }

    private static LongSet toLongSet(List<Value> values) {
        LongSet set = new LongSet(values.size());
        for (Value v : values) {
            set.add(v.getLong());
        }
        return set;
    }

    // ------------------------------------------------------------------
    // EXISTS / NOT EXISTS（不相关形态；关联形态交 CorrelatedExistsFold）
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
        return CorrelatedExistsFold.compileCorrelated((Select) query, not,
                filter, session);
    }

    // ------------------------------------------------------------------
    // 节点实现
    // ------------------------------------------------------------------

    /** 恒真或恒假。 */
    static final class ConstNode implements VectorizedFilter.Node {
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
}

/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.vectorized;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.h2.command.query.Query;
import org.h2.command.query.Select;
import org.h2.engine.SessionLocal;
import org.h2.expression.BinaryOperation;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.H2ExpressionAccess;
import org.h2.expression.Operation1;
import org.h2.expression.OperationN;
import org.h2.expression.Subquery;
import org.h2.expression.ValueExpression;
import org.h2.expression.condition.CompareLike;
import org.h2.expression.condition.Comparison;
import org.h2.expression.condition.ConditionAndOr;
import org.h2.expression.condition.ConditionAndOrN;
import org.h2.expression.condition.ConditionInConstantSet;
import org.h2.expression.condition.ConditionInList;
import org.h2.expression.condition.ConditionNot;
import org.h2.expression.condition.H2ConditionAccess;
import org.h2.expression.condition.NullPredicate;
import org.h2.expression.function.ArrayFunction;
import org.h2.expression.function.CardinalityExpression;
import org.h2.table.TableFilter;
import org.h2.value.CompareMode;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueCollectionBase;
import org.h2.value.ValueNull;
import org.h2.value.ValueNumeric;
import org.h2.value.ValueTimestamp;

import com.hyd.h2omap.stats.ZoneMap;
import com.hyd.h2omap.storage.ColumnSegment;
import com.hyd.h2omap.vector.ArrayVector;
import com.hyd.h2omap.vector.ColumnVector;
import com.hyd.h2omap.vector.DictValueVector;
import com.hyd.h2omap.vector.DictVarcharVector;
import com.hyd.h2omap.vector.DoubleVector;
import com.hyd.h2omap.vector.IntVector;
import com.hyd.h2omap.vector.LongVector;
import com.hyd.h2omap.vector.NullBitmap;
import com.hyd.h2omap.vector.TimestampVector;
import com.hyd.h2omap.vector.VarcharVector;

/**
 * 将 H2 单表过滤条件编译为可在 {@link ColumnSegment} 原始数组上运行的向量化过滤树。
 * 遇到不支持的表达式时 {@link #compile} 返回 {@code null}，由上层无缝回退 H2 原路径。
 */
public final class VectorizedFilter {

    public interface Node {

        /**
         * 对段内候选行做过滤。
         *
         * @param segment     当前段
         * @param sel         选中行下标数组；若 {@code fullRange} 为 true 则无需预填充 {@code 0..selCount-1}
         * @param selCount    候选行数（{@code fullRange == true} 时等于 {@code segment.getRowCount()}）
         * @param fullRange   是否为全段无屏蔽的连续区间 {@code [0, selCount)}
         * @param masked      若 {@code fullRange == false} 且首次未过滤时用于屏蔽 delta 删除/更新行；可为 null
         * @param session     H2 会话
         * @param compareMode 比较模式
         * @return 过滤后的命中行数，结果写入 {@code sel[0..return-1]}
         */
        int filterSegment(ColumnSegment segment, int[] sel, int selCount, boolean fullRange,
                BitSet masked, SessionLocal session, CompareMode compareMode);

        /** 对 delta/开放段的行数组做逐行过滤。 */
        boolean evalRow(Value[] row, SessionLocal session, CompareMode compareMode);

        /**
         * 收集本节点给出的列值区间，供存储层按段剪枝。
         *
         * <p>只有顶层 {@code AND} 的合取项参与：区间取宽不取窄，漏剪只是少省一点，
         * 剪错会丢行。因此 {@code OR}、{@code IN}、{@code <>}、列上算术等形态保持空实现。
         *
         * @param lower 每列下界，{@code null} 元素表示该列无下界
         * @param upper 每列上界，{@code null} 元素表示该列无上界
         */
        default void collectBounds(Value[] lower, Value[] upper) {
        }
    }

    private final Node root;

    private VectorizedFilter(Node root) {
        this.root = root;
    }

    public int filterSegment(ColumnSegment segment, int[] sel, BitSet masked,
            SessionLocal session, CompareMode compareMode) {
        int count = segment.getRowCount();
        if (masked == null || masked.isEmpty()) {
            return root.filterSegment(segment, sel, count, true, null, session, compareMode);
        }
        int valid = 0;
        for (int i = 0; i < count; i++) {
            if (!masked.get(i)) {
                sel[valid++] = i;
            }
        }
        if (valid == 0) {
            return 0;
        }
        return root.filterSegment(segment, sel, valid, false, null, session, compareMode);
    }

    public boolean evalRow(Value[] row, SessionLocal session, CompareMode compareMode) {
        return root.evalRow(row, session, compareMode);
    }

    /**
     * 收集顶层合取项给出的列值区间。
     *
     * @param columnCount 表的列数
     * @return 长度为 2 的数组：{@code [0]} 是每列下界、{@code [1]} 是每列上界，
     *         元素为 {@code null} 表示该侧无界
     */
    public Value[][] segmentBounds(int columnCount) {
        Value[] lower = new Value[columnCount];
        Value[] upper = new Value[columnCount];
        root.collectBounds(lower, upper);
        return new Value[][] { lower, upper };
    }

    /** 编译过滤表达式；不支持时返回 {@code null}。 */
    public static VectorizedFilter compile(Expression expr, TableFilter tableFilter, SessionLocal session) {
        if (expr == null) {
            return null;
        }
        Node node = compileNode(expr, tableFilter, session);
        return node == null ? null : new VectorizedFilter(node);
    }

    private static Node compileNode(Expression expr, TableFilter filter, SessionLocal session) {
        if (expr instanceof ConditionAndOr) {
            ConditionAndOr andOr = (ConditionAndOr) expr;
            int type = H2ConditionAccess.getAndOrType(andOr);
            if (type == ConditionAndOr.AND) {
                List<Node> children = new ArrayList<>();
                if (!collectAndNodes(expr, filter, session, children)) {
                    return null;
                }
                return new AndNode(children.toArray(new Node[0]));
            }
            if (type == ConditionAndOr.OR) {
                Node left = compileNode(andOr.getSubexpression(0), filter, session);
                Node right = compileNode(andOr.getSubexpression(1), filter, session);
                if (left == null || right == null) {
                    return null;
                }
                return new OrNode(left, right);
            }
            return null;
        }
        if (expr instanceof ConditionAndOrN) {
            ConditionAndOrN andOrN = (ConditionAndOrN) expr;
            int type = H2ConditionAccess.getAndOrType(andOrN);
            if (type == ConditionAndOr.AND) {
                List<Node> children = new ArrayList<>();
                if (!collectAndNodes(expr, filter, session, children)) {
                    return null;
                }
                return new AndNode(children.toArray(new Node[0]));
            }
            if (type == ConditionAndOr.OR) {
                int subCount = andOrN.getSubexpressionCount();
                Node acc = compileNode(andOrN.getSubexpression(0), filter, session);
                if (acc == null) {
                    return null;
                }
                for (int i = 1; i < subCount; i++) {
                    Node next = compileNode(andOrN.getSubexpression(i), filter, session);
                    if (next == null) {
                        return null;
                    }
                    acc = new OrNode(acc, next);
                }
                return acc;
            }
            return null;
        }
        if (expr instanceof Comparison) {
            return compileComparison((Comparison) expr, filter, session);
        }
        if (expr instanceof NullPredicate) {
            NullPredicate np = (NullPredicate) expr;
            if (np.isWhenConditionOperand()) {
                return null;
            }
            Expression arg = H2ConditionAccess.getSimpleLeft(np);
            int col = resolveColumn(arg, filter);
            if (col < 0) {
                return null;
            }
            return new NullCheckNode(col, H2ConditionAccess.isNot(np));
        }
        if (expr instanceof ConditionInConstantSet || expr instanceof ConditionInList) {
            int col = resolveColumn(H2ConditionAccess.getInLeft(expr), filter);
            if (col < 0) {
                return null;
            }
            List<Expression> valExprs = H2ConditionAccess.getInValues(expr);
            List<Value> vals = new ArrayList<>(valExprs.size());
            org.h2.value.TypeInfo colType = filter.getTable().getColumn(col).getType();
            for (Expression ve : valExprs) {
                if (!ve.isConstant()) {
                    return null;
                }
                Value v = ve.getValue(session);
                if (v == ValueNull.INSTANCE) {
                    return null;
                }
                try {
                    v = v.convertTo(colType, session);
                } catch (RuntimeException ignored) {
                    return null;
                }
                vals.add(v);
            }
            return new InNode(col, H2ConditionAccess.isInNot(expr), vals);
        }
        if (expr instanceof CompareLike) {
            return compileLike((CompareLike) expr, filter, session);
        }
        if (isArrayContains(expr)) {
            return compileArrayContains((ArrayFunction) expr, false, filter, session);
        }
        if (expr instanceof ConditionNot) {
            Expression inner = expr.getSubexpression(0);
            if (isArrayContains(inner)) {
                return compileArrayContains((ArrayFunction) inner, true, filter, session);
            }
            return null;
        }
        return SubqueryFold.compileNode(expr, filter, session);
    }

    private static boolean isArrayContains(Expression expr) {
        return expr instanceof ArrayFunction
                && "ARRAY_CONTAINS".equals(((ArrayFunction) expr).getName());
    }

    /**
     * 编译 {@code ARRAY_CONTAINS(数组列, 常量)}；{@code negate} 为 {@code true} 表示外面包了一层 {@code NOT}。
     *
     * <p>语义按 H2 的 {@code ArrayFunction.ARRAY_CONTAINS}：数组为 NULL 时结果是 NULL（两种形式都不命中），
     * 否则只要有一个元素与目标值相等（NULL 与 NULL 视为相等）即为 TRUE，没有则为 FALSE。
     */
    private static Node compileArrayContains(ArrayFunction fn, boolean negate,
            TableFilter filter, SessionLocal session) {
        Expression[] args = H2ExpressionAccess.getArgs(fn);
        if (args.length != 2) {
            return null;
        }
        int col = resolveColumn(args[0], filter);
        if (col < 0 || !args[1].isConstant()) {
            return null;
        }
        int columnType = filter.getTable().getColumn(col).getType().getValueType();
        if (columnType != Value.ARRAY && columnType != Value.ROW) {
            return null;
        }
        Value target = args[1].getValue(session);
        if (target == null) {
            return null;
        }
        return new ArrayContainsNode(col, target, negate);
    }

    private static Node compileLike(CompareLike like, TableFilter filter, SessionLocal session) {
        if (like.isWhenConditionOperand() || like.getSubexpressionCount() > 3) {
            return null;
        }
        Expression left = like.getSubexpression(0);
        int col = resolveColumn(left, filter);
        if (col < 0) {
            return null;
        }
        int colType = filter.getTable().getColumn(col).getType().getValueType();
        if (colType != Value.VARCHAR && colType != Value.CHAR) {
            return null;
        }
        Expression right = like.getSubexpression(1);
        if (!right.isConstant()) {
            return null;
        }
        Value patVal = right.getValue(session);
        if (patVal == null || patVal == ValueNull.INSTANCE) {
            return null;
        }
        char escapeChar = '\\';
        if (like.getSubexpressionCount() == 3) {
            Expression escExpr = like.getSubexpression(2);
            if (!escExpr.isConstant()) {
                return null;
            }
            Value escVal = escExpr.getValue(session);
            if (escVal == null || escVal == ValueNull.INSTANCE) {
                return null;
            }
            String escStr = escVal.getString();
            escapeChar = escStr.isEmpty() ? (char) 0 : escStr.charAt(0);
        } else {
            String defEsc = session.getDatabase().getSettings().defaultEscape;
            escapeChar = (defEsc == null || defEsc.isEmpty()) ? (char) 0 : defEsc.charAt(0);
        }
        String leftSql = left.getUnenclosedSQL(new StringBuilder(), 0).toString();
        String fullSql = like.getUnenclosedSQL(new StringBuilder(), 0).toString();
        String opPart = fullSql.length() > leftSql.length() ? fullSql.substring(leftSql.length()) : "";
        boolean negate;
        if (opPart.startsWith(" NOT LIKE ")) {
            negate = true;
        } else if (opPart.startsWith(" LIKE ")) {
            negate = false;
        } else {
            // ILIKE / REGEXP 等其他形态交回 H2 原路径
            return null;
        }
        String pattern = patVal.getString();
        try {
            like.test(pattern, "", escapeChar);
        } catch (RuntimeException e) {
            return null;
        }
        return new LikeNode(col, like, pattern, escapeChar, negate);
    }

    private static boolean collectAndNodes(Expression expr, TableFilter filter,
            SessionLocal session, List<Node> out) {
        if (expr instanceof ConditionAndOr
                && H2ConditionAccess.getAndOrType((ConditionAndOr) expr) == ConditionAndOr.AND) {
            return collectAndNodes(expr.getSubexpression(0), filter, session, out)
                    && collectAndNodes(expr.getSubexpression(1), filter, session, out);
        }
        if (expr instanceof ConditionAndOrN
                && H2ConditionAccess.getAndOrType((ConditionAndOrN) expr) == ConditionAndOr.AND) {
            int subCount = expr.getSubexpressionCount();
            for (int i = 0; i < subCount; i++) {
                if (!collectAndNodes(expr.getSubexpression(i), filter, session, out)) {
                    return false;
                }
            }
            return true;
        }
        Node n = compileNode(expr, filter, session);
        if (n == null) {
            return false;
        }
        out.add(n);
        return true;
    }

    private static Node compileComparison(Comparison cmp, TableFilter filter, SessionLocal session) {
        int cmpType = cmp.getCompareType();
        if (cmpType < Comparison.EQUAL || cmpType > Comparison.BIGGER_EQUAL) {
            return null;
        }
        Expression left = cmp.getSubexpression(0);
        Expression right = cmp.getSubexpression(1);

        if (isFoldableConstant(right, filter)) {
            return compileLeftWithConst(left, cmpType, right.getValue(session), filter, session);
        }
        if (isFoldableConstant(left, filter)) {
            return compileLeftWithConst(right, flipCompare(cmpType), left.getValue(session), filter, session);
        }
        if (right instanceof Subquery) {
            Node n = SubqueryFold.compileCorrelatedCmp(left, cmpType, (Subquery) right, filter, session);
            if (n != null) {
                return n;
            }
        }
        if (left instanceof Subquery) {
            Node n = SubqueryFold.compileCorrelatedCmp(right, flipCompare(cmpType), (Subquery) left, filter, session);
            if (n != null) {
                return n;
            }
        }
        return null;
    }

    /**
     * 编译期只求值一次的表达式：字面常量，以及不引用被扫描表的不相关子查询。
     *
     * <p>相关子查询的值随外层行变化，必须交回 H2 逐行求值。
     */
    private static boolean isFoldableConstant(Expression expr, TableFilter filter) {
        if (expr.isConstant()) {
            return true;
        }
        return expr instanceof Subquery
                && !referencesFilter(((Subquery) expr).getQuery(), filter);
    }

    /** 查询是否引用指定过滤表的列，包括嵌套子查询里的引用。 */
    static boolean referencesFilter(Query query, TableFilter filter) {
        List<Expression> expressions = query.getExpressions();
        for (int i = 0; i < expressions.size(); i++) {
            if (referencesFilter(expressions.get(i), filter)) {
                return true;
            }
        }
        if (!(query instanceof Select)) {
            return false;
        }
        Select select = (Select) query;
        // ORDER BY 不参与 select 列表，单独判定；GROUP BY 表达式在 prepare 阶段
        // 已并入 select 列表，不需单独遍历
        if (select.getSortOrder() != null) {
            return true;
        }
        if (referencesFilter(select.getCondition(), filter)
                || referencesFilter(select.getHaving(), filter)) {
            return true;
        }
        for (TableFilter tableFilter : select.getTableFilters()) {
            if (referencesFilter(tableFilter.getJoinCondition(), filter)
                    || referencesFilter(tableFilter.getFilterCondition(), filter)) {
                return true;
            }
        }
        return false;
    }

    static boolean referencesFilter(Expression expr, TableFilter filter) {
        if (expr == null) {
            return false;
        }
        if (expr instanceof ExpressionColumn) {
            return ((ExpressionColumn) expr).getTableFilter() == filter;
        }
        if (expr instanceof Subquery) {
            return referencesFilter(((Subquery) expr).getQuery(), filter);
        }
        int count = expr.getSubexpressionCount();
        for (int i = 0; i < count; i++) {
            if (referencesFilter(expr.getSubexpression(i), filter)) {
                return true;
            }
        }
        return false;
    }

    private static Node compileLeftWithConst(Expression left, int cmpType, Value constant,
            TableFilter filter, SessionLocal session) {
        if (constant == null || constant == ValueNull.INSTANCE) {
            return null;
        }
        int col = resolveColumn(left, filter);
        int[] cmp = { cmpType };
        Value bound = integerBound(constant, filter.getTable().getColumn(col >= 0 ? col : 0).getType(), cmp);
        if (bound == null) {
            return null;
        }
        cmpType = cmp[0];
        Value converted;
        try {
            converted = bound.convertTo(filter.getTable().getColumn(col >= 0 ? col : 0).getType(), session);
        } catch (RuntimeException ignored) {
            converted = bound;
        }
        if (col >= 0) {
            return new ColCmpNode(col, cmpType, converted);
        }
        if (left instanceof BinaryOperation) {
            BinaryOperation bin = (BinaryOperation) left;
            BinaryOperation.OpType op = bin.getOperationType();
            if (op == BinaryOperation.OpType.PLUS
                    || op == BinaryOperation.OpType.MINUS
                    || op == BinaryOperation.OpType.MULTIPLY) {
                Expression l = H2ExpressionAccess.getLeft(bin);
                Expression r = H2ExpressionAccess.getRight(bin);
                int binCol = resolveColumn(l, filter);
                if (binCol >= 0 && r.isConstant()) {
                    Value arithVal = r.getValue(session);
                    if (arithVal == null || arithVal == ValueNull.INSTANCE) {
                        return null;
                    }
                    int valType = filter.getTable().getColumn(binCol).getType().getValueType();
                    if (valType == Value.TINYINT || valType == Value.SMALLINT
                            || valType == Value.INTEGER || valType == Value.BIGINT) {
                        return new ArithLongCmpNode(binCol, op, arithVal.getLong(), cmpType, constant.getLong());
                    }
                }
            }
        }
        if (left instanceof CardinalityExpression) {
            CardinalityExpression ce = (CardinalityExpression) left;
            String sql = ce.getUnenclosedSQL(new StringBuilder(), 0).toString();
            if (sql.startsWith("CARDINALITY(")) {
                int arrCol = resolveColumn(H2ExpressionAccess.getArg((Operation1) ce), filter);
                if (arrCol >= 0) {
                    return new CardinalityCmpNode(arrCol, cmpType, constant.getInt());
                }
            }
        }
        return null;
    }

    /**
     * 整型列与带小数的常量比较时，把常量收敛到整数边界并相应调整比较类型。
     *
     * <p>{@code convertTo} 对带小数的常量做四舍五入，会把 {@code INT_COL > 499999.5}
     * 变成 {@code INT_COL > 500000}，结果集不同。
     *
     * @return 调整后的常量；用整数边界表达不了该关系时返回 {@code null}
     */
    static Value integerBound(Value constant, TypeInfo columnType, int[] cmpType) {
        int type = columnType.getValueType();
        if (type != Value.TINYINT && type != Value.SMALLINT
                && type != Value.INTEGER && type != Value.BIGINT) {
            return constant;
        }
        BigDecimal decimal;
        try {
            decimal = constant.getBigDecimal();
        } catch (RuntimeException ignored) {
            return constant;
        }
        if (decimal == null || decimal.stripTrailingZeros().scale() <= 0) {
            return constant;
        }
        switch (cmpType[0]) {
        case Comparison.BIGGER:
            cmpType[0] = Comparison.BIGGER_EQUAL;
            return ValueNumeric.get(decimal.setScale(0, RoundingMode.CEILING));
        case Comparison.BIGGER_EQUAL:
            return ValueNumeric.get(decimal.setScale(0, RoundingMode.CEILING));
        case Comparison.SMALLER:
            cmpType[0] = Comparison.SMALLER_EQUAL;
            return ValueNumeric.get(decimal.setScale(0, RoundingMode.FLOOR));
        case Comparison.SMALLER_EQUAL:
            return ValueNumeric.get(decimal.setScale(0, RoundingMode.FLOOR));
        default:
            return null;
        }
    }

    /**
     * 记录某列的边界值。同一列已有边界时保持原值不动，即区间取宽不取窄。
     *
     * <p>合取项中的每一个都是合法区间，因此只保留其中一个也不会漏掉任何命中的行，
     * 只是多保留一段更宽的区间、少省一点。这样不需要比较两个常量，
     * 也就不依赖类型的比较模式。
     *
     * @param bounds 每列边界数组
     * @param column 列号
     * @param value  候选边界值
     */
    static void setBound(Value[] bounds, int column, Value value) {
        if (bounds == null || column < 0 || column >= bounds.length || value == null
                || value == ValueNull.INSTANCE) {
            return;
        }
        Value current = bounds[column];
        if (current == null || current == ValueNull.INSTANCE) {
            bounds[column] = value;
        }
    }

    static int resolveColumn(Expression expr, TableFilter filter) {
        if (expr instanceof ExpressionColumn) {
            ExpressionColumn ec = (ExpressionColumn) expr;
            if (ec.getTableFilter() == filter) {
                return ec.getColumn().getColumnId();
            }
        }
        return -1;
    }

    private static int flipCompare(int cmpType) {
        switch (cmpType) {
        case Comparison.SMALLER:
            return Comparison.BIGGER;
        case Comparison.BIGGER:
            return Comparison.SMALLER;
        case Comparison.SMALLER_EQUAL:
            return Comparison.BIGGER_EQUAL;
        case Comparison.BIGGER_EQUAL:
            return Comparison.SMALLER_EQUAL;
        default:
            return cmpType;
        }
    }

    static boolean matchCompare(int cmp, int cmpType) {
        switch (cmpType) {
        case Comparison.EQUAL:
            return cmp == 0;
        case Comparison.NOT_EQUAL:
            return cmp != 0;
        case Comparison.SMALLER:
            return cmp < 0;
        case Comparison.BIGGER:
            return cmp > 0;
        case Comparison.SMALLER_EQUAL:
            return cmp <= 0;
        case Comparison.BIGGER_EQUAL:
            return cmp >= 0;
        default:
            return false;
        }
    }

    // ------------------------------------------------------------------
    // 算子实现
    // ------------------------------------------------------------------

    private static final class AndNode implements Node {
        private final Node[] children;

        AndNode(Node[] children) {
            this.children = children;
        }

        @Override
        public int filterSegment(ColumnSegment segment, int[] sel, int selCount, boolean fullRange,
                BitSet masked, SessionLocal session, CompareMode compareMode) {
            int count = selCount;
            boolean full = fullRange;
            for (Node child : children) {
                count = child.filterSegment(segment, sel, count, full, null, session, compareMode);
                if (count == 0) {
                    return 0;
                }
                full = false;
            }
            return count;
        }

        @Override
        public boolean evalRow(Value[] row, SessionLocal session, CompareMode compareMode) {
            for (Node child : children) {
                if (!child.evalRow(row, session, compareMode)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public void collectBounds(Value[] lower, Value[] upper) {
            for (Node child : children) {
                child.collectBounds(lower, upper);
            }
        }
    }

    private static final class OrNode implements Node {
        private final Node left;
        private final Node right;

        OrNode(Node left, Node right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public int filterSegment(ColumnSegment segment, int[] sel, int selCount, boolean fullRange,
                BitSet masked, SessionLocal session, CompareMode compareMode) {
            int[] leftSel = new int[selCount];
            if (!fullRange) {
                System.arraycopy(sel, 0, leftSel, 0, selCount);
            }
            int leftCount = left.filterSegment(segment, leftSel, selCount, fullRange, null, session, compareMode);
            if (leftCount == selCount) {
                if (fullRange) {
                    for (int i = 0; i < selCount; i++) {
                        sel[i] = i;
                    }
                }
                return selCount;
            }
            BitSet hit = new BitSet(segment.getRowCount());
            for (int i = 0; i < leftCount; i++) {
                hit.set(leftSel[i]);
            }
            int[] rightInput = new int[selCount - leftCount];
            int rLen = 0;
            if (fullRange) {
                for (int i = 0; i < selCount; i++) {
                    if (!hit.get(i)) {
                        rightInput[rLen++] = i;
                    }
                }
            } else {
                for (int i = 0; i < selCount; i++) {
                    int idx = sel[i];
                    if (!hit.get(idx)) {
                        rightInput[rLen++] = idx;
                    }
                }
            }
            int rightCount = right.filterSegment(segment, rightInput, rLen, false, null, session, compareMode);
            for (int i = 0; i < rightCount; i++) {
                hit.set(rightInput[i]);
            }
            int out = 0;
            if (fullRange) {
                for (int i = hit.nextSetBit(0); i >= 0; i = hit.nextSetBit(i + 1)) {
                    sel[out++] = i;
                }
            } else {
                for (int i = 0; i < selCount; i++) {
                    int idx = sel[i];
                    if (hit.get(idx)) {
                        sel[out++] = idx;
                    }
                }
            }
            return out;
        }

        @Override
        public boolean evalRow(Value[] row, SessionLocal session, CompareMode compareMode) {
            return left.evalRow(row, session, compareMode)
                    || right.evalRow(row, session, compareMode);
        }
    }

    private static final class ColCmpNode implements Node {
        private final int col;
        private final int cmpType;
        private final Value constant;

        ColCmpNode(int col, int cmpType, Value constant) {
            this.col = col;
            this.cmpType = cmpType;
            this.constant = constant;
        }

        @Override
        public void collectBounds(Value[] lower, Value[] upper) {
            switch (cmpType) {
            case Comparison.EQUAL:
                setBound(lower, col, constant);
                setBound(upper, col, constant);
                break;
            case Comparison.BIGGER:
            case Comparison.BIGGER_EQUAL:
                setBound(lower, col, constant);
                break;
            case Comparison.SMALLER:
            case Comparison.SMALLER_EQUAL:
                setBound(upper, col, constant);
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
                return filterDict(dv.rawCodes(), dv.rawDict(), dv.dictSize(), sel, selCount, fullRange,
                        session, compareMode);
            }
            if (vec instanceof DictValueVector) {
                DictValueVector dv = (DictValueVector) vec;
                return filterDict(dv.rawCodes(), dv.rawDict(), dv.dictSize(), sel, selCount, fullRange,
                        session, compareMode);
            }
            return filterGeneric(vec, sel, selCount, fullRange, session, compareMode);
        }

        private int filterInt(IntVector vec, int[] sel, int selCount, boolean fullRange) {
            int[] vals = vec.rawValues();
            NullBitmap nulls = vec.nulls();
            boolean hasNull = nulls.hasNull();
            long target = constant.getLong();
            int out = 0;
            if (fullRange) {
                if (!hasNull) {
                    for (int i = 0; i < selCount; i++) {
                        if (matchLong(vals[i], target)) {
                            sel[out++] = i;
                        }
                    }
                } else {
                    long[] bits = nulls.rawBits();
                    for (int i = 0; i < selCount; i++) {
                        if ((bits[i >>> 6] & (1L << i)) == 0 && matchLong(vals[i], target)) {
                            sel[out++] = i;
                        }
                    }
                }
            } else {
                if (!hasNull) {
                    for (int i = 0; i < selCount; i++) {
                        int idx = sel[i];
                        if (matchLong(vals[idx], target)) {
                            sel[out++] = idx;
                        }
                    }
                } else {
                    long[] bits = nulls.rawBits();
                    for (int i = 0; i < selCount; i++) {
                        int idx = sel[i];
                        if ((bits[idx >>> 6] & (1L << idx)) == 0 && matchLong(vals[idx], target)) {
                            sel[out++] = idx;
                        }
                    }
                }
            }
            return out;
        }

        private int filterLong(LongVector vec, int[] sel, int selCount, boolean fullRange) {
            long[] vals = vec.rawValues();
            NullBitmap nulls = vec.nulls();
            boolean hasNull = nulls.hasNull();
            long target = constant.getLong();
            int out = 0;
            if (fullRange) {
                for (int i = 0; i < selCount; i++) {
                    if ((!hasNull || !nulls.isNull(i)) && matchLong(vals[i], target)) {
                        sel[out++] = i;
                    }
                }
            } else {
                for (int i = 0; i < selCount; i++) {
                    int idx = sel[i];
                    if ((!hasNull || !nulls.isNull(idx)) && matchLong(vals[idx], target)) {
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

        private int filterDict(int[] codes, Value[] dict, int dictSize,
                int[] sel, int selCount, boolean fullRange, SessionLocal session, CompareMode compareMode) {
            boolean[] allowed = new boolean[dictSize];
            boolean anyAllowed = false;
            for (int d = 0; d < dictSize; d++) {
                int cmp = dict[d].compareTo(constant, session, compareMode);
                if (matchCompare(cmp, cmpType)) {
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
                    int code = codes[i];
                    if (code >= 0 && allowed[code]) {
                        sel[out++] = i;
                    }
                }
            } else {
                for (int i = 0; i < selCount; i++) {
                    int idx = sel[i];
                    int code = codes[idx];
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
                    if (v != ValueNull.INSTANCE && matchCompare(v.compareTo(constant, session, compareMode), cmpType)) {
                        sel[out++] = i;
                    }
                }
            } else {
                for (int i = 0; i < selCount; i++) {
                    int idx = sel[i];
                    Value v = vec.get(idx);
                    if (v != ValueNull.INSTANCE && matchCompare(v.compareTo(constant, session, compareMode), cmpType)) {
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
            return matchCompare(cmp, cmpType);
        }

        private int filterTimestamp(TimestampVector vec, int[] sel, int selCount, boolean fullRange) {
            if (!(constant instanceof ValueTimestamp)) {
                return filterGeneric(vec, sel, selCount, fullRange, null, null);
            }
            ValueTimestamp ts = (ValueTimestamp) constant;
            long targetDate = ts.getDateValue();
            long targetNanos = ts.getTimeNanos();
            long[] dates = vec.rawDateValues();
            long[] nanos = vec.rawTimeNanos();
            NullBitmap nulls = vec.nulls();
            boolean hasNull = nulls.hasNull();
            int out = 0;
            if (fullRange) {
                if (!hasNull) {
                    for (int i = 0; i < selCount; i++) {
                        if (matchTimestamp(dates[i], nanos[i], targetDate, targetNanos)) {
                            sel[out++] = i;
                        }
                    }
                } else {
                    long[] bits = nulls.rawBits();
                    for (int i = 0; i < selCount; i++) {
                        if ((bits[i >>> 6] & (1L << i)) == 0
                                && matchTimestamp(dates[i], nanos[i], targetDate, targetNanos)) {
                            sel[out++] = i;
                        }
                    }
                }
            } else {
                if (!hasNull) {
                    for (int i = 0; i < selCount; i++) {
                        int idx = sel[i];
                        if (matchTimestamp(dates[idx], nanos[idx], targetDate, targetNanos)) {
                            sel[out++] = idx;
                        }
                    }
                } else {
                    long[] bits = nulls.rawBits();
                    for (int i = 0; i < selCount; i++) {
                        int idx = sel[i];
                        if ((bits[idx >>> 6] & (1L << idx)) == 0
                                && matchTimestamp(dates[idx], nanos[idx], targetDate, targetNanos)) {
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
            return matchCompare(cmp, cmpType);
        }

        @Override
        public boolean evalRow(Value[] row, SessionLocal session, CompareMode compareMode) {
            Value v = row[col];
            return v != ValueNull.INSTANCE
                    && matchCompare(v.compareTo(constant, session, compareMode), cmpType);
        }
    }

    private static final class ArithLongCmpNode implements Node {
        private final int col;
        private final BinaryOperation.OpType arithOp;
        private final long arithConst;
        private final int cmpType;
        private final long target;

        ArithLongCmpNode(int col, BinaryOperation.OpType arithOp, long arithConst,
                int cmpType, long target) {
            this.col = col;
            this.arithOp = arithOp;
            this.arithConst = arithConst;
            this.cmpType = cmpType;
            this.target = target;
        }

        @Override
        public int filterSegment(ColumnSegment segment, int[] sel, int selCount, boolean fullRange,
                BitSet masked, SessionLocal session, CompareMode compareMode) {
            ColumnVector vec = segment.getColumn(col);
            if (vec instanceof IntVector) {
                IntVector iv = (IntVector) vec;
                int[] vals = iv.rawValues();
                NullBitmap nulls = iv.nulls();
                boolean hasNull = nulls.hasNull();
                int out = 0;
                if (fullRange) {
                    if (!hasNull) {
                        for (int i = 0; i < selCount; i++) {
                            if (matchLong(evalArith(vals[i]))) {
                                sel[out++] = i;
                            }
                        }
                    } else {
                        for (int i = 0; i < selCount; i++) {
                            if (!nulls.isNull(i) && matchLong(evalArith(vals[i]))) {
                                sel[out++] = i;
                            }
                        }
                    }
                } else {
                    for (int i = 0; i < selCount; i++) {
                        int idx = sel[i];
                        if ((!hasNull || !nulls.isNull(idx)) && matchLong(evalArith(vals[idx]))) {
                            sel[out++] = idx;
                        }
                    }
                }
                return out;
            }
            if (vec instanceof LongVector) {
                LongVector lv = (LongVector) vec;
                long[] vals = lv.rawValues();
                NullBitmap nulls = lv.nulls();
                boolean hasNull = nulls.hasNull();
                int out = 0;
                if (fullRange) {
                    for (int i = 0; i < selCount; i++) {
                        if ((!hasNull || !nulls.isNull(i)) && matchLong(evalArith(vals[i]))) {
                            sel[out++] = i;
                        }
                    }
                } else {
                    for (int i = 0; i < selCount; i++) {
                        int idx = sel[i];
                        if ((!hasNull || !nulls.isNull(idx)) && matchLong(evalArith(vals[idx]))) {
                            sel[out++] = idx;
                        }
                    }
                }
                return out;
            }
            int out = 0;
            if (fullRange) {
                for (int i = 0; i < selCount; i++) {
                    Value v = vec.get(i);
                    if (v != ValueNull.INSTANCE && matchLong(evalArith(v.getLong()))) {
                        sel[out++] = i;
                    }
                }
            } else {
                for (int i = 0; i < selCount; i++) {
                    int idx = sel[i];
                    Value v = vec.get(idx);
                    if (v != ValueNull.INSTANCE && matchLong(evalArith(v.getLong()))) {
                        sel[out++] = idx;
                    }
                }
            }
            return out;
        }

        private long evalArith(long v) {
            switch (arithOp) {
            case PLUS:
                return v + arithConst;
            case MINUS:
                return v - arithConst;
            case MULTIPLY:
                return v * arithConst;
            default:
                throw new IllegalStateException("Unexpected op: " + arithOp);
            }
        }

        private boolean matchLong(long v) {
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

        @Override
        public boolean evalRow(Value[] row, SessionLocal session, CompareMode compareMode) {
            Value v = row[col];
            return v != ValueNull.INSTANCE && matchLong(evalArith(v.getLong()));
        }
    }

    /** 原始 long 开放寻址集合，供整数列的 IN 过滤避开 Value 装箱哈希。 */
    static final class LongSet {
        private long[] keys;
        private boolean[] used;
        private int mask;
        private int size;

        LongSet(int expected) {
            int cap = Integer.highestOneBit(Math.max(8, expected * 2 - 1)) << 1;
            keys = new long[cap];
            used = new boolean[cap];
            mask = cap - 1;
        }

        void add(long key) {
            // 固定容量下满槽会让开放寻址死循环；按元素个数扩容，
            // 预估容量的调用方（InNode）永远不会触到阈值，只有逐步增长的调用方（SubqueryFold）会。
            if (size * 4 > mask * 3) {
                rehash();
            }
            int i = (int) (mix(key) & mask);
            while (used[i]) {
                if (keys[i] == key) {
                    return;
                }
                i = (i + 1) & mask;
            }
            used[i] = true;
            keys[i] = key;
            size++;
        }

        boolean contains(long key) {
            int i = (int) (mix(key) & mask);
            while (used[i]) {
                if (keys[i] == key) {
                    return true;
                }
                i = (i + 1) & mask;
            }
            return false;
        }

        private void rehash() {
            long[] oldKeys = keys;
            boolean[] oldUsed = used;
            int cap = (mask + 1) << 1;
            keys = new long[cap];
            used = new boolean[cap];
            mask = cap - 1;
            for (int i = 0; i < oldKeys.length; i++) {
                if (oldUsed[i]) {
                    int j = (int) (mix(oldKeys[i]) & mask);
                    while (used[j]) {
                        j = (j + 1) & mask;
                    }
                    used[j] = true;
                    keys[j] = oldKeys[i];
                }
            }
        }

        private static long mix(long key) {
            long h = key * 0x9E3779B97F4A7C15L;
            return h ^ (h >>> 32);
        }
    }

    static final class InNode implements Node {
        private final int col;
        private final boolean not;
        private final Set<Value> set;
        private final LongSet longSet;

        InNode(int col, boolean not, List<Value> values) {
            this(col, not, new HashSet<>(values), null);
        }

        InNode(int col, boolean not, LongSet longSet) {
            this(col, not, null, longSet);
        }

        private InNode(int col, boolean not, Set<Value> set, LongSet longSet) {
            this.col = col;
            this.not = not;
            this.set = set;
            this.longSet = longSet;
        }

        @Override
        public int filterSegment(ColumnSegment segment, int[] sel, int selCount, boolean fullRange,
                BitSet masked, SessionLocal session, CompareMode compareMode) {
            ColumnVector vec = segment.getColumn(col);
            if (vec instanceof DictVarcharVector) {
                DictVarcharVector dv = (DictVarcharVector) vec;
                return filterDict(dv.rawCodes(), dv.rawDict(), dv.dictSize(), sel, selCount, fullRange);
            }
            if (vec instanceof DictValueVector) {
                DictValueVector dv = (DictValueVector) vec;
                return filterDict(dv.rawCodes(), dv.rawDict(), dv.dictSize(), sel, selCount, fullRange);
            }
            if (longSet != null && vec instanceof IntVector) {
                return filterLongSet(((IntVector) vec).rawValues(), sel, selCount, fullRange);
            }
            if (longSet != null && vec instanceof LongVector) {
                return filterLongSet(((LongVector) vec).rawValues(), sel, selCount, fullRange);
            }
            int out = 0;
            if (fullRange) {
                for (int i = 0; i < selCount; i++) {
                    Value v = vec.get(i);
                    if (v != ValueNull.INSTANCE && contains(v)) {
                        sel[out++] = i;
                    }
                }
            } else {
                for (int i = 0; i < selCount; i++) {
                    int idx = sel[i];
                    Value v = vec.get(idx);
                    if (v != ValueNull.INSTANCE && contains(v)) {
                        sel[out++] = idx;
                    }
                }
            }
            return out;
        }

        private boolean contains(Value v) {
            if (longSet != null) {
                return longSet.contains(v.getLong()) ^ not;
            }
            return set.contains(v) ^ not;
        }

        private int filterLongSet(int[] vals, int[] sel, int selCount, boolean fullRange) {
            int out = 0;
            if (fullRange) {
                for (int i = 0; i < selCount; i++) {
                    if (longSet.contains(vals[i]) ^ not) {
                        sel[out++] = i;
                    }
                }
            } else {
                for (int i = 0; i < selCount; i++) {
                    int idx = sel[i];
                    if (longSet.contains(vals[idx]) ^ not) {
                        sel[out++] = idx;
                    }
                }
            }
            return out;
        }

        private int filterLongSet(long[] vals, int[] sel, int selCount, boolean fullRange) {
            int out = 0;
            if (fullRange) {
                for (int i = 0; i < selCount; i++) {
                    if (longSet.contains(vals[i]) ^ not) {
                        sel[out++] = i;
                    }
                }
            } else {
                for (int i = 0; i < selCount; i++) {
                    int idx = sel[i];
                    if (longSet.contains(vals[idx]) ^ not) {
                        sel[out++] = idx;
                    }
                }
            }
            return out;
        }

        private int filterDict(int[] codes, Value[] dict, int dictSize,
                int[] sel, int selCount, boolean fullRange) {
            boolean[] allowed = new boolean[dictSize];
            boolean anyAllowed = false;
            for (int d = 0; d < dictSize; d++) {
                if ((longSet != null ? longSet.contains(dict[d].getLong()) : set.contains(dict[d])) ^ not) {
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
                    int code = codes[i];
                    if (code >= 0 && allowed[code]) {
                        sel[out++] = i;
                    }
                }
            } else {
                for (int i = 0; i < selCount; i++) {
                    int idx = sel[i];
                    int code = codes[idx];
                    if (code >= 0 && allowed[code]) {
                        sel[out++] = idx;
                    }
                }
            }
            return out;
        }

        @Override
        public boolean evalRow(Value[] row, SessionLocal session, CompareMode compareMode) {
            Value v = row[col];
            return v != ValueNull.INSTANCE && contains(v);
        }
    }

    private static final class NullCheckNode implements Node {
        private final int col;
        private final boolean not;

        NullCheckNode(int col, boolean not) {
            this.col = col;
            this.not = not;
        }

        @Override
        public int filterSegment(ColumnSegment segment, int[] sel, int selCount, boolean fullRange,
                BitSet masked, SessionLocal session, CompareMode compareMode) {
            NullBitmap nulls = segment.getColumn(col).nulls();
            if (!nulls.hasNull()) {
                if (not) {
                    if (fullRange) {
                        for (int i = 0; i < selCount; i++) {
                            sel[i] = i;
                        }
                    }
                    return selCount;
                }
                return 0;
            }
            long[] bits = nulls.rawBits();
            int out = 0;
            if (fullRange) {
                for (int i = 0; i < selCount; i++) {
                    boolean isNull = (bits[i >>> 6] & (1L << i)) != 0;
                    if (isNull ^ not) {
                        sel[out++] = i;
                    }
                }
            } else {
                for (int i = 0; i < selCount; i++) {
                    int idx = sel[i];
                    boolean isNull = (bits[idx >>> 6] & (1L << idx)) != 0;
                    if (isNull ^ not) {
                        sel[out++] = idx;
                    }
                }
            }
            return out;
        }

        @Override
        public boolean evalRow(Value[] row, SessionLocal session, CompareMode compareMode) {
            boolean isNull = row[col] == ValueNull.INSTANCE;
            return isNull ^ not;
        }
    }

    private static final class CardinalityCmpNode implements Node {
        private final int col;
        private final int cmpType;
        private final int target;

        CardinalityCmpNode(int col, int cmpType, int target) {
            this.col = col;
            this.cmpType = cmpType;
            this.target = target;
        }

        @Override
        public int filterSegment(ColumnSegment segment, int[] sel, int selCount, boolean fullRange,
                BitSet masked, SessionLocal session, CompareMode compareMode) {
            ColumnVector vec = segment.getColumn(col);
            int out = 0;
            if (vec instanceof ArrayVector av) {
                // 数组列已经按行存了元素的起止下标，求元素个数不必重建 ValueArray。
                for (int i = 0; i < selCount; i++) {
                    int idx = fullRange ? i : sel[i];
                    int len = av.length(idx);
                    if (len >= 0 && matchCompare(Integer.compare(len, target), cmpType)) {
                        sel[out++] = idx;
                    }
                }
                return out;
            }
            if (fullRange) {
                for (int i = 0; i < selCount; i++) {
                    Value v = vec.get(i);
                    if (v != ValueNull.INSTANCE && matchCard(v)) {
                        sel[out++] = i;
                    }
                }
            } else {
                for (int i = 0; i < selCount; i++) {
                    int idx = sel[i];
                    Value v = vec.get(idx);
                    if (v != ValueNull.INSTANCE && matchCard(v)) {
                        sel[out++] = idx;
                    }
                }
            }
            return out;
        }

        private boolean matchCard(Value v) {
            if (v.getValueType() != Value.ARRAY) {
                return false;
            }
            int len = ((ValueArray) v).getList().length;
            return matchCompare(Integer.compare(len, target), cmpType);
        }

        @Override
        public boolean evalRow(Value[] row, SessionLocal session, CompareMode compareMode) {
            Value v = row[col];
            return v != ValueNull.INSTANCE && matchCard(v);
        }
    }

    private static final class ArrayContainsNode implements Node {
        private final int col;
        private final Value target;
        private final boolean negate;

        ArrayContainsNode(int col, Value target, boolean negate) {
            this.col = col;
            this.target = target;
            this.negate = negate;
        }

        @Override
        public int filterSegment(ColumnSegment segment, int[] sel, int selCount, boolean fullRange,
                BitSet masked, SessionLocal session, CompareMode compareMode) {
            ColumnVector vec = segment.getColumn(col);
            if (vec instanceof ArrayVector) {
                return filterArray((ArrayVector) vec, sel, selCount, fullRange, session, compareMode);
            }
            int out = 0;
            for (int i = 0; i < selCount; i++) {
                int idx = fullRange ? i : sel[i];
                if (matchValue(vec.get(idx), session, compareMode) == (negate ? 0 : 1)) {
                    sel[out++] = idx;
                }
            }
            return out;
        }

        /**
         * 在元素列上判等：{@code starts} 给出每行元素的区间，按行的区间求“是否有元素相等”。
         *
         * <p>与 {@link ArrayVector#get(int)} 逐行重建 {@code ValueArray} 的区别是这里不产生任何对象。
         */
        private int filterArray(ArrayVector av, int[] sel, int selCount, boolean fullRange,
                SessionLocal session, CompareMode compareMode) {
            ColumnVector elements = av.elements();
            NullBitmap arrayNulls = av.nulls();
            int[] starts = av.rawStarts();
            int targetType = target.getValueType();
            if (target == ValueNull.INSTANCE) {
                NullBitmap elementNulls = elements.nulls();
                int out = 0;
                for (int i = 0; i < selCount; i++) {
                    int idx = fullRange ? i : sel[i];
                    if (arrayNulls.isNull(idx)) {
                        continue;
                    }
                    boolean found = false;
                    for (int j = starts[idx], end = starts[idx + 1]; j < end; j++) {
                        if (elementNulls.isNull(j)) {
                            found = true;
                            break;
                        }
                    }
                    if (found != negate) {
                        sel[out++] = idx;
                    }
                }
                return out;
            }
            if (elements instanceof IntVector && targetType == Value.INTEGER) {
                IntVector iv = (IntVector) elements;
                int[] vals = iv.rawValues();
                NullBitmap elementNulls = iv.nulls();
                boolean hasNull = elementNulls.hasNull();
                int t = target.getInt();
                int out = 0;
                for (int i = 0; i < selCount; i++) {
                    int idx = fullRange ? i : sel[i];
                    if (arrayNulls.isNull(idx)) {
                        continue;
                    }
                    boolean found = false;
                    for (int j = starts[idx], end = starts[idx + 1]; j < end; j++) {
                        if ((!hasNull || !elementNulls.isNull(j)) && vals[j] == t) {
                            found = true;
                            break;
                        }
                    }
                    if (found != negate) {
                        sel[out++] = idx;
                    }
                }
                return out;
            }
            if (elements instanceof LongVector && targetType == Value.BIGINT) {
                LongVector lv = (LongVector) elements;
                long[] vals = lv.rawValues();
                NullBitmap elementNulls = lv.nulls();
                boolean hasNull = elementNulls.hasNull();
                long t = target.getLong();
                int out = 0;
                for (int i = 0; i < selCount; i++) {
                    int idx = fullRange ? i : sel[i];
                    if (arrayNulls.isNull(idx)) {
                        continue;
                    }
                    boolean found = false;
                    for (int j = starts[idx], end = starts[idx + 1]; j < end; j++) {
                        if ((!hasNull || !elementNulls.isNull(j)) && vals[j] == t) {
                            found = true;
                            break;
                        }
                    }
                    if (found != negate) {
                        sel[out++] = idx;
                    }
                }
                return out;
            }
            if (elements instanceof DoubleVector && targetType == Value.DOUBLE) {
                DoubleVector dv = (DoubleVector) elements;
                double[] vals = dv.rawValues();
                NullBitmap elementNulls = dv.nulls();
                boolean hasNull = elementNulls.hasNull();
                double t = target.getDouble();
                int out = 0;
                for (int i = 0; i < selCount; i++) {
                    int idx = fullRange ? i : sel[i];
                    if (arrayNulls.isNull(idx)) {
                        continue;
                    }
                    boolean found = false;
                    for (int j = starts[idx], end = starts[idx + 1]; j < end; j++) {
                        if ((!hasNull || !elementNulls.isNull(j)) && Double.compare(vals[j], t) == 0) {
                            found = true;
                            break;
                        }
                    }
                    if (found != negate) {
                        sel[out++] = idx;
                    }
                }
                return out;
            }
            if (elements instanceof VarcharVector && targetType == Value.VARCHAR) {
                VarcharVector sv = (VarcharVector) elements;
                NullBitmap elementNulls = sv.nulls();
                boolean hasNull = elementNulls.hasNull();
                String t = target.getString();
                int out = 0;
                for (int i = 0; i < selCount; i++) {
                    int idx = fullRange ? i : sel[i];
                    if (arrayNulls.isNull(idx)) {
                        continue;
                    }
                    boolean found = false;
                    for (int j = starts[idx], end = starts[idx + 1]; j < end; j++) {
                        if ((!hasNull || !elementNulls.isNull(j))
                                && compareMode.compareString(sv.getString(j), t, false) == 0) {
                            found = true;
                            break;
                        }
                    }
                    if (found != negate) {
                        sel[out++] = idx;
                    }
                }
                return out;
            }
            if (elements instanceof DictVarcharVector && isStringType(targetType)) {
                DictVarcharVector dv = (DictVarcharVector) elements;
                int[] codes = dv.rawCodes();
                Value[] dict = dv.rawDict();
                boolean[] matched = new boolean[dv.dictSize()];
                for (int d = 0; d < matched.length; d++) {
                    matched[d] = dict[d].compareTo(target, session, compareMode) == 0;
                }
                int out = 0;
                for (int i = 0; i < selCount; i++) {
                    int idx = fullRange ? i : sel[i];
                    if (arrayNulls.isNull(idx)) {
                        continue;
                    }
                    boolean found = false;
                    for (int j = starts[idx], end = starts[idx + 1]; j < end; j++) {
                        int code = codes[j];
                        if (code >= 0 && matched[code]) {
                            found = true;
                            break;
                        }
                    }
                    if (found != negate) {
                        sel[out++] = idx;
                    }
                }
                return out;
            }
            int out = 0;
            for (int i = 0; i < selCount; i++) {
                int idx = fullRange ? i : sel[i];
                if (arrayNulls.isNull(idx)) {
                    continue;
                }
                boolean found = false;
                for (int j = starts[idx], end = starts[idx + 1]; j < end; j++) {
                    if (elements.get(j).compareTo(target, session, compareMode) == 0) {
                        found = true;
                        break;
                    }
                }
                if (found != negate) {
                    sel[out++] = idx;
                }
            }
            return out;
        }

        /**
         * 整值判等，返回 {@code 1} 命中、{@code 0} 不命中、{@code -1} 结果为 NULL。
         *
         * <p>非数组值按 H2 的定义结果也是 NULL（{@code ArrayFunction} 对既非 ARRAY 也非 ROW 的首参返回 NULL）。
         */
        private int matchValue(Value v, SessionLocal session, CompareMode compareMode) {
            if (v == null || v == ValueNull.INSTANCE) {
                return -1;
            }
            int type = v.getValueType();
            if (type != Value.ARRAY && type != Value.ROW) {
                return -1;
            }
            for (Value element : ((ValueCollectionBase) v).getList()) {
                if (element.compareTo(target, session, compareMode) == 0) {
                    return 1;
                }
            }
            return 0;
        }

        @Override
        public boolean evalRow(Value[] row, SessionLocal session, CompareMode compareMode) {
            return matchValue(row[col], session, compareMode) == (negate ? 0 : 1);
        }
    }

    private static boolean isStringType(int type) {
        return type == Value.VARCHAR || type == Value.CHAR || type == Value.VARCHAR_IGNORECASE;
    }

    private static final class LikeNode implements Node {
        private final int col;
        private final CompareLike like;
        private final String pattern;
        private final char escapeChar;
        private final boolean negate;

        LikeNode(int col, CompareLike like, String pattern, char escapeChar, boolean negate) {
            this.col = col;
            this.like = like;
            this.pattern = pattern;
            this.escapeChar = escapeChar;
            this.negate = negate;
        }

        private boolean matchString(String s) {
            return like.test(pattern, s, escapeChar) ^ negate;
        }

        @Override
        public int filterSegment(ColumnSegment segment, int[] sel, int selCount, boolean fullRange,
                BitSet masked, SessionLocal session, CompareMode compareMode) {
            ColumnVector vec = segment.getColumn(col);
            int out = 0;
            if (vec instanceof DictVarcharVector) {
                DictVarcharVector dv = (DictVarcharVector) vec;
                int dictSize = dv.dictSize();
                Value[] dict = dv.rawDict();
                boolean[] dictMatched = new boolean[dictSize];
                synchronized (like) {
                    for (int d = 0; d < dictSize; d++) {
                        dictMatched[d] = matchString(dict[d].getString());
                    }
                }
                int[] codes = dv.rawCodes();
                if (fullRange) {
                    for (int i = 0; i < selCount; i++) {
                        int c = codes[i];
                        if (c >= 0 && dictMatched[c]) {
                            sel[out++] = i;
                        }
                    }
                } else {
                    for (int i = 0; i < selCount; i++) {
                        int idx = sel[i];
                        int c = codes[idx];
                        if (c >= 0 && dictMatched[c]) {
                            sel[out++] = idx;
                        }
                    }
                }
                return out;
            }
            if (vec instanceof VarcharVector) {
                VarcharVector vv = (VarcharVector) vec;
                NullBitmap nulls = vv.nulls();
                boolean hasNull = nulls.hasNull();
                synchronized (like) {
                    if (fullRange) {
                        for (int i = 0; i < selCount; i++) {
                            if ((!hasNull || !nulls.isNull(i)) && matchString(vv.getString(i))) {
                                sel[out++] = i;
                            }
                        }
                    } else {
                        for (int i = 0; i < selCount; i++) {
                            int idx = sel[i];
                            if ((!hasNull || !nulls.isNull(idx)) && matchString(vv.getString(idx))) {
                                sel[out++] = idx;
                            }
                        }
                    }
                }
                return out;
            }
            synchronized (like) {
                if (fullRange) {
                    for (int i = 0; i < selCount; i++) {
                        Value v = vec.get(i);
                        if (v != ValueNull.INSTANCE && matchString(v.getString())) {
                            sel[out++] = i;
                        }
                    }
                } else {
                    for (int i = 0; i < selCount; i++) {
                        int idx = sel[i];
                        Value v = vec.get(idx);
                        if (v != ValueNull.INSTANCE && matchString(v.getString())) {
                            sel[out++] = idx;
                        }
                    }
                }
            }
            return out;
        }

        @Override
        public boolean evalRow(Value[] row, SessionLocal session, CompareMode compareMode) {
            Value v = row[col];
            if (v == ValueNull.INSTANCE) {
                return false;
            }
            synchronized (like) {
                return matchString(v.getString());
            }
        }
    }
}

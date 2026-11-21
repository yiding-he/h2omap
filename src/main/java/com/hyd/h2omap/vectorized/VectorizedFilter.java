/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.vectorized;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.TreeSet;
import java.util.function.IntConsumer;

import org.h2.command.query.Query;
import org.h2.command.query.Select;
import org.h2.engine.SessionLocal;
import org.h2.expression.BinaryOperation;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.H2ExpressionAccess;
import org.h2.expression.Operation1;
import org.h2.expression.Subquery;
import org.h2.expression.ValueExpression;
import org.h2.expression.condition.BetweenPredicate;
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
import org.h2.value.ValueNull;
import org.h2.value.ValueNumeric;

import com.hyd.h2omap.storage.ColumnSegment;

/**
 * 将 H2 单表过滤条件编译为可在 {@link ColumnSegment} 原始数组上运行的向量化过滤树。
 * 遇到不支持的表达式时 {@link #compile} 返回 {@code null}，由上层无缝回退 H2 原路径。
 *
 * <p>各节点实现与共享原语拆在同包独立文件中：{@link AndNode}、{@link OrNode}、
 * {@link ColCmpNode}、{@link ArithLongCmpNode}、{@link InNode}、{@link NullCheckNode}、
 * {@link CardinalityCmpNode}、{@link ArrayContainsNode}、{@link LikeNode}、{@link LongSet}，
 * 子查询折叠在 {@link SubqueryFold}。
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

        /**
         * 本节点的判定语义签名，供段级部分聚合缓存复用；
         * 形态复杂或含子查询预计算的节点保持默认 {@code null}，上层因此不缓存。
         */
        default String sig() {
            return null;
        }

        /**
         * 收集本节点求值时会读取的表列，供快照部分列读确定请求列集。
         *
         * <p>漏收集的列在快照段里是占位向量，意外访问会抛异常而不是静默错值。
         */
        default void collectColumns(IntConsumer sink) {
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

    /** 整棵过滤树的语义签名；含不支持签名的节点（如子查询预计算）时返回 {@code null}。 */
    public String signature() {
        return root.sig();
    }

    static String valueSig(Value value) {
        return value == null ? "~" : value.getValueType() + ":" + value;
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

    /** 快照部分列读的请求列集：过滤树求值会读取的全部表列，升序去重。 */
    public int[] referencedColumns() {
        TreeSet<Integer> columns = new TreeSet<>();
        root.collectColumns(columns::add);
        int[] result = new int[columns.size()];
        int i = 0;
        for (int c : columns) {
            result[i++] = c;
        }
        return result;
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
        if (expr instanceof BetweenPredicate) {
            return compileBetween((BetweenPredicate) expr, filter, session);
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

    /**
     * 编译 BETWEEN 谓词：普通形态折叠成两个区间比较的 AND，
     * NOT BETWEEN 折成两个取反比较的 OR（列空值两侧都不命中，三值语义一致）。
     * SYMMETRIC、边界为空值或非常量的形态交回 H2。
     */
    private static Node compileBetween(BetweenPredicate between, TableFilter filter,
            SessionLocal session) {
        if (between.isWhenConditionOperand()) {
            return null;
        }
        Expression left = between.getSubexpression(0);
        Expression a = between.getSubexpression(1);
        Expression b = between.getSubexpression(2);
        if (!isFoldableConstant(a, filter) || !isFoldableConstant(b, filter)) {
            return null;
        }
        // NOT 与 SYMMETRIC 从渲染 SQL 识别，与 LIKE 的操作符识别同一方式。
        String leftSql = left.getUnenclosedSQL(new StringBuilder(), 0).toString();
        String fullSql = between.getUnenclosedSQL(new StringBuilder(), 0).toString();
        String opPart = fullSql.length() > leftSql.length()
                ? fullSql.substring(leftSql.length()) : "";
        boolean not;
        if (opPart.startsWith(" NOT BETWEEN ")) {
            not = true;
        } else if (opPart.startsWith(" BETWEEN ")) {
            not = false;
        } else {
            return null;
        }
        if (opPart.substring(not ? 13 : 9).startsWith("SYMMETRIC ")) {
            return null;
        }
        Value va = a.getValue(session);
        Value vb = b.getValue(session);
        if (va == null || va == ValueNull.INSTANCE || vb == null || vb == ValueNull.INSTANCE) {
            return null;
        }
        Node lo;
        Node hi;
        if (not) {
            lo = compileLeftWithConst(left, Comparison.SMALLER, va, filter, session);
            hi = compileLeftWithConst(left, Comparison.BIGGER, vb, filter, session);
        } else {
            lo = compileLeftWithConst(left, Comparison.BIGGER_EQUAL, va, filter, session);
            hi = compileLeftWithConst(left, Comparison.SMALLER_EQUAL, vb, filter, session);
        }
        if (lo == null || hi == null) {
            return null;
        }
        return not ? new OrNode(lo, hi) : new AndNode(new Node[] { lo, hi });
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

    static boolean isStringType(int type) {
        return type == Value.VARCHAR || type == Value.CHAR || type == Value.VARCHAR_IGNORECASE;
    }
}

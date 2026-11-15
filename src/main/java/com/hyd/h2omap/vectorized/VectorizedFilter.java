/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.vectorized;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.h2.engine.SessionLocal;
import org.h2.expression.BinaryOperation;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.H2ExpressionAccess;
import org.h2.expression.Operation1;
import org.h2.expression.ValueExpression;
import org.h2.expression.condition.Comparison;
import org.h2.expression.condition.ConditionAndOr;
import org.h2.expression.condition.ConditionAndOrN;
import org.h2.expression.condition.ConditionInConstantSet;
import org.h2.expression.condition.ConditionInList;
import org.h2.expression.condition.H2ConditionAccess;
import org.h2.expression.condition.NullPredicate;
import org.h2.expression.function.CardinalityExpression;
import org.h2.table.TableFilter;
import org.h2.value.CompareMode;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueNull;

import com.hyd.h2omap.stats.ZoneMap;
import com.hyd.h2omap.storage.ColumnSegment;
import com.hyd.h2omap.vector.ColumnVector;
import com.hyd.h2omap.vector.DictValueVector;
import com.hyd.h2omap.vector.DictVarcharVector;
import com.hyd.h2omap.vector.DoubleVector;
import com.hyd.h2omap.vector.IntVector;
import com.hyd.h2omap.vector.LongVector;
import com.hyd.h2omap.vector.NullBitmap;

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
        return null;
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

        if (right.isConstant()) {
            return compileLeftWithConst(left, cmpType, right.getValue(session), filter, session);
        }
        if (left.isConstant()) {
            return compileLeftWithConst(right, flipCompare(cmpType), left.getValue(session), filter, session);
        }
        return null;
    }

    private static Node compileLeftWithConst(Expression left, int cmpType, Value constant,
            TableFilter filter, SessionLocal session) {
        if (constant == null || constant == ValueNull.INSTANCE) {
            return null;
        }
        int col = resolveColumn(left, filter);
        Value converted;
        try {
            converted = constant.convertTo(filter.getTable().getColumn(col >= 0 ? col : 0).getType(), session);
        } catch (RuntimeException ignored) {
            converted = constant;
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

    private static int resolveColumn(Expression expr, TableFilter filter) {
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

    private static boolean matchCompare(int cmp, int cmpType) {
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

    private static final class InNode implements Node {
        private final int col;
        private final boolean not;
        private final Set<Value> set;

        InNode(int col, boolean not, List<Value> values) {
            this.col = col;
            this.not = not;
            this.set = new HashSet<>(values);
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
            int out = 0;
            if (fullRange) {
                for (int i = 0; i < selCount; i++) {
                    Value v = vec.get(i);
                    if (v != ValueNull.INSTANCE && (set.contains(v) ^ not)) {
                        sel[out++] = i;
                    }
                }
            } else {
                for (int i = 0; i < selCount; i++) {
                    int idx = sel[i];
                    Value v = vec.get(idx);
                    if (v != ValueNull.INSTANCE && (set.contains(v) ^ not)) {
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
                if (set.contains(dict[d]) ^ not) {
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
            return v != ValueNull.INSTANCE && (set.contains(v) ^ not);
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
}

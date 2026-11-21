package com.hyd.h2omap.vectorized;

import org.h2.engine.SessionLocal;
import org.h2.expression.BinaryOperation;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.H2ExpressionAccess;
import org.h2.expression.aggregate.Aggregate;
import org.h2.expression.aggregate.AggregateType;
import org.h2.expression.aggregate.H2AggregateAccess;
import org.h2.table.TableFilter;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * 向量化聚合的口径：聚合类型、参数列、算术口径（列与常量或两列做 + - *）与输出口径，
 * 外加把 H2 的 {@link Aggregate} 表达式折成 {@code AggSpec} 的解析入口。
 * 字段按包内可见，{@link AggState} 直接读取。解析只在编译期调用，不持有存储锁。
 */
final class AggSpec {
    final AggregateType type;
    final int col;
    final int rightCol;
    final BinaryOperation.OpType arithOp;
    final boolean colOnLeft;
    final Value arithConst;
    final int outIdx;
    final TypeInfo resultType;
    final boolean distinct;

    AggSpec(AggregateType type, int col, int outIdx, TypeInfo resultType, boolean distinct) {
        this(type, col, -1, null, true, null, outIdx, resultType, distinct);
    }

    AggSpec(AggregateType type, int col, int rightCol, BinaryOperation.OpType arithOp,
            boolean colOnLeft, Value arithConst, int outIdx, TypeInfo resultType) {
        this(type, col, rightCol, arithOp, colOnLeft, arithConst, outIdx, resultType, false);
    }

    private AggSpec(AggregateType type, int col, int rightCol, BinaryOperation.OpType arithOp,
            boolean colOnLeft, Value arithConst, int outIdx, TypeInfo resultType, boolean distinct) {
        this.type = type;
        this.col = col;
        this.rightCol = rightCol;
        this.arithOp = arithOp;
        this.colOnLeft = colOnLeft;
        this.arithConst = arithConst;
        this.outIdx = outIdx;
        this.resultType = resultType;
        this.distinct = distinct;
    }

    boolean sameKey(AggSpec other) {
        if (type != other.type || col != other.col || rightCol != other.rightCol
                || arithOp != other.arithOp || colOnLeft != other.colOnLeft
                || distinct != other.distinct
                || resultType.getValueType() != other.resultType.getValueType()) {
            return false;
        }
        if (arithConst == null && other.arithConst == null) {
            return true;
        }
        return arithConst != null && arithConst.equals(other.arithConst);
    }

    /** 把 H2 聚合表达式折成 AggSpec；不支持的形态返回 {@code null}。 */
    static AggSpec parseAggregate(Aggregate agg, TableFilter filter, int outIdx, SessionLocal session) {
        AggregateType type = agg.getAggregateType();

        Expression[] args = H2AggregateAccess.getArgs(agg);
        boolean distinct = agg.isDistinct();
        switch (type) {
        case COUNT_ALL:
            return distinct ? null : new AggSpec(type, -1, outIdx, agg.getType(), false);
        case AVG: {
            int rt = agg.getType().getValueType();
            if (rt != Value.DOUBLE && rt != Value.NUMERIC) {
                return null;
            }
            // fall through
        }
        case COUNT:
        case SUM:
        case MIN:
        case MAX: {
            if (distinct && type != AggregateType.COUNT) {
                return null;
            }
            if (args.length != 1) {
                return null;
            }
            Expression arg = args[0].getNonAliasExpression();
            if (arg instanceof ExpressionColumn) {
                ExpressionColumn ec = (ExpressionColumn) arg;
                if (ec.getTableFilter() != filter) {
                    return null;
                }
                return new AggSpec(type, ec.getColumn().getColumnId(), outIdx, agg.getType(), distinct);
            }
            if (!distinct && arg instanceof BinaryOperation) {
                return parseArithAggregate(type, (BinaryOperation) arg, filter, outIdx, agg.getType(), session);
            }
            return null;
        }
        default:
            return null;
        }
    }

    private static AggSpec parseArithAggregate(AggregateType type, BinaryOperation bin, TableFilter filter,
            int outIdx, TypeInfo resultType, SessionLocal session) {
        BinaryOperation.OpType op = bin.getOperationType();
        if (op != BinaryOperation.OpType.PLUS
                && op != BinaryOperation.OpType.MINUS
                && op != BinaryOperation.OpType.MULTIPLY) {
            return null;
        }
        Expression left = H2ExpressionAccess.getLeft(bin);
        Expression right = H2ExpressionAccess.getRight(bin);
        if (left instanceof ExpressionColumn && right.isConstant()) {
            ExpressionColumn ec = (ExpressionColumn) left;
            if (ec.getTableFilter() != filter || !isNumericType(ec.getType().getValueType())) {
                return null;
            }
            Value c = right.getValue(session);
            if (c == null || c == ValueNull.INSTANCE || !isNumericType(c.getValueType())) {
                return null;
            }
            return new AggSpec(type, ec.getColumn().getColumnId(), -1, op, true, c, outIdx, resultType);
        }
        if (right instanceof ExpressionColumn && left.isConstant()) {
            ExpressionColumn ec = (ExpressionColumn) right;
            if (ec.getTableFilter() != filter || !isNumericType(ec.getType().getValueType())) {
                return null;
            }
            Value c = left.getValue(session);
            if (c == null || c == ValueNull.INSTANCE || !isNumericType(c.getValueType())) {
                return null;
            }
            return new AggSpec(type, ec.getColumn().getColumnId(), -1, op, false, c, outIdx, resultType);
        }
        if (left instanceof ExpressionColumn && right instanceof ExpressionColumn) {
            ExpressionColumn lc = (ExpressionColumn) left;
            ExpressionColumn rc = (ExpressionColumn) right;
            if (lc.getTableFilter() != filter || rc.getTableFilter() != filter) {
                return null;
            }
            if (!isNumericType(lc.getType().getValueType()) || !isNumericType(rc.getType().getValueType())) {
                return null;
            }
            return new AggSpec(type, lc.getColumn().getColumnId(), rc.getColumn().getColumnId(),
                    op, true, null, outIdx, resultType);
        }
        return null;
    }

    private static boolean isNumericType(int valueType) {
        return valueType == Value.TINYINT || valueType == Value.SMALLINT
                || valueType == Value.INTEGER || valueType == Value.BIGINT
                || valueType == Value.REAL || valueType == Value.DOUBLE;
    }
}

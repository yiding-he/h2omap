/*
 * Copyright 2026 HYD.
 */
package org.h2.expression;

/**
 * 暴露 {@link Operation1}、{@link Operation2} 与 {@link OperationN} 的受保护子表达式字段。
 */
public final class H2ExpressionAccess {

    private H2ExpressionAccess() {
    }

    public static Expression getArg(Operation1 op) {
        return op.arg;
    }

    public static Expression getLeft(Operation2 op) {
        return op.left;
    }

    public static Expression getRight(Operation2 op) {
        return op.right;
    }

    public static Expression[] getArgs(OperationN op) {
        return op.args;
    }
}

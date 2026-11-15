/*
 * Copyright 2026 HYD.
 */
package org.h2.expression.aggregate;

import org.h2.expression.Expression;

/**
 * 暴露 {@link AbstractAggregate} 的受保护参数数组。
 */
public final class H2AggregateAccess {

    private H2AggregateAccess() {
    }

    public static Expression[] getArgs(AbstractAggregate agg) {
        return agg.args;
    }
}

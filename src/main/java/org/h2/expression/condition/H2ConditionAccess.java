/*
 * Copyright 2026 HYD.
 */
package org.h2.expression.condition;

import java.util.List;

import org.h2.expression.Expression;

/**
 * 暴露 {@link SimplePredicate}、{@link ConditionIn} 与 {@link ConditionAndOr} 的包级成员。
 */
public final class H2ConditionAccess {

    private H2ConditionAccess() {
    }

    public static int getAndOrType(ConditionAndOr c) {
        return c.getAndOrType();
    }

    public static int getAndOrType(ConditionAndOrN c) {
        return c.getAndOrType();
    }

    public static Expression getSimpleLeft(SimplePredicate predicate) {
        return predicate.left;
    }

    public static boolean isNot(SimplePredicate predicate) {
        return predicate.not;
    }

    public static Expression getInLeft(Expression condition) {
        return ((ConditionIn) condition).left;
    }

    public static boolean isInNot(Expression condition) {
        return ((ConditionIn) condition).not;
    }

    public static List<Expression> getInValues(Expression condition) {
        return ((ConditionIn) condition).valueList;
    }
}

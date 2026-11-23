/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.vectorized;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.Subquery;
import org.h2.table.Column;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.h2.value.ValueNull;

import com.hyd.h2omap.table.OmapTable;

/**
 * 流式执行（M20 有序 / M23 平面）共用的投影声明：把投影项分类成
 * 「单表列引用 / 常量 / 确定表达式」三类并归并出最小读取列集。
 *
 * <p>分类规则（两类执行器共用同一口径）：
 * <ul>
 * <li>列引用：限本表数据列（{@code _ROWID_} 等伪列 {@code columnId < 0} 拒绝）；</li>
 * <li>常量：编译期求值一次；</li>
 * <li>表达式：限确定表达式（{@code DETERMINISTIC_VISITOR}，{@code RAND()} 交回 H2）、
 * 引用列全在本表数据列范围内，行发出时才逐行求值；顶层 {@link Subquery} 交回 H2，
 * 嵌套在其他表达式里的子查询照 H2 行式口径逐行求值。</li>
 * </ul>
 * 其余形态返回 {@code null}，调用方整条交回 H2 行式路径。
 */
final class ProjectionPlan {

    /** 输出列数。 */
    final int outLen;
    /** 各输出列的数据列 id；-1 表示该列不是直接列引用。 */
    final int[] projCols;
    /** 各输出列的常量值（非常量列为 NULL 占位）。 */
    final Value[] projConsts;
    /** 各输出列的发射期表达式；无该类投影时全 null。 */
    final Expression[] projExprs;
    /** 输出列到去重列槽的下标；-1 表示常量或表达式。 */
    final int[] projToUnique;
    /** 去重后的数据列 id（精确 {@link #uniqueCount} 个槽）。 */
    final int[] uniqueProjCols;
    final int uniqueCount;
    /** 是否含发射期表达式投影。 */
    final boolean hasExprs;

    private ProjectionPlan(int outLen, int[] projCols, Value[] projConsts,
            Expression[] projExprs, int[] projToUnique, int[] uniqueProjCols,
            int uniqueCount, boolean hasExprs) {
        this.outLen = outLen;
        this.projCols = projCols;
        this.projConsts = projConsts;
        this.projExprs = projExprs;
        this.projToUnique = projToUnique;
        this.uniqueProjCols = uniqueProjCols;
        this.uniqueCount = uniqueCount;
        this.hasExprs = hasExprs;
    }

    /**
     * 分类投影并归并最小读取列集。返回 {@code null} 表示形态不受支持，调用方交回 H2。
     */
    static ProjectionPlan claim(SessionLocal session, TableFilter tableFilter,
            List<Expression> expressions, int resultColumnCount) {
        OmapTable table = (OmapTable) tableFilter.getTable();
        Column[] columns = table.getColumns();

        int outLen = Math.max(resultColumnCount, expressions.size());
        int[] projCols = new int[outLen];
        Value[] projConsts = new Value[outLen];
        Expression[] projExprs = new Expression[outLen];
        List<Integer> exprRefCols = new ArrayList<>();
        for (int i = 0; i < outLen; i++) {
            Expression e = i < expressions.size() ? expressions.get(i) : null;
            if (e == null) {
                projCols[i] = -1;
                projConsts[i] = ValueNull.INSTANCE;
                continue;
            }
            if (e instanceof ExpressionColumn) {
                ExpressionColumn ec = (ExpressionColumn) e;
                if (ec.getTableFilter() != tableFilter) {
                    return null;
                }
                int id = ec.getColumn().getColumnId();
                if (id < 0 || id >= columns.length) {
                    return null;
                }
                projCols[i] = id;
            } else if (e.isConstant()) {
                projCols[i] = -1;
                projConsts[i] = e.getValue(session);
            } else {
                // 表达式投影在行发出时逐行求值；非确定表达式与顶层子查询交回 H2
                if (e instanceof Subquery
                        || !e.isEverything(ExpressionVisitor.DETERMINISTIC_VISITOR)) {
                    return null;
                }
                HashSet<Column> refs = new HashSet<>();
                e.isEverything(ExpressionVisitor.getColumnsVisitor(refs, table));
                for (Column c : refs) {
                    if (c.getTable() != table || c.getColumnId() < 0
                            || c.getColumnId() >= columns.length) {
                        return null;
                    }
                }
                projCols[i] = -1;
                projConsts[i] = ValueNull.INSTANCE;
                projExprs[i] = e;
                for (Column c : refs) {
                    exprRefCols.add(c.getColumnId());
                }
            }
        }
        int[] uniqueProjCols = new int[outLen + exprRefCols.size()];
        int uniqueCount = 0;
        int[] projToUnique = new int[outLen];
        for (int i = 0; i < outLen; i++) {
            int col = projCols[i];
            if (col < 0) {
                projToUnique[i] = -1;
                continue;
            }
            int found = -1;
            for (int u = 0; u < uniqueCount; u++) {
                if (uniqueProjCols[u] == col) {
                    found = u;
                    break;
                }
            }
            if (found < 0) {
                found = uniqueCount;
                uniqueProjCols[uniqueCount++] = col;
            }
            projToUnique[i] = found;
        }
        // 表达式引用列并入读取集：段内容里同样要能读到这些列
        for (int i = 0; i < exprRefCols.size(); i++) {
            int col = exprRefCols.get(i);
            int found = -1;
            for (int u = 0; u < uniqueCount; u++) {
                if (uniqueProjCols[u] == col) {
                    found = u;
                    break;
                }
            }
            if (found < 0) {
                uniqueProjCols[uniqueCount++] = col;
            }
        }
        boolean exprs = false;
        for (Expression e : projExprs) {
            if (e != null) {
                exprs = true;
                break;
            }
        }
        return new ProjectionPlan(outLen, projCols, projConsts, projExprs, projToUnique,
                java.util.Arrays.copyOf(uniqueProjCols, uniqueCount), uniqueCount, exprs);
    }
}

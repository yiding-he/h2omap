/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.vectorized;

import java.util.List;
import java.util.TreeSet;

import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.result.LocalResult;
import org.h2.result.SortOrder;
import org.h2.table.TableFilter;
import org.h2.value.Value;

import com.hyd.h2omap.storage.ColumnarStore;
import com.hyd.h2omap.storage.VectorizedSnapshot;
import com.hyd.h2omap.table.OmapTable;

/**
 * 单表过滤与聚合的向量化执行入口（facade）。
 *
 * <p>两个公开入口保持与替换后的 {@code org.h2.command.query.Select} 的调用契约不变：
 * <ul>
 * <li>{@link #tryExecuteFlat}：非聚合单表投影，实际执行在 {@link FlatExecutor}
 *     （有序模式在 {@link OrderedExecutor}）；</li>
 * <li>{@link #tryExecuteGroup}：单表聚合，实际执行在 {@link GroupExecutor}
 *     （聚合口径解析在 {@link AggSpec}，累加器在 {@link AggState}，
 *     HAVING 编译在 {@link HavingPredicates}）。</li>
 * </ul>
 *
 * 本类另持有两条执行路径共用的快照管线：取向量化快照并把过滤条件给出的列值区间
 * 交给存储层按段剪枝，逐出段按请求列集做部分列读、不进驻留模型。
 * 遇到不支持的形态时入口返回 {@code false}，交由 H2 原路径无缝兜底。
 */
public final class OmapVectorizedExecutor {

    private OmapVectorizedExecutor() {
    }

    public static boolean tryExecuteFlat(
            SessionLocal session,
            TableFilter tableFilter,
            Expression condition,
            List<Expression> expressions,
            int resultColumnCount,
            long maxRows,
            SortOrder sortOrder,
            LocalResult result) {
        return FlatExecutor.execute(session, tableFilter, condition, expressions,
                resultColumnCount, maxRows, sortOrder, result);
    }

    public static boolean tryExecuteGroup(
            SessionLocal session,
            TableFilter tableFilter,
            Expression condition,
            Expression having,
            int havingIndex,
            List<Expression> expressions,
            int resultColumnCount,
            int[] groupIndex,
            boolean[] groupByExpression,
            int[] groupByCopies,
            LocalResult result) {
        return GroupExecutor.execute(session, tableFilter, condition, having, havingIndex,
                expressions, resultColumnCount, groupIndex, groupByExpression, groupByCopies, result);
    }

    /**
     * 取向量化快照，并把过滤条件给出的列值区间交给存储层按段剪枝；
     * 逐出段按请求列集做部分列读，不进驻留模型。
     *
     * <p>区间只由顶层 {@code AND} 的合取项给出，与合取项的排列顺序无关，
     * 所以不可能命中的段在任何谓词开始求值之前就被跳过。
     */
    static VectorizedSnapshot takeSnapshot(OmapTable table, VectorizedFilter filter,
            TableFilter tableFilter, int[] columns) {
        ColumnarStore store = table.getStore();
        if (filter == null) {
            return store.takeVectorizedSnapshot(null, null, columns);
        }
        Value[][] bounds = filter.segmentBounds(tableFilter.getTable().getColumns().length);
        return store.takeVectorizedSnapshot(bounds[0], bounds[1], columns);
    }

    /** 部分列读的请求列集：并集升序，负值（常量或无排序键）忽略。 */
    static int[] unionColumns(int[]... sets) {
        TreeSet<Integer> all = new TreeSet<>();
        for (int[] set : sets) {
            if (set != null) {
                for (int c : set) {
                    if (c >= 0) {
                        all.add(c);
                    }
                }
            }
        }
        int[] result = new int[all.size()];
        int i = 0;
        for (int c : all) {
            result[i++] = c;
        }
        return result;
    }
}

/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.index;

import org.h2.command.query.AllColumnsForPlan;
import org.h2.engine.SessionLocal;
import org.h2.index.Cursor;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.IndexColumn;
import org.h2.table.Table;
import org.h2.table.TableFilter;

import com.hyd.h2omap.storage.ColumnarStore;

/**
 * 全表扫描索引，也就是 {@link Table#getScanIndex} 返回的那个。
 *
 * <p>{@code Index} 是抽象类（不是接口），所以这里用 {@code extends}。
 * H2 自己的样例见 {@code TreeSetIndex}，
 * 位置 {@code h2/src/test/org/h2/test/db/TestTableEngines.java:896}。
 */
public final class OmapScanIndex extends Index {

    private final ColumnarStore store;

    public OmapScanIndex(Table table, int id, String name, IndexColumn[] columns, IndexType indexType,
            ColumnarStore store) {
        super(table, id, name, columns, 0, indexType);
        this.store = store;
    }

    @Override
    public void add(SessionLocal session, Row row) {
        // 行数据由 ColumnarStore 持有，本索引不另建结构。
    }

    /**
     * 必须返回 null。
     *
     * <p>{@code Select.getIndexSorts} 用 {@code getCreateSQL() != null}
     * 判断一个索引能否提供行顺序（{@code org/h2/command/query/Select.java:635}）。
     * 返回非 null 时 H2 会认为本索引已按 ORDER BY 排好序，直接跳过排序，
     * 结果顺序就是 rowId 顺序。列扫描不给任何列顺序。
     *
     * <p>见 {@code org/h2/command/query/Select.java:638}。
     */
    @Override
    public String getCreateSQL() {
        return null;
    }

    @Override
    public void remove(SessionLocal session, Row row) {
        // 同上。
    }

    @Override
    public Cursor find(SessionLocal session, SearchRow first, SearchRow last, boolean reverse) {
        // TODO zone map 剪枝：把 first / last 翻译成每列的下界上界，
        //      跳过 zone map 不相交的列段，只返回候选 rowId。
        //      ColumnSegment#getZoneMap 与 ZoneMap#mayIntersect 已经就位。
        long[] rowIds = store.collectVisibleRowIds(reverse);
        return new OmapCursor(store, rowIds);
    }

    @Override
    public double getCost(SessionLocal session, int[] masks, TableFilter[] filters, int filter,
            SortOrder sortOrder, AllColumnsForPlan allColumnsSet, boolean isSelectCommand) {
        return getCostRangeIndex(masks, getRowCountApproximation(session), filters, filter,
                sortOrder, true, allColumnsSet, isSelectCommand);
    }

    @Override
    public void remove(SessionLocal session) {
        // 索引本身随表一起销毁，这里没有额外资源。
    }

    @Override
    public void truncate(SessionLocal session) {
        // 由 OmapTable#truncate 统一清空存储，本索引无额外结构。
    }

    @Override
    public boolean needRebuild() {
        return true;
    }

    @Override
    public long getRowCount(SessionLocal session) {
        return store.getRowCount();
    }

    @Override
    public long getRowCountApproximation(SessionLocal session) {
        return store.getRowCount();
    }

    @Override
    public void close(SessionLocal session) {
        // 纯内存，没有资源需要释放。
    }
}

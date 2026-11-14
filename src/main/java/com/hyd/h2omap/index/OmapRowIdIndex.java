/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.index;

import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.h2.command.query.AllColumnsForPlan;
import org.h2.engine.SessionLocal;
import org.h2.index.Cursor;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.index.SingleRowCursor;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.IndexColumn;
import org.h2.table.Table;
import org.h2.table.TableFilter;

import com.hyd.h2omap.storage.ColumnarStore;

/**
 * 主键或唯一索引，用来按值定位 rowId。
 *
 * <p>列式存储里 UPDATE 和 DELETE 都要先定位到行，
 * 这个索引是细粒度变更的关键。没有它，每次点改都要全表扫描。
 *
 * <p>骨架用 {@code TreeMap} 撑着，真正的实现应该换成
 * 针对列类型特化的哈希或跳表，避免每次比较都装箱。
 */
public final class OmapRowIdIndex extends Index {

    private final ColumnarStore store;

    /** 索引键到 rowId。比较规则复用 {@link Index#compareRows}。 */
    private final NavigableMap<SearchRow, Long> entries = new TreeMap<>(this::compareRows);

    public OmapRowIdIndex(Table table, int id, String name, IndexColumn[] columns,
            int uniqueColumnCount, IndexType indexType, ColumnarStore store) {
        super(table, id, name, columns, uniqueColumnCount, indexType);
        this.store = store;
    }

    @Override
    public void add(SessionLocal session, Row row) {
        if (entries.get(row) != null) {
            throw org.h2.message.DbException.get(
                    org.h2.api.ErrorCode.DUPLICATE_KEY_1, getTable().getName());
        }
        entries.put(row, row.getKey());
    }

    @Override
    public void remove(SessionLocal session, Row row) {
        entries.remove(row);
    }

    @Override
    public Cursor find(SessionLocal session, SearchRow first, SearchRow last, boolean reverse) {
        if (first != null && last != null && compareRows(first, last) == 0) {
            Long rowId = entries.get(first);
            if (rowId == null) {
                return SingleRowCursor.EMPTY;
            }
            Row row = store.materialize(rowId);
            return row == null ? SingleRowCursor.EMPTY : new SingleRowCursor(row);
        }
        // TODO 区间查询：按索引列区间取 subMap，再交给 OmapCursor。
        //      目前退化成全表扫描。
        return new OmapCursor(store, store.rowIterator(null, null, reverse));
    }

    @Override
    public double getCost(SessionLocal session, int[] masks, TableFilter[] filters, int filter,
            SortOrder sortOrder, AllColumnsForPlan allColumnsSet, boolean isSelectCommand) {        return 10 * getCostRangeIndex(masks, getRowCountApproximation(session), filters, filter,
                sortOrder, false, allColumnsSet, isSelectCommand);
    }

    @Override
    public String getCreateSQL() {
        // 同 OmapScanIndex：返回非 null 会让 H2 跳过 ORDER BY 的排序。
        // TODO M4 实现区间查询后，find 按索引列有序返回行，可以改成非 null。
        return null;
    }

    @Override
    public void remove(SessionLocal session) {
        entries.clear();
    }

    @Override
    public void truncate(SessionLocal session) {
        entries.clear();
    }

    @Override
    public boolean needRebuild() {
        return true;
    }

    @Override
    public long getRowCount(SessionLocal session) {
        return entries.size();
    }

    @Override
    public long getRowCountApproximation(SessionLocal session) {
        return entries.size();
    }

    @Override
    public void close(SessionLocal session) {
        entries.clear();
    }

    /** 供后续区间查询使用。 */
    Map<SearchRow, Long> entries() {
        return entries;
    }
}

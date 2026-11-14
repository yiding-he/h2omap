/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.index;

import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

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
 * 主键或唯一索引，用来按值定位 rowId。
 *
 * <p>列式存储里 UPDATE 和 DELETE 都要先定位到行，
 * 这个索引是细粒度变更的关键。没有它，每次点改都要全表扫描。
 *
 * <p>用 {@code ConcurrentSkipListMap} 撑着：{@code find} 要在存储锁之外迭代
 * {@code subMap}，而写路径并发 {@code put}/{@code remove}，普通 {@code TreeMap}
 * 会因此损坏或产生 {@code ConcurrentModificationException}。后续应换成
 * 针对列类型特化的结构，避免按 {@code SearchRow} 逐次比较。
 */
public final class OmapRowIdIndex extends Index {

    private final ColumnarStore store;

    /** 索引键到 rowId。比较规则复用 {@link Index#compareRows}。 */
    private final NavigableMap<SearchRow, Long> entries = new ConcurrentSkipListMap<>(this::compareRows);

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

    /**
     * 按索引列的区间返回行。等值是区间的退化形式，两种情形共用同一条路径。
     *
     * <p>{@code first}/{@code last} 为 null 表示该侧无界；H2 在倒序时已经
     * 把两者交换，所以这里先取正序区间，再按需倒序迭代。
     */
    @Override
    public Cursor find(SessionLocal session, SearchRow first, SearchRow last, boolean reverse) {
        NavigableMap<SearchRow, Long> range = range(first, last);
        Iterator<Long> rowIds = (reverse ? range.descendingMap() : range).values().iterator();
        return new OmapCursor(store, () -> rowIds.hasNext() ? rowIds.next() : -1);
    }

    private NavigableMap<SearchRow, Long> range(SearchRow first, SearchRow last) {
        if (first == null) {
            return last == null ? entries : entries.headMap(last, true);
        }
        return last == null ? entries.tailMap(first, true) : entries.subMap(first, true, last, true);
    }

    /**
     * 与扫描索引用同一个估算函数，不额外加码。
     *
     * <p>加码会让 {@code WHERE pk > ?} 这类范围查询永远选扫描索引，
     * 上面的区间分支就不会被调用（实测 6 种查询的计划后确认）。
     */
    @Override
    public double getCost(SessionLocal session, int[] masks, TableFilter[] filters, int filter,
            SortOrder sortOrder, AllColumnsForPlan allColumnsSet, boolean isSelectCommand) {
        return getCostRangeIndex(masks, getRowCountApproximation(session), filters, filter,
                sortOrder, false, allColumnsSet, isSelectCommand);
    }

    @Override
    public String getCreateSQL() {
        // 同 OmapScanIndex：返回非 null 会让 H2 认为本索引已按索引列排好序，
        // 从而跳过 ORDER BY 的排序。区间分支确实按索引列有序返回，但索引列与
        // 排序列的匹配、null 排序方向都由 Select.getIndexSorts 判断，判断失误
        // 会得到错误的结果顺序，所以保持 null，交给扫描加排序。
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

    /** 索引键到 rowId 的底层映射。 */
    Map<SearchRow, Long> entries() {
        return entries;
    }
}

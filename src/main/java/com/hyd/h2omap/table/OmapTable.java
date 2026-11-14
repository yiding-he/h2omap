/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.table;

import java.util.ArrayList;
import java.util.List;

import org.h2.command.ddl.CreateTableData;
import org.h2.engine.SessionLocal;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.result.LocalResult;
import org.h2.result.Row;
import org.h2.table.IndexColumn;
import org.h2.table.TableBase;
import org.h2.table.TableType;

import com.hyd.h2omap.engine.OmapOptions;
import com.hyd.h2omap.index.OmapRowIdIndex;
import com.hyd.h2omap.index.OmapScanIndex;
import com.hyd.h2omap.storage.ColumnarStore;

/**
 * 列式表。H2 的 {@code Table} 接口在这一层收口，下面全部是 OMAP 自己的结构。
 *
 * <p>骨架说明：
 * <ul>
 * <li>数据放在 {@link ColumnarStore}，由不可变列段加行式 delta 组成；
 * <li>写入路径是逐行回调，正好可以直接落到 delta；
 * <li>不加锁，不参与 H2 的事务。
 * </ul>
 *
 * <p>和 H2 的约定见 README 的“与 H2 的契约”一节。
 */
public final class OmapTable extends TableBase {

    private final ColumnarStore store;

    private final OmapScanIndex scanIndex;

    private final List<Index> indexes = new ArrayList<>(2);

    public OmapTable(CreateTableData data, OmapOptions options) {
        super(data);
        this.store = new ColumnarStore(getColumns(), options);
        this.scanIndex = new OmapScanIndex(this, data.id, data.tableName + "_SCAN",
                IndexColumn.wrap(getColumns()), IndexType.createScan(false), store);
        this.indexes.add(scanIndex);
    }

    public ColumnarStore getColumnarStore() {
        return store;
    }

    // ------------------------------------------------------------------
    // 写入路径：都是逐行回调，直接进 delta，不重写列段
    // ------------------------------------------------------------------

    @Override
    public void addRow(SessionLocal session, Row row) {
        long rowId = store.append(row.getValueList());
        row.setKey(rowId);
        // 扫描索引不占结构，主键索引要登记新行。
        for (Index index : indexes) {
            index.add(session, row);
        }
    }

    @Override
    public void removeRow(SessionLocal session, Row row) {
        store.remove(row.getKey());
        for (Index index : indexes) {
            index.remove(session, row);
        }
    }

    @Override
    public long truncate(SessionLocal session) {
        long old = store.truncate();
        for (Index index : indexes) {
            index.truncate(session);
        }
        return old;
    }

    /**
     * 覆盖默认实现。
     *
     * <p>H2 的默认实现会把 UPDATE 结果全量物化进 {@code LocalResult}，
     * 然后两趟遍历做 remove 和 add（见 {@code table/Table.java:525}）。
     * 这里有 delta，单趟流式处理就够。
     *
     * <p>{@code rows} 里每逻辑行占两个位置：先前像，后新像。
     */
    @Override
    public void updateRows(SessionLocal session, LocalResult rows, Runnable cancellationCheck) {
        rows.done();
        int scanned = 0;
        while (rows.next()) {
            if ((++scanned & 127) == 0) {
                cancellationCheck.run();
            }
            Row oldRow = rows.currentRowForTable();
            rows.next();
            Row newRow = rows.currentRowForTable();
            store.update(oldRow.getKey(), newRow.getValueList());
            for (Index index : indexes) {
                index.remove(session, oldRow);
                index.add(session, newRow);
            }
        }
        rows.reset();
    }

    // ------------------------------------------------------------------
    // 索引
    // ------------------------------------------------------------------

    /**
     * DDL 带 PRIMARY KEY 或 UNIQUE 时 H2 会调这里。
     * 目前只支持唯一索引，用 rowId 点查索引实现，支撑 UPDATE 和 DELETE。
     */
    @Override
    public Index addIndex(SessionLocal session, String indexName, int indexId, IndexColumn[] cols,
            int uniqueColumnCount, IndexType indexType, boolean create, String indexComment) {
        if (!indexType.isPrimaryKey() && !indexType.isUnique()) {
            throw org.h2.message.DbException.getUnsupportedException(
                    "OMAP only supports PRIMARY KEY and UNIQUE indexes: " + indexName);
        }
        OmapRowIdIndex index = new OmapRowIdIndex(this, indexId, indexName, cols, uniqueColumnCount,
                indexType, store);
        // TODO 已有数据要回填进索引
        indexes.add(index);
        setModified();
        return index;
    }

    @Override
    public Index getScanIndex(SessionLocal session) {
        return scanIndex;
    }

    @Override
    public List<Index> getIndexes() {
        return indexes;
    }

    @Override
    public long getMaxDataModificationId() {
        return store.getDataModificationId();
    }

    // ------------------------------------------------------------------
    // 元信息
    // ------------------------------------------------------------------

    @Override
    public TableType getTableType() {
        return TableType.EXTERNAL_TABLE_ENGINE;
    }

    /**
     * 必须保持 false。
     *
     * <p>返回 true 会让 H2 调 {@code lockRow}，而 {@code Table.lockRow}
     * 的默认实现直接抛 {@code UnsupportedException}。
     */
    @Override
    public boolean isRowLockable() {
        return false;
    }

    /**
     * 有 delta 时扫描结果随写入变化，不能返回 true。
     */
    @Override
    public boolean isDeterministic() {
        return false;
    }

    @Override
    public boolean canGetRowCount() {
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
    public boolean canDrop() {
        return true;
    }

    @Override
    public void checkSupportAlter() {
        // ALTER TABLE 暂不支持，改成抛异常即可。
    }

    @Override
    public void close(SessionLocal session) {
        // 纯内存，没有资源需要释放。
    }
}

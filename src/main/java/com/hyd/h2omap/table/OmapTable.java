/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.table;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.h2.command.ddl.CreateTableData;
import org.h2.engine.SessionLocal;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.message.DbException;
import org.h2.result.LocalResult;
import org.h2.result.Row;
import org.h2.table.IndexColumn;
import org.h2.table.TableBase;
import org.h2.table.TableType;
import org.h2.value.Value;

import com.hyd.h2omap.engine.OmapOptions;
import com.hyd.h2omap.index.OmapRowIdIndex;
import com.hyd.h2omap.index.OmapScanIndex;
import com.hyd.h2omap.persistence.OmapPersistence;
import com.hyd.h2omap.storage.ColumnarStore;

/**
 * 列式表。H2 的 {@code Table} 接口在这一层收口，下面全部是 OMAP 自己的结构。
 *
 * <p>数据放在 {@link ColumnarStore}，由不可变列段加行式 delta 组成。
 * 写入路径是逐行回调，正好可以直接落进开放段。
 * 不加锁，不参与 H2 的事务。
 */
public final class OmapTable extends TableBase {

    private final ColumnarStore store;

    private final OmapScanIndex scanIndex;

    private final List<Index> indexes = new ArrayList<>(2);

    private final AtomicLong lastModificationId = new AtomicLong();

    private final OmapPersistence persistence;

    public OmapTable(CreateTableData data, OmapOptions options) {
        super(data);
        this.store = new ColumnarStore(getColumns(), options, getCompareMode());
        // 惰性加载由服务端启动参数决定，默认关闭。
        this.store.setLazyLoad(database.getSettings().omapLazyLoad);
        this.store.setVisibilityListener(this::setModified);
        this.scanIndex = new OmapScanIndex(this, data.id, data.tableName + "_SCAN",
                IndexColumn.wrap(getColumns()), IndexType.createScan(false), store);
        this.indexes.add(scanIndex);
        this.persistence = initPersistence(data, options);
        if (this.persistence != null) {
            // 先登记 persistence 再恢复：恢复收尾的预算逐出要校验段文件存在。
            this.store.setPersistence(this.persistence);
            if (this.persistence.hasExistingData()) {
                this.persistence.recover(store);
                this.store.startLazyLoading();
                syncLastModificationIdWithDatabase();
            } else {
                this.persistence.initEmptyWal();
            }
        }
    }

    private OmapPersistence initPersistence(CreateTableData data, OmapOptions options) {
        Path dir = null;
        if (options.getBackupDir() != null) {
            dir = Paths.get(options.getBackupDir());
        } else if (Boolean.FALSE.equals(options.getBackup())) {
            return null;
        } else if (database.isPersistent() || Boolean.TRUE.equals(options.getBackup())) {
            String dbPath = database.getDatabasePath();
            if (dbPath == null) {
                throw DbException.getInvalidValueException(
                        "OMAP parameter backupDir", "required when backup=true on mem database");
            }
            dir = Paths.get(dbPath + ".omap", data.schema.getName() + "_" + data.tableName);
        }
        if (dir == null) {
            return null;
        }
        return new OmapPersistence(dir, database, getCompareMode(), options.getSyncIntervalMs());
    }

    public ColumnarStore getColumnarStore() {
        return store;
    }

    // ------------------------------------------------------------------
    // 写入路径：逐行回调，直接进开放段，不重写已冻结的列段
    // ------------------------------------------------------------------

    @Override
    public void addRow(SessionLocal session, Row row) {
        long rowId = store.append(row.getValueList());
        row.setKey(rowId);
        // 扫描索引不占结构，主键索引要登记新行。
        try {
            for (Index index : indexes) {
                index.add(session, row);
            }
        } catch (DbException e) {
            // 没有事务：唯一约束失败时行已经写进段缓冲，必须自己撤回。
            store.remove(rowId);
            row.setKey(0);
            throw e;
        } finally {
            syncLastModificationIdWithDatabase();
        }
    }

    @Override
    public void removeRow(SessionLocal session, Row row) {
        store.remove(row.getKey());
        for (Index index : indexes) {
            index.remove(session, row);
        }
        syncLastModificationIdWithDatabase();
    }

    @Override
    public long truncate(SessionLocal session) {
        long old = store.truncate();
        for (Index index : indexes) {
            index.truncate(session);
        }
        syncLastModificationIdWithDatabase();
        return old;
    }

    /**
     * 覆盖默认实现。
     *
     * <p>H2 的默认实现会把 UPDATE 结果全量物化进 {@code LocalResult}，
     * 然后两趟遍历做 remove 和 add（见 {@code table/Table.java:525}）。
     * 这里有开放段与 delta，单趟流式处理就够。
     *
     * <p>{@code rows} 里每逻辑行占两个位置：先前像，后新像。
     */
    @Override
    public void updateRows(SessionLocal session, LocalResult rows, Runnable cancellationCheck) {
        rows.done();
        int scanned = 0;
        try {
            while (rows.next()) {
                if ((++scanned & 127) == 0) {
                    cancellationCheck.run();
                }
                Row oldRow = rows.currentRowForTable();
                rows.next();
                Row newRow = rows.currentRowForTable();
                // 先在旧值失效前取到它，索引失败时靠它回退。
                Value[] oldValues = oldRow.getValueList();
                store.update(oldRow.getKey(), newRow.getValueList());
                try {
                    for (Index index : indexes) {
                        index.remove(session, oldRow);
                        index.add(session, newRow);
                    }
                } catch (DbException e) {
                    // 同上：唯一约束失败时把旧像写回去。
                    store.update(oldRow.getKey(), oldValues);
                    for (Index index : indexes) {
                        index.remove(session, newRow);
                        index.add(session, oldRow);
                    }
                    throw e;
                }
            }
        } finally {
            if (scanned > 0) {
                syncLastModificationIdWithDatabase();
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
        // ALTER TABLE ADD CONSTRAINT 时表里可能已经有行，索引必须回填。
        // H2 只在 MVTable.addIndex 里根据 needRebuild() 重建索引，
        // 外部引擎表不会走到那段代码。
        for (long rowId : store.collectVisibleRowIds(false)) {
            Row row = store.materialize(rowId);
            if (row != null) {
                index.add(session, row);
            }
        }
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
        return lastModificationId.get();
    }

    /**
     * 与 H2 全局修改计数同步（CAS 保证并发写时不倒退），
     * 使 {@code Query} 的结果集缓存判定与标量子查询复用生效并在写入后失效。
     */
    private void syncLastModificationIdWithDatabase() {
        long next = database.getNextModificationDataId();
        long current;
        do {
            current = lastModificationId.get();
        } while (next > current && !lastModificationId.compareAndSet(current, next));
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
     * 表本身是确定性的，数据变更通过 {@link #getMaxDataModificationId()} 通知 H2 失效缓存。
     * 若返回 false，{@code Query.getNoCache()} 会禁用结果复用，导致标量子查询按外层行数重复执行。
     */
    @Override
    public boolean isDeterministic() {
        return true;
    }

    @Override
    public boolean canGetRowCount(SessionLocal session) {
        return true;
    }

    /**
     * 默认实现返回 false，会让 {@code TRUNCATE TABLE} 被拒绝
     * （{@code org/h2/command/ddl/TruncateTable.java:41}）。
     */
    @Override
    public boolean canTruncate() {
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

    public ColumnarStore getStore() {
        return store;
    }

    @Override
    public void close(SessionLocal session) {
        store.stopLazyLoading();
        if (persistence != null) {
            persistence.close();
        }
    }

    @Override
    public void removeIndexOrTransferOwnership(SessionLocal session, Index index) {
        if (index instanceof OmapRowIdIndex) {
            indexes.remove(index);
            return;
        }
        super.removeIndexOrTransferOwnership(session, index);
    }

    @Override
    public void removeChildrenAndResources(SessionLocal session) {
        super.removeChildrenAndResources(session);
        if (persistence != null) {
            persistence.deleteDirectory();
        }
    }
}

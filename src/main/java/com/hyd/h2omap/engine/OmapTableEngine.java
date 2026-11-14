/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.engine;

import org.h2.api.TableEngine;
import org.h2.command.ddl.CreateTableData;
import org.h2.table.Table;

import com.hyd.h2omap.table.OmapTable;

/**
 * OMAP 接入 H2 的唯一入口。
 *
 * <p>用法：
 * <pre>
 * CREATE TABLE T(...) ENGINE 'com.hyd.h2omap.engine.OmapTableEngine'
 *     WITH 'segmentSize=65536'
 * </pre>
 * 或者设置成默认引擎：
 * <pre>
 * SET DEFAULT_TABLE_ENGINE 'com.hyd.h2omap.engine.OmapTableEngine'
 * </pre>
 *
 * <p>H2 通过 {@code org.h2.schema.Schema#createTable} 调用本类，
 * 这条路径不经过 {@code org.h2.mvstore.db.Store}，所以表数据
 * 完全不落在 MVStore 里。
 */
public final class OmapTableEngine implements TableEngine {

    @Override
    public Table createTable(CreateTableData data) {
        return new OmapTable(data, OmapOptions.from(data.tableEngineParams));
    }
}

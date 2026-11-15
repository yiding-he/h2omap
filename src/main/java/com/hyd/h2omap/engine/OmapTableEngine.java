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
 * CREATE TABLE T(...) ENGINE "com.hyd.h2omap.engine.OmapTableEngine"
 *     WITH "segmentSize=65536"
 * </pre>
 * {@code ENGINE} 与 {@code WITH} 后面必须是双引号标识符，单引号与不加引号的标识符都不接受。
 * 不写 {@code ENGINE} 时，普通用户表默认走本引擎（由替换后的
 * {@code org.h2.schema.Schema#createTable} 决定）；显式写
 * {@code ENGINE "org.h2.mvstore.db.Store"} 则回退到 MVStore。
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

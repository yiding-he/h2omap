/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.command;

import org.h2.command.CommandInterface;
import org.h2.command.Prepared;
import org.h2.engine.Database;
import org.h2.engine.SessionLocal;
import org.h2.engine.Setting;
import org.h2.schema.Schema;
import org.h2.expression.Expression;
import org.h2.message.DbException;
import org.h2.result.ResultInterface;
import org.h2.table.Table;
import org.h2.value.Value;

import com.hyd.h2omap.engine.OmapOptions;
import com.hyd.h2omap.table.OmapTable;

/**
 * {@code SET OMAP_SLICE_CACHE_BYTES <带单位的数字>}。
 *
 * <p>数据库全局变量：逐出段列切片缓存的字节预算。值是带单位的数字，后缀 k/m/g
 * （不区分大小写，按 1024 换算），带后缀时需写成字符串字面量（如 {@code '64m'}），
 * 纯字节数可以不加引号；不接受 NULL。默认与下限均为 32 MiB（32768k），更小的值报
 * Invalid value，即缓存不可关闭、不可低于 32 MiB。
 *
 * <p>生效语义与驻留预算对称：调小立即对库内全部 OMAP 表按 LRU 淘汰到新一档；调大不
 * 主动预载，由后续查询自然填入缓存。每个 OMAP 表的切片缓存各自持有该预算（多表时
 * 总占用为预算 × 表数）。
 *
 * <p>持久化：值以规范化字节数存进 H2 目录的 {@code Setting}（{@code SETTING_NAME} 为
 * {@link OmapOptions#SLICE_CACHE_SETTING_NAME}），出现在 {@code INFORMATION_SCHEMA.SETTINGS}；
 * 重开库时重放 {@code SET} 语句对已存在的表生效，重放之后建的表在构造时从目录读同一值。
 */
public final class OmapSetOmapSliceCacheBytes extends Prepared {

    private final Expression value;

    public OmapSetOmapSliceCacheBytes(SessionLocal session, Expression value) {
        super(session);
        this.value = value;
    }

    @Override
    public boolean isTransactional() {
        return false;
    }

    @Override
    public ResultInterface queryMeta() {
        return null;
    }

    @Override
    public int getType() {
        return CommandInterface.SET;
    }

    @Override
    public long update() {
        Database database = getDatabase();
        session.getUser().checkAdmin();
        Value v = value.optimize(session).getValue(session);
        if (v.getValueType() == Value.NULL) {
            throw DbException.getInvalidValueException(OmapOptions.SLICE_CACHE_SETTING_NAME, "NULL");
        }
        long bytes = OmapOptions.parseByteSize(OmapOptions.SLICE_CACHE_SETTING_NAME, v.getString());
        if (bytes < OmapOptions.MIN_SLICE_CACHE_BYTES) {
            throw DbException.getInvalidValueException(OmapOptions.SLICE_CACHE_SETTING_NAME,
                    v.getString());
        }
        for (Schema schema : database.getAllSchemas()) {
            for (Table table : schema.getAllTablesAndViews(session)) {
                if (table instanceof OmapTable omapTable) {
                    omapTable.applySliceCacheBudget(bytes);
                }
            }
        }
        persistSetting(database, bytes);
        return 0;
    }

    /** 与 H2 自带 {@code Set.addOrUpdateSetting} 同一模式：值不变不重写目录。 */
    private void persistSetting(Database database, long bytes) {
        if (database.isReadOnly()) {
            return;
        }
        String canonical = Long.toString(bytes);
        synchronized (database) {
            Setting setting = database.findSetting(OmapOptions.SLICE_CACHE_SETTING_NAME);
            if (setting == null) {
                setting = new Setting(database, getObjectId(), OmapOptions.SLICE_CACHE_SETTING_NAME);
                setting.setStringValue(canonical);
                database.addDatabaseObject(session, setting);
            } else if (!canonical.equals(setting.getStringValue())) {
                setting.setStringValue(canonical);
                database.updateMeta(session, setting);
            }
        }
    }
}

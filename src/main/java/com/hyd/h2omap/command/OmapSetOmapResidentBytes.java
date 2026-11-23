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
 * {@code SET OMAP_RESIDENT_BYTES <带单位的数字>}。
 *
 * <p>数据库全局变量：库内全部 OMAP 表共享的冻结段驻留字节预算（真共享池，
 * 各表驻留量之和的软上限）。值是带单位的数字，后缀 k/m/g（不区分大小写，按 1024 换算），
 * 带后缀时需写成字符串字面量（如 {@code '8g'}），纯字节数可以不加引号；不接受 NULL。
 * 默认 0 表示不逐出；预算可随时调小到 0 关闭逐出。
 *
 * <p>生效语义与切片缓存预算对称：调小立即逐出到新一档；调大不主动预载，由后续查询
 * 自然填驻留。逐出判定在各表自己的写锁内进行、只能逐出本表的段，因此预算是软上限：
 * 长期无写入的冷表占用的驻留量不会被其他表的逐出动作清掉，超出量以冷表持量为上界。
 * 预算调整不改变数据，H2 结果缓存对重复查询仍直接命中。
 *
 * <p>持久化：值以规范化字节数存进 H2 目录的 {@code Setting}（{@code SETTING_NAME} 为
 * {@link OmapOptions#RESIDENT_SETTING_NAME}），出现在 {@code INFORMATION_SCHEMA.SETTINGS}；
 * 重开库时重放 {@code SET} 语句对已存在的表生效，重放之后建的表在构造时从目录读同一值。
 */
public final class OmapSetOmapResidentBytes extends Prepared {

    private final Expression value;

    public OmapSetOmapResidentBytes(SessionLocal session, Expression value) {
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
            throw DbException.getInvalidValueException(OmapOptions.RESIDENT_SETTING_NAME, "NULL");
        }
        // parseByteSize 拒绝负值与非法后缀；0 合法（关闭逐出）。
        long bytes = OmapOptions.parseByteSize(OmapOptions.RESIDENT_SETTING_NAME, v.getString());
        for (Schema schema : database.getAllSchemas()) {
            for (Table table : schema.getAllTablesAndViews(session)) {
                if (table instanceof OmapTable omapTable) {
                    omapTable.applyResidentBudget(bytes);
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
            Setting setting = database.findSetting(OmapOptions.RESIDENT_SETTING_NAME);
            if (setting == null) {
                setting = new Setting(database, getObjectId(), OmapOptions.RESIDENT_SETTING_NAME);
                setting.setStringValue(canonical);
                database.addDatabaseObject(session, setting);
            } else if (!canonical.equals(setting.getStringValue())) {
                setting.setStringValue(canonical);
                database.updateMeta(session, setting);
            }
        }
    }
}

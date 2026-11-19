/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.command;

import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.command.ddl.SchemaCommand;
import org.h2.engine.Right;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.message.DbException;
import org.h2.schema.Schema;
import org.h2.table.Table;
import org.h2.value.Value;

import com.hyd.h2omap.engine.OmapOptions;
import com.hyd.h2omap.table.OmapTable;

/**
 * {@code ALTER TABLE ... SET RESIDENT_BYTES <字节数>}。
 *
 * <p>运行时修改 OMAP 表的驻留字节预算，并同步进 H2 目录里的建表 SQL，重开库后按新值生效。
 * 预算调小立即逐出超预算的段；调大不重启预载，由后续查询自然填驻留。
 */
public final class OmapAlterTableSetResidentBytes extends SchemaCommand {

    private boolean ifTableExists;

    private String tableName;

    private final Expression value;

    public OmapAlterTableSetResidentBytes(SessionLocal session, Schema schema, Expression value) {
        super(session, schema);
        this.value = value;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setIfTableExists(boolean ifTableExists) {
        this.ifTableExists = ifTableExists;
    }

    @Override
    public boolean isTransactional() {
        return true;
    }

    @Override
    public long update() {
        Table table = getSchema().resolveTableOrView(session, tableName);
        if (table == null) {
            if (ifTableExists) {
                return 0;
            }
            throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, tableName);
        }
        if (!(table instanceof OmapTable omapTable)) {
            throw DbException.getUnsupportedException("RESIDENT_BYTES");
        }
        session.getUser().checkTableRight(table, Right.SCHEMA_OWNER);
        table.lock(session, Table.EXCLUSIVE_LOCK);
        Value v = value.getValue(session);
        if (v.getValueType() == Value.NULL) {
            throw DbException.getInvalidValueException("RESIDENT_BYTES", "NULL");
        }
        long bytes = OmapOptions.parseByteSize("residentBytes", v.getString());
        omapTable.applyResidentBudget(bytes);
        session.getDatabase().updateMeta(session, table);
        return 0;
    }

    @Override
    public int getType() {
        // 语句族与 ALTER TABLE SET 相同；H2 侧没有对该类型的其他分发。
        return CommandInterface.ALTER_TABLE_SET_REFERENTIAL_INTEGRITY;
    }
}

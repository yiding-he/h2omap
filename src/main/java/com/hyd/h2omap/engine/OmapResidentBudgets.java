/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.engine;

import java.util.concurrent.ConcurrentHashMap;

import org.h2.engine.Database;
import org.h2.engine.Setting;

import com.hyd.h2omap.storage.ResidentBudget;

/**
 * 按 Database 归一的共享驻留预算注册表：一个库内全部 OMAP 表拿到同一个
 * {@link ResidentBudget} 实例。最后一个 OMAP 表关闭时移除注册表条目。
 *
 * <p>预算初值从 H2 目录的 Setting（{@link OmapOptions#RESIDENT_SETTING_NAME}）读，
 * 未设置过用 {@link OmapOptions#DEFAULT_RESIDENT_BYTES}（0，不逐出）；
 * 运行期修改由 {@code SET OMAP_RESIDENT_BYTES} 命令完成。
 */
public final class OmapResidentBudgets {

    private static final ConcurrentHashMap<Database, ResidentBudget> BY_DATABASE =
            new ConcurrentHashMap<>();

    private OmapResidentBudgets() {
    }

    public static ResidentBudget of(Database database) {
        return BY_DATABASE.computeIfAbsent(database, d -> new ResidentBudget(readPersisted(d)));
    }

    /** 最后一个 OMAP 表关闭时移除条目，避免 Database 对象被静态表强引用。 */
    public static void release(Database database, ResidentBudget budget) {
        BY_DATABASE.remove(database, budget);
    }

    private static long readPersisted(Database database) {
        Setting setting = database.findSetting(OmapOptions.RESIDENT_SETTING_NAME);
        if (setting != null) {
            String value = setting.getStringValue();
            if (value != null && !value.isEmpty()) {
                try {
                    return OmapOptions.parseByteSize(OmapOptions.RESIDENT_SETTING_NAME, value);
                } catch (RuntimeException ignore) {
                    // 目录里的值异常时退回默认，不让建表失败
                }
            }
        }
        return OmapOptions.DEFAULT_RESIDENT_BYTES;
    }
}

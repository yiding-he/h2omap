/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.engine;

import java.util.List;

import org.h2.message.DbException;

/**
 * {@code CREATE TABLE ... WITH "key=value", ...} 里的引擎参数。
 *
 * <p>H2 把 {@code WITH} 后面逗号分隔的每个双引号标识符当成一个参数
 * （见 {@code org/h2/command/Parser.readTableEngineParams}），
 * 本类只负责拆出 {@code key=value}。未知的键直接报错，
 * 避免拼写错误被静默忽略。
 */
public final class OmapOptions {

    /** 每个列段的目标行数。段是不可变的，写满就换新段。 */
    public static final int DEFAULT_SEGMENT_SIZE = 65536;

    /** delta 攒到多少行之后触发合并。 */
    public static final int DEFAULT_COMPACTION_MIN_ROWS = 16384;

    /** 后台 WAL 刷盘间隔（毫秒）。 */
    public static final int DEFAULT_SYNC_INTERVAL_MS = 200;

    /** 冻结段驻留堆的估算字节数预算，0 表示不逐出。 */
    public static final long DEFAULT_RESIDENT_BYTES = 0;

    /** 逐出段列切片缓存的数据库全局预算下限（字节），{@code SET OMAP_SLICE_CACHE_BYTES} 不接受更小值。 */
    public static final long MIN_SLICE_CACHE_BYTES = 32L * 1024 * 1024;

    /** 逐出段列切片缓存的默认预算（字节），未设置过 {@code SET OMAP_SLICE_CACHE_BYTES} 时生效。 */
    public static final long DEFAULT_SLICE_CACHE_BYTES = MIN_SLICE_CACHE_BYTES;

    /** {@code SET OMAP_SLICE_CACHE_BYTES} 在数据库目录里的设置名。 */
    public static final String SLICE_CACHE_SETTING_NAME = "OMAP_SLICE_CACHE_BYTES";

    private final int segmentSize;
    private final int compactionMinRows;
    private final Boolean backup;
    private final String backupDir;
    private final int syncIntervalMs;

    /** 驻留预算可被 ALTER TABLE SET RESIDENT_BYTES 运行时修改，因此非 final。 */
    private volatile long residentBytes;

    private OmapOptions(int segmentSize, int compactionMinRows,
            Boolean backup, String backupDir, int syncIntervalMs, long residentBytes) {
        this.segmentSize = segmentSize;
        this.compactionMinRows = compactionMinRows;
        this.backup = backup;
        this.backupDir = backupDir;
        this.syncIntervalMs = syncIntervalMs;
        this.residentBytes = residentBytes;
    }

    public static OmapOptions defaults() {
        return new OmapOptions(DEFAULT_SEGMENT_SIZE, DEFAULT_COMPACTION_MIN_ROWS,
                null, null, DEFAULT_SYNC_INTERVAL_MS, DEFAULT_RESIDENT_BYTES);
    }

    /**
     * 解析 WITH 参数。null 或空列表返回默认配置。
     *
     * @param params H2 传入的 tableEngineParams
     * @return 配置
     */
    public static OmapOptions from(List<String> params) {
        if (params == null || params.isEmpty()) {
            return defaults();
        }
        int segmentSize = DEFAULT_SEGMENT_SIZE;
        int compactionMinRows = DEFAULT_COMPACTION_MIN_ROWS;
        Boolean backup = null;
        String backupDir = null;
        int syncIntervalMs = DEFAULT_SYNC_INTERVAL_MS;
        long residentBytes = DEFAULT_RESIDENT_BYTES;
        for (String param : params) {
            String text = param == null ? "" : param.trim();
            if (text.isEmpty()) {
                continue;
            }
            int eq = text.indexOf('=');
            if (eq < 0) {
                throw DbException.getInvalidValueException("OMAP parameter", text);
            }
            String key = text.substring(0, eq).trim();
            String value = text.substring(eq + 1).trim();
            switch (key) {
            case "segmentSize":
                segmentSize = parseInt(key, value);
                break;
            case "compactionMinRows":
                compactionMinRows = parseInt(key, value);
                break;
            case "backup":
                if ("true".equalsIgnoreCase(value)) {
                    backup = Boolean.TRUE;
                } else if ("false".equalsIgnoreCase(value)) {
                    backup = Boolean.FALSE;
                } else {
                    throw DbException.getInvalidValueException("OMAP parameter " + key, value);
                }
                break;
            case "backupDir":
                backupDir = value.isEmpty() ? null : value;
                break;
            case "syncIntervalMs":
                syncIntervalMs = parseInt(key, value);
                break;
            case "residentBytes":
                residentBytes = parseByteSize(key, value);
                break;
            default:
                throw DbException.getInvalidValueException("OMAP parameter name", key);
            }
        }
        if (segmentSize <= 0 || compactionMinRows <= 0 || syncIntervalMs <= 0 || residentBytes < 0) {
            throw DbException.getInvalidValueException("OMAP parameter", params.toString());
        }
        return new OmapOptions(segmentSize, compactionMinRows, backup, backupDir, syncIntervalMs,
                residentBytes);
    }

    private static int parseInt(String key, String value) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw DbException.getInvalidValueException("OMAP parameter " + key, value);
        }
    }

    public int getSegmentSize() {
        return segmentSize;
    }

    public int getCompactionMinRows() {
        return compactionMinRows;
    }

    public Boolean getBackup() {
        return backup;
    }

    public String getBackupDir() {
        return backupDir;
    }

    /**
     * 解析字节数：纯数字按字节，可跟单位后缀 k/m/g（不区分大小写，按 1024 换算），
     * 如 40960、40k、8g。建表 WITH 参数与 ALTER TABLE SET RESIDENT_BYTES 共用。
     */
    public static long parseByteSize(String key, String value) {
        String s = value.trim();
        int split = 0;
        while (split < s.length() && Character.isDigit(s.charAt(split))) {
            split++;
        }
        String number = s.substring(0, split);
        long multiplier;
        switch (s.substring(split).toLowerCase(java.util.Locale.ROOT)) {
        case "":
            multiplier = 1;
            break;
        case "k":
            multiplier = 1024L;
            break;
        case "m":
            multiplier = 1024L * 1024;
            break;
        case "g":
            multiplier = 1024L * 1024 * 1024;
            break;
        default:
            throw DbException.getInvalidValueException("OMAP parameter " + key, value);
        }
        if (number.isEmpty()) {
            throw DbException.getInvalidValueException("OMAP parameter " + key, value);
        }
        long n;
        try {
            n = Long.parseLong(number);
        } catch (NumberFormatException e) {
            throw DbException.getInvalidValueException("OMAP parameter " + key, value);
        }
        if (n < 0 || n > Long.MAX_VALUE / multiplier) {
            throw DbException.getInvalidValueException("OMAP parameter " + key, value);
        }
        return n * multiplier;
    }

    public int getSyncIntervalMs() {
        return syncIntervalMs;
    }

    /** 冻结段驻留堆的字节预算，0 表示不逐出。 */
    public long getResidentBytes() {
        return residentBytes;
    }

    /** 运行时修改驻留预算，调用方负责保证值非负。 */
    public void setResidentBytes(long residentBytes) {
        this.residentBytes = residentBytes;
    }
}

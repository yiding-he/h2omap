/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.engine;

import java.util.List;

import org.h2.message.DbException;

/**
 * {@code CREATE TABLE ... WITH 'key=value', ...} 里的引擎参数。
 *
 * <p>参数按逗号分隔由 H2 解析成 {@code List<String>}，本类只负责拆出
 * {@code key=value}。未知的键直接报错，避免拼写错误被静默忽略。
 */
public final class OmapOptions {

    /** 每个列段的目标行数。段是不可变的，写满就换新段。 */
    public static final int DEFAULT_SEGMENT_SIZE = 65536;

    /** delta 攒到多少行之后触发合并。 */
    public static final int DEFAULT_COMPACTION_MIN_ROWS = 16384;

    private final int segmentSize;
    private final int compactionMinRows;

    private OmapOptions(int segmentSize, int compactionMinRows) {
        this.segmentSize = segmentSize;
        this.compactionMinRows = compactionMinRows;
    }

    public static OmapOptions defaults() {
        return new OmapOptions(DEFAULT_SEGMENT_SIZE, DEFAULT_COMPACTION_MIN_ROWS);
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
            default:
                throw DbException.getInvalidValueException("OMAP parameter name", key);
            }
        }
        if (segmentSize <= 0 || compactionMinRows <= 0) {
            throw DbException.getInvalidValueException("OMAP parameter", params.toString());
        }
        return new OmapOptions(segmentSize, compactionMinRows);
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
}

/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.storage;

/**
 * rowId 编码。一个 long 同时表达“在哪”和“哪一行”。
 *
 * <pre>
 * 段内行：0000_0000_0000_0000 | 段号(31 位) | 段内偏移(32 位)
 * </pre>
 *
 * <p>H2 通过 {@code SearchRow#getKey} / {@code setKey} 传 rowId，
 * 所以 Cursor 取行时必须把 rowId 写回 {@code Row}，
 * 否则后面的 UPDATE 和 DELETE 找不到行。
 */
public final class RowId {

    private RowId() {
    }

    public static long ofSegment(int segmentId, int offset) {
        return ((long) segmentId << 32) | (offset & 0xffffffffL);
    }

    public static int segmentId(long rowId) {
        return (int) (rowId >>> 32);
    }

    public static int offset(long rowId) {
        return (int) rowId;
    }
}

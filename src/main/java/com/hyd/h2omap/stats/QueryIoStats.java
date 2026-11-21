/*
 * 查询期段文件 IO 统计。
 *
 * <p>开关：服务端系统属性 {@code -Domap.ioStats=true}，默认关闭。关闭时
 * {@link #record} 立即返回，不产生任何分配。
 *
 * <p>归组口径：以一次 {@code Command.executeQuery} 为边界（含该命令内的全部子查询），
 * 记录每一次段文件读取的段号、代数、列集合与搬运字节数。统计经 ThreadLocal 从命令执行
 * 线程传给 JDBC 层，由 {@code JdbcStatement.getMoreResults()} 取走，服务端不留累积状态。
 */
package com.hyd.h2omap.stats;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 单次查询的段文件读取明细。
 *
 * <p>由 {@code OmapPersistence} 的读取方法写入，由替换后的 {@code org.h2.jdbc.JdbcStatement}
 * 通过 {@link #pollLast()} 取走并构造第二个结果集。
 */
public final class QueryIoStats {

    /** 是否采集段文件 IO 明细，初始由服务端系统属性 {@code omap.ioStats} 决定。 */
    private static volatile boolean enabled = Boolean.getBoolean("omap.ioStats");

    /**
     * 当前是否采集段文件 IO 明细。
     *
     * @return 是否开启
     */
    public static boolean isEnabled() {
        return enabled;
    }

    /**
     * 设置是否采集段文件 IO 明细。
     *
     * @param value 是否开启
     */
    public static void setEnabled(boolean value) {
        enabled = value;
    }

    /** 单次查询最多记录的读取条目数，超出后只计数不再存明细。 */
    private static final int MAX_RECORDS = 8192;

    private static final ThreadLocal<Collector> CURRENT = new ThreadLocal<>();
    private static final ThreadLocal<QueryIoStats> LAST = new ThreadLocal<>();

    private final List<Record> records;
    private final boolean truncated;
    private final long totalBytes;
    private final long totalFooterBytes;
    private final long totalIoReads;

    private QueryIoStats(List<Record> records, boolean truncated, long totalBytes, long totalFooterBytes,
            long totalIoReads) {
        this.records = records;
        this.truncated = truncated;
        this.totalBytes = totalBytes;
        this.totalFooterBytes = totalFooterBytes;
        this.totalIoReads = totalIoReads;
    }

    /**
     * 开始一次查询的统计收集。同一线程上的嵌套命令共用同一个收集器。
     */
    public static void begin() {
        if (!enabled) {
            return;
        }
        Collector c = CURRENT.get();
        if (c == null) {
            c = new Collector();
            CURRENT.set(c);
        }
        c.depth++;
    }

    /**
     * 结束一次查询的统计收集。最外层结束时把结果发布给 JDBC 层取用。
     */
    public static void end() {
        if (!enabled) {
            return;
        }
        Collector c = CURRENT.get();
        if (c == null) {
            return;
        }
        if (--c.depth == 0) {
            CURRENT.remove();
            LAST.set(c.toStats());
        }
    }

    /**
     * 取走最近一次查询的统计，没有则返回 {@code null}。
     */
    public static QueryIoStats pollLast() {
        QueryIoStats s = LAST.get();
        LAST.remove();
        return s;
    }

    /**
     * 记录一次段文件读取。
     *
     * @param segmentId 段号
     * @param generation 段的代数
     * @param columns 列集合的文本表示，整段读为 {@code "*"}，部分列读为逗号分隔的列 id
     * @param bytes 搬运的字节数（列数据）
     * @param footerBytes 尾部目录等固定开销的字节数
     * @param ioReads 提交给文件通道的读请求次数，用于判断读取是否碎片化
     */
    public static void record(int segmentId, int generation, String columns, long bytes, long footerBytes,
            int ioReads) {
        if (!enabled) {
            return;
        }
        Collector c = CURRENT.get();
        if (c == null) {
            return;
        }
        c.add(segmentId, generation, columns, bytes, footerBytes, ioReads);
    }

    public boolean isEmpty() {
        return records.isEmpty();
    }

    public int size() {
        return records.size();
    }

    public Record get(int index) {
        return records.get(index);
    }

    public List<Record> records() {
        return Collections.unmodifiableList(records);
    }

    public boolean isTruncated() {
        return truncated;
    }

    /** 列数据字节合计。 */
    public long getTotalBytes() {
        return totalBytes;
    }

    /** 尾部目录等固定开销字节合计。 */
    public long getTotalFooterBytes() {
        return totalFooterBytes;
    }

    /** 提交给文件通道的读请求次数合计。 */
    public long getTotalIoReads() {
        return totalIoReads;
    }

    /**
     * 平均每次读请求搬运的字节数。
     *
     * @return 平均字节数，没有读请求时为 0
     */
    public long getAverageIoReadBytes() {
        return totalIoReads == 0 ? 0 : totalBytes / totalIoReads;
    }

    /**
     * 一条段文件读取记录。
     */
    public record Record(int segmentId, int generation, String columns, long dataBytes, long footerBytes,
            int ioReads) {
    }

    private static final class Collector {

        int depth;
        private final ArrayList<Record> records = new ArrayList<>();
        private boolean truncated;
        private long totalBytes;
        private long totalFooterBytes;
        private long totalIoReads;

        void add(int segmentId, int generation, String columns, long bytes, long footerBytes, int ioReads) {
            totalBytes += bytes;
            totalFooterBytes += footerBytes;
            totalIoReads += ioReads;
            if (records.size() >= MAX_RECORDS) {
                truncated = true;
                return;
            }
            records.add(new Record(segmentId, generation, columns, bytes, footerBytes, ioReads));
        }

        QueryIoStats toStats() {
            return new QueryIoStats(new ArrayList<>(records), truncated, totalBytes, totalFooterBytes,
                    totalIoReads);
        }
    }
}

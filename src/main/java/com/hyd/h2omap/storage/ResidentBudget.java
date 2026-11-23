/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.storage;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 数据库内全部 OMAP 表共享的驻留字节预算（真共享池）。
 *
 * <p>预算 0 表示不逐出。全局占用是全部注册表当前驻留字节之和；逐出判定在各表自己的
 * 写锁内进行，只能逐出本表的段，因此预算是软上限：一张长期无写入的冷表占用的驻留量
 * 不会被热表的逐出动作清掉（超出量以冷表持量为上界），持续写入的表会把自己逐到预算内。
 *
 * <p>预算值由 {@code SET OMAP_RESIDENT_BYTES} 下推并持久化进 H2 目录的 Setting；
 * 各表共享同一个本类实例（经 {@code OmapResidentBudgets} 按 Database 归一），
 * 与切片缓存「每表各自持有同一预算值」的口径不同。
 */
public final class ResidentBudget {

    /** 预算 0 表示不逐出；volatile 保证 SET 命令的写入对逐出触点立即可见。 */
    private volatile long budget;

    /** 全库驻留字节数的无锁账本：各表在自己的写锁内经 {@link #add} 增量维护，
     * 求和不逐个加其他表的锁（跨表锁序会死锁）。 */
    private final AtomicLong used = new AtomicLong();

    private final List<ColumnarStore> stores = new CopyOnWriteArrayList<>();

    public ResidentBudget(long budget) {
        this.budget = budget;
    }

    public long getBudget() {
        return budget;
    }

    public void setBudget(long budget) {
        this.budget = budget;
    }

    void register(ColumnarStore store) {
        stores.add(store);
    }

    /** 表关闭时注销；返回 true 表示本预算已无注册表，调用方据此移除库级注册条目。 */
    public boolean unregister(ColumnarStore store) {
        stores.remove(store);
        return stores.isEmpty();
    }

    /** 表在自己写锁内调用，增量维护全库驻留字节。 */
    void add(long delta) {
        used.addAndGet(delta);
    }

    /** 全部注册表当前驻留字节之和（无锁读）。 */
    public long residentUsed() {
        return used.get();
    }
}

/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.vectorized;

import org.h2.value.Value;

/**
 * 有序流式执行（M20）的行源：每次调用取下一行，返回 {@code null} 表示结束。
 *
 * <p>调用方（替换后的 {@code org.h2.command.query.Select}）把它包进
 * {@code LazyResultSelect} 逐行驱动：客户端取一行才推进一次，停止取数扫描即停。
 */
public interface OrderedStreamingRowSource {

    /** 取下一行；行已按排序口径就位；返回 {@code null} 表示结果集结束。 */
    Value[] nextRow();

    /** 重置到结果集开头：重新截取向量化快照并重放 OFFSET。 */
    void reset();
}

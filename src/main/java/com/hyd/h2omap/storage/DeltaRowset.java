/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.storage;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

import org.h2.value.CompareMode;
import org.h2.value.TypeInfo;
import org.h2.value.Value;

import com.hyd.h2omap.stats.ZoneMap;
import com.hyd.h2omap.vector.ColumnVector;

/**
 * 已冻结段更新新像的列式 rowset（MOR 批次 2）。
 *
 * <p>取代 {@code Map<rowId, Value[]>}：新像按列向量存放（复用段向量的
 * 类型特化与打包编码），每列带自己的 zone map——新像的值域与旧段无关，
 * 向量化路径据此整组跳过不可能命中的新像，不再逐行求值。
 *
 * <p>位置不可变：重复更新同一行时追加新位置、旧位置标死。因此锁外读取
 * 任意位置都是安全的，合并的锁外构建阶段直接按位置物化，不再依赖
 * 值引用比对。死位置在合并后超过半数时整体重排回收。
 *
 * <p>不是线程安全的，调用方（{@link DeltaStore}）负责加锁。
 */
final class DeltaRowset {

    private static final int INITIAL_CAPACITY = 16;

    private final TypeInfo[] columnTypes;

    private final CompareMode compareMode;

    private ColumnVector[] columns;

    private long[] rowIds;

    private int filled;

    private int live;

    private final HashMap<Long, Integer> positionByRowId = new HashMap<>();

    private final BitSet dead = new BitSet();

    private ZoneMap[] zoneMaps;

    private boolean zonesDirty = true;

    DeltaRowset(TypeInfo[] columnTypes, CompareMode compareMode) {
        this.columnTypes = columnTypes;
        this.compareMode = compareMode;
        this.columns = newColumns(INITIAL_CAPACITY);
        this.rowIds = new long[INITIAL_CAPACITY];
    }

    private ColumnVector[] newColumns(int capacity) {
        ColumnVector[] vectors = new ColumnVector[columnTypes.length];
        for (int c = 0; c < columnTypes.length; c++) {
            vectors[c] = ColumnVector.create(columnTypes[c].getValueType(), capacity);
        }
        return vectors;
    }

    /** 记录一行的新像；重复更新追加新位置、旧位置标死（位置不可变）。 */
    void upsert(long rowId, Value[] values) {
        ensureCapacity(filled + 1);
        for (int c = 0; c < columns.length; c++) {
            columns[c].set(filled, values[c]);
        }
        rowIds[filled] = rowId;
        Integer old = positionByRowId.put(rowId, filled);
        if (old != null) {
            dead.set(old);
        } else {
            live++;
        }
        filled++;
        zonesDirty = true;
    }

    private void ensureCapacity(int required) {
        if (required <= rowIds.length) {
            return;
        }
        int newCapacity = Math.max(required, rowIds.length * 2);
        long[] newRowIds = new long[newCapacity];
        System.arraycopy(rowIds, 0, newRowIds, 0, filled);
        rowIds = newRowIds;
        ColumnVector[] newColumns = newColumns(newCapacity);
        for (int c = 0; c < columns.length; c++) {
            for (int r = 0; r < filled; r++) {
                newColumns[c].set(r, columns[c].get(r));
            }
        }
        columns = newColumns;
    }

    /** 取一行的新像；该行没有新像时返回 {@code null}。 */
    Value[] get(long rowId) {
        Integer pos = positionByRowId.get(rowId);
        return pos == null ? null : materializeAt(pos);
    }

    /** 按不可变位置物化一行。 */
    Value[] materializeAt(int pos) {
        Value[] out = new Value[columns.length];
        for (int c = 0; c < columns.length; c++) {
            out[c] = columns[c].get(pos);
        }
        return out;
    }

    /** 行随删除消失：撤掉它的新像。返回该行原本是否有活像。 */
    boolean remove(long rowId) {
        Integer pos = positionByRowId.remove(rowId);
        if (pos == null) {
            return false;
        }
        dead.set(pos);
        live--;
        zonesDirty = true;
        return true;
    }

    /** 仅当该行仍指向快照时的位置才撤掉，防止误删合并锁外期间写入的新更新。 */
    boolean removeIfSamePosition(long rowId, int position) {
        Integer pos = positionByRowId.get(rowId);
        if (pos == null || pos != position) {
            return false;
        }
        remove(rowId);
        return true;
    }

    int liveCount() {
        return live;
    }

    boolean isEmpty() {
        return live == 0;
    }

    /** 活位置升序快照。位置不可变，快照在锁外读取仍然安全。 */
    int[] livePositions() {
        int[] positions = new int[live];
        int p = 0;
        for (int i = 0; i < filled && p < live; i++) {
            if (!dead.get(i)) {
                positions[p++] = i;
            }
        }
        return positions;
    }

    long rowIdAt(int pos) {
        return rowIds[pos];
    }

    /** 全部活像物化成 rowId 到值的映射，供 checkpoint 写 tail 使用。 */
    Map<Long, Value[]> snapshotEntries() {
        Map<Long, Value[]> out = new HashMap<>(live * 2);
        for (int i = 0; i < filled; i++) {
            if (!dead.get(i)) {
                out.put(rowIds[i], materializeAt(i));
            }
        }
        return out;
    }

    /** 某列的 zone map，覆盖 [0, filled)（死位置的旧值只会让区间偏宽，剪枝仍然保守正确）。 */
    ZoneMap zoneMap(int column) {
        if (zonesDirty) {
            ZoneMap[] rebuilt = new ZoneMap[columns.length];
            for (int c = 0; c < columns.length; c++) {
                rebuilt[c] = ZoneMap.of(columns[c], 0, filled, compareMode);
            }
            zoneMaps = rebuilt;
            zonesDirty = false;
        }
        return zoneMaps[column];
    }

    void clear() {
        columns = newColumns(INITIAL_CAPACITY);
        rowIds = new long[INITIAL_CAPACITY];
        filled = 0;
        live = 0;
        positionByRowId.clear();
        dead.clear();
        zonesDirty = true;
    }

    /** 死位置超过半数时重排回收，把活像压回位置前端。调用方持有写锁。 */
    void rebuildIfWasteful() {
        if (dead.cardinality() <= live) {
            return;
        }
        int[] positions = livePositions();
        ColumnVector[] newColumns = newColumns(Math.max(INITIAL_CAPACITY, positions.length * 2));
        long[] newRowIds = new long[Math.max(INITIAL_CAPACITY, positions.length * 2)];
        positionByRowId.clear();
        for (int i = 0; i < positions.length; i++) {
            int pos = positions[i];
            for (int c = 0; c < columns.length; c++) {
                newColumns[c].set(i, columns[c].get(pos));
            }
            newRowIds[i] = rowIds[pos];
            positionByRowId.put(rowIds[pos], i);
        }
        columns = newColumns;
        rowIds = newRowIds;
        filled = positions.length;
        dead.clear();
        zonesDirty = true;
    }
}

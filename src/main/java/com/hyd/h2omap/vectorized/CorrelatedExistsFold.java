package com.hyd.h2omap.vectorized;

import java.util.BitSet;
import java.util.HashSet;
import java.util.Set;

import org.h2.command.query.Select;
import org.h2.engine.SessionLocal;
import org.h2.table.TableFilter;
import org.h2.value.CompareMode;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueNull;

import com.hyd.h2omap.storage.ColumnSegment;
import com.hyd.h2omap.storage.ColumnarStore.VectorizedSnapshot;
import com.hyd.h2omap.table.OmapTable;
import com.hyd.h2omap.vector.ColumnVector;
import com.hyd.h2omap.vector.IntVector;
import com.hyd.h2omap.vector.LongVector;
import com.hyd.h2omap.vector.NullBitmap;


/**
 * S4：关联 {@code EXISTS} / {@code NOT EXISTS}（单列等值关联）的折叠。
 *
 * <p>对内层表一次向量化快照预计算关联键集合，外层每行按键做半连接判定。
 * 全部方法在 {@code VectorizedFilter.compile} 编译期调用，不持有存储锁。
 */
final class CorrelatedExistsFold {

    private CorrelatedExistsFold() {
    }

    /** 编译关联 {@code EXISTS} / {@code NOT EXISTS}：预计算内层关联键集合，折成半连接节点。不识别返回 {@code null}。 */
    static VectorizedFilter.Node compileCorrelated(Select select, boolean not,
            TableFilter filter, SessionLocal session) {
        SubqueryInner.InnerQuery inner = SubqueryInner.parseInner(select, filter, session, false);
        if (inner == null) {
            return null;
        }
        Set<Object> keys = precomputeKeySet(inner, session);
        if (keys == null) {
            return null;
        }
        return new SemiJoinNode(inner.outerKeyCol, keys, not, inner.outerKeyType);
    }

    /** 预计算内层关联键集合；键规范化到外层键列类型。失败返回 {@code null}。 */
    private static Set<Object> precomputeKeySet(SubqueryInner.InnerQuery inner, SessionLocal session) {
        OmapTable table = (OmapTable) inner.innerFilter.getTable();
        CompareMode compareMode = table.getCompareMode();
        TypeInfo keyType = inner.outerKeyType;
        boolean intKeys = keyType.getValueType() == Value.INTEGER;
        boolean longKeys = keyType.getValueType() == Value.BIGINT;
        Set<Object> keys = new HashSet<>();
        VectorizedSnapshot snapshot = table.getStore().takeVectorizedSnapshot();
        try {
            int[] count = new int[1];
            for (ColumnSegment segment : snapshot.segments) {
                int[] sel = SubqueryInner.innerSelection(inner, segment, snapshot, session, compareMode, count);
                int selCount = count[0];
                if (selCount == 0) {
                    continue;
                }
                ColumnVector keyVec = segment.getColumn(inner.innerKeyCol);
                if (intKeys && keyVec instanceof IntVector) {
                    int[] raw = ((IntVector) keyVec).rawValues();
                    NullBitmap nulls = keyVec.nulls();
                    for (int i = 0; i < selCount; i++) {
                        int idx = sel == null ? i : sel[i];
                        if (!nulls.isNull(idx)) {
                            keys.add(raw[idx]);
                        }
                    }
                } else if (longKeys && keyVec instanceof LongVector) {
                    long[] raw = ((LongVector) keyVec).rawValues();
                    NullBitmap nulls = keyVec.nulls();
                    for (int i = 0; i < selCount; i++) {
                        int idx = sel == null ? i : sel[i];
                        if (!nulls.isNull(idx)) {
                            keys.add(raw[idx]);
                        }
                    }
                } else {
                    for (int i = 0; i < selCount; i++) {
                        int idx = sel == null ? i : sel[i];
                        Value k = keyVec.get(idx);
                        if (k != ValueNull.INSTANCE) {
                            keys.add(SubqueryInner.normalizeKey(k, keyType, session));
                        }
                    }
                }
            }
            for (Value[] row : snapshot.extraRows) {
                if (inner.constFilter != null
                        && !inner.constFilter.evalRow(row, session, compareMode)) {
                    continue;
                }
                Value k = row[inner.innerKeyCol];
                if (k != ValueNull.INSTANCE) {
                    keys.add(SubqueryInner.normalizeKey(k, keyType, session));
                }
            }
        } catch (RuntimeException e) {
            return null;
        }
        return keys;
    }

    /** 半连接：外层键命中预计算键集合则通过。{@code NOT EXISTS} 时取反；NULL 键视为未命中。 */
    static final class SemiJoinNode implements VectorizedFilter.Node {
        private final int keyCol;
        private final Set<Object> keys;
        private final boolean not;
        private final TypeInfo outerKeyType;

        SemiJoinNode(int keyCol, Set<Object> keys, boolean not, TypeInfo outerKeyType) {
            this.keyCol = keyCol;
            this.keys = keys;
            this.not = not;
            this.outerKeyType = outerKeyType;
        }

        @Override
        public void collectColumns(java.util.function.IntConsumer sink) {
            sink.accept(keyCol);
        }

        @Override
        public int filterSegment(ColumnSegment segment, int[] sel, int selCount, boolean fullRange,
                BitSet masked, SessionLocal session, CompareMode compareMode) {
            ColumnVector keyVec = segment.getColumn(keyCol);
            int out = 0;
            if (keyVec instanceof IntVector) {
                int[] raw = ((IntVector) keyVec).rawValues();
                NullBitmap nulls = keyVec.nulls();
                if (fullRange) {
                    for (int i = 0; i < selCount; i++) {
                        if (match(nulls.isNull(i) ? null : raw[i])) {
                            sel[out++] = i;
                        }
                    }
                } else {
                    for (int i = 0; i < selCount; i++) {
                        int idx = sel[i];
                        if (match(nulls.isNull(idx) ? null : raw[idx])) {
                            sel[out++] = idx;
                        }
                    }
                }
                return out;
            }
            if (keyVec instanceof LongVector) {
                long[] raw = ((LongVector) keyVec).rawValues();
                NullBitmap nulls = keyVec.nulls();
                if (fullRange) {
                    for (int i = 0; i < selCount; i++) {
                        if (match(nulls.isNull(i) ? null : raw[i])) {
                            sel[out++] = i;
                        }
                    }
                } else {
                    for (int i = 0; i < selCount; i++) {
                        int idx = sel[i];
                        if (match(nulls.isNull(idx) ? null : raw[idx])) {
                            sel[out++] = idx;
                        }
                    }
                }
                return out;
            }
            if (fullRange) {
                for (int i = 0; i < selCount; i++) {
                    Value k = keyVec.get(i);
                    if (match(k == ValueNull.INSTANCE ? null
                            : SubqueryInner.normalizeKey(k, outerKeyType, session))) {
                        sel[out++] = i;
                    }
                }
            } else {
                for (int i = 0; i < selCount; i++) {
                    int idx = sel[i];
                    Value k = keyVec.get(idx);
                    if (match(k == ValueNull.INSTANCE ? null
                            : SubqueryInner.normalizeKey(k, outerKeyType, session))) {
                        sel[out++] = idx;
                    }
                }
            }
            return out;
        }

        private boolean match(Object key) {
            return (key != null && keys.contains(key)) ^ not;
        }

        @Override
        public boolean evalRow(Value[] row, SessionLocal session, CompareMode compareMode) {
            Value k = row[keyCol];
            return match(k == ValueNull.INSTANCE ? null
                    : SubqueryInner.normalizeKey(k, outerKeyType, session));
        }
    }
}

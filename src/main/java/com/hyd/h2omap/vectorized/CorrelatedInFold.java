package com.hyd.h2omap.vectorized;

import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.h2.command.query.Query;
import org.h2.command.query.Select;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.table.TableFilter;
import org.h2.value.CompareMode;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueNull;

import com.hyd.h2omap.storage.ColumnSegment;
import com.hyd.h2omap.storage.VectorizedSnapshot;
import com.hyd.h2omap.table.OmapTable;
import com.hyd.h2omap.vector.ColumnVector;
import com.hyd.h2omap.vector.DictValueVector;
import com.hyd.h2omap.vector.DictVarcharVector;
import com.hyd.h2omap.vector.IntVector;
import com.hyd.h2omap.vector.LongVector;
import com.hyd.h2omap.vector.NullBitmap;

/**
 * S5：关联 {@code IN} / {@code NOT IN (SELECT ...)}（单列等值关联、内层投影为列引用）的折叠。
 *
 * <p>编译期对内层表一次向量化快照，建「归一化关联键 → 内层值集合」；外层每行按键取集合
 * 做成员判定。整数族值列走原始 {@code long} 集合，字典键列按码预解析集合引用；
 * {@code NOT IN} 按 SQL 三值语义，UNKNOWN 不命中。全部方法在
 * {@code VectorizedFilter.compile} 编译期调用，不持有存储锁。
 */
final class CorrelatedInFold {

    private CorrelatedInFold() {
    }

    /** 编译关联 {@code IN (SELECT y FROM 内层表 WHERE 内层.k = 外层.K [AND 内层常量谓词])}。内层投影必须是内层表的列引用；不识别返回 {@code null} 交回 H2。 */
    static VectorizedFilter.Node compileCorrelatedIn(boolean not, int col, Query query,
            TableFilter filter, SessionLocal session) {
        if (!(query instanceof Select)) {
            return null;
        }
        Select select = (Select) query;
        SubqueryInner.InnerQuery inner = SubqueryInner.parseInner(select, filter, session, false);
        if (inner == null) {
            return null;
        }
        List<Expression> exprs = select.getExpressions();
        if (exprs.size() != 1) {
            return null;
        }
        Expression proj = exprs.get(0).getNonAliasExpression();
        if (!(proj instanceof ExpressionColumn)) {
            return null;
        }
        ExpressionColumn projCol = (ExpressionColumn) proj;
        if (projCol.getTableFilter() != inner.innerFilter) {
            return null;
        }
        int innerValCol = projCol.getColumn().getColumnId();
        TypeInfo outerValType = filter.getTable().getColumn(col).getType();
        Map<Object, KeyMembership> sets = precomputeKeyedSets(inner, innerValCol, outerValType, session);
        if (sets == null) {
            return null;
        }
        if (sets.isEmpty()) {
            // 内层没有任何非 NULL 关联键：IN 恒 false，NOT IN 恒 true
            return new SubqueryFold.ConstNode(not);
        }
        return new InSemiJoinNode(inner.outerKeyCol, col, not, sets, inner.outerKeyType,
                isIntFamily(outerValType.getValueType()));
    }

    private static boolean isIntFamily(int valueType) {
        return valueType == Value.TINYINT || valueType == Value.SMALLINT
                || valueType == Value.INTEGER || valueType == Value.BIGINT;
    }

    /** 预计算「归一化关联键 → 内层值集合」；值转换到外层值列类型，失败返回 {@code null}。 */
    private static Map<Object, KeyMembership> precomputeKeyedSets(SubqueryInner.InnerQuery inner,
            int innerValCol, TypeInfo outerValType, SessionLocal session) {
        OmapTable table = (OmapTable) inner.innerFilter.getTable();
        CompareMode compareMode = table.getCompareMode();
        TypeInfo keyType = inner.outerKeyType;
        boolean intKeys = keyType.getValueType() == Value.INTEGER;
        boolean longKeys = keyType.getValueType() == Value.BIGINT;
        boolean longValues = isIntFamily(outerValType.getValueType());
        Map<Object, KeyMembership> sets = new HashMap<>();
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
                ColumnVector valVec = segment.getColumn(innerValCol);
                if (intKeys && keyVec instanceof IntVector) {
                    int[] raw = ((IntVector) keyVec).rawValues();
                    NullBitmap nulls = keyVec.nulls();
                    for (int i = 0; i < selCount; i++) {
                        int idx = sel == null ? i : sel[i];
                        if (!nulls.isNull(idx)) {
                            membership(sets, raw[idx], longValues)
                                    .add(valVec.get(idx), outerValType, session);
                        }
                    }
                } else if (longKeys && keyVec instanceof LongVector) {
                    long[] raw = ((LongVector) keyVec).rawValues();
                    NullBitmap nulls = keyVec.nulls();
                    for (int i = 0; i < selCount; i++) {
                        int idx = sel == null ? i : sel[i];
                        if (!nulls.isNull(idx)) {
                            membership(sets, raw[idx], longValues)
                                    .add(valVec.get(idx), outerValType, session);
                        }
                    }
                } else {
                    for (int i = 0; i < selCount; i++) {
                        int idx = sel == null ? i : sel[i];
                        Value k = keyVec.get(idx);
                        if (k == ValueNull.INSTANCE) {
                            continue;
                        }
                        membership(sets, SubqueryInner.normalizeKey(k, keyType, session), longValues)
                                .add(valVec.get(idx), outerValType, session);
                    }
                }
            }
            for (Value[] row : snapshot.extraRows) {
                if (inner.constFilter != null
                        && !inner.constFilter.evalRow(row, session, compareMode)) {
                    continue;
                }
                Value k = row[inner.innerKeyCol];
                if (k == ValueNull.INSTANCE) {
                    continue;
                }
                membership(sets, SubqueryInner.normalizeKey(k, keyType, session), longValues)
                        .add(row[innerValCol], outerValType, session);
            }
        } catch (RuntimeException e) {
            return null;
        }
        return sets;
    }

    private static KeyMembership membership(Map<Object, KeyMembership> sets, Object key,
            boolean longValues) {
        KeyMembership m = sets.get(key);
        if (m == null) {
            m = new KeyMembership(longValues);
            sets.put(key, m);
        }
        return m;
    }

    /** 单个关联键的内层值集合；{@code hasNull} 记录内层投影里的 NULL（NOT IN 的三值语义需要它）。 */
    static final class KeyMembership {
        /** 外层值列为整数族时使用，其余为 {@code null}。 */
        final LongSet longs;
        final Set<Value> values;
        boolean hasNull;

        KeyMembership(boolean longValues) {
            this.longs = longValues ? new LongSet(4) : null;
            this.values = longValues ? null : new HashSet<>();
        }

        void add(Value v, TypeInfo outerValType, SessionLocal session) {
            if (v == ValueNull.INSTANCE) {
                hasNull = true;
                return;
            }
            if (v.getValueType() != outerValType.getValueType()) {
                v = v.convertTo(outerValType, session);
            }
            if (longs != null) {
                longs.add(v.getLong());
            } else {
                values.add(v);
            }
        }

        boolean contains(Value v) {
            return longs != null ? longs.contains(v.getLong()) : values.contains(v);
        }
    }

    /**
     * 关联 IN / NOT IN：外层按归一化关联键取内层值集合做成员判定。
     * NULL 键与未命中键都按「内层空集」处理（IN 不命中、NOT IN 命中）；
     * 外层值为 NULL 时两者都不命中；集合含 NULL 且外层值不在集合中时 NOT IN 为
     * UNKNOWN，同样不命中。整数族值列走原始数组快路径，键列支持 IntVector、
     * LongVector 与字典向量（按码预解析集合引用）。
     */
    static final class InSemiJoinNode implements VectorizedFilter.Node {
        private final int keyCol;
        private final int valCol;
        private final boolean not;
        private final Map<Object, KeyMembership> sets;
        private final TypeInfo outerKeyType;
        private final boolean longValues;

        InSemiJoinNode(int keyCol, int valCol, boolean not, Map<Object, KeyMembership> sets,
                TypeInfo outerKeyType, boolean longValues) {
            this.keyCol = keyCol;
            this.valCol = valCol;
            this.not = not;
            this.sets = sets;
            this.outerKeyType = outerKeyType;
            this.longValues = longValues;
        }

        @Override
        public void collectColumns(java.util.function.IntConsumer sink) {
            sink.accept(keyCol);
            sink.accept(valCol);
        }

        @Override
        public int filterSegment(ColumnSegment segment, int[] sel, int selCount, boolean fullRange,
                BitSet masked, SessionLocal session, CompareMode compareMode) {
            ColumnVector keyVec = segment.getColumn(keyCol);
            ColumnVector valVec = segment.getColumn(valCol);
            boolean fastKey = keyVec instanceof IntVector || keyVec instanceof LongVector
                    || SubqueryInner.isDict(keyVec);
            if (longValues && fastKey && valVec instanceof IntVector) {
                return filterInt(keyVec, (IntVector) valVec, sel, selCount, fullRange, session);
            }
            if (longValues && fastKey && valVec instanceof LongVector) {
                return filterLong(keyVec, (LongVector) valVec, sel, selCount, fullRange, session);
            }
            int[] keyCodes = SubqueryInner.dictCodes(keyVec);
            KeyMembership[] dictKm = keyCodes != null ? dictSets(keyVec, session) : null;
            int out = 0;
            if (fullRange) {
                for (int i = 0; i < selCount; i++) {
                    if (matchValue(keyVec, valVec, i, keyCodes, dictKm, session)) {
                        sel[out++] = i;
                    }
                }
            } else {
                for (int i = 0; i < selCount; i++) {
                    int idx = sel[i];
                    if (matchValue(keyVec, valVec, idx, keyCodes, dictKm, session)) {
                        sel[out++] = idx;
                    }
                }
            }
            return out;
        }

        /** 整数值列的快路径；键列是 IntVector、LongVector 或字典向量。 */
        private int filterInt(ColumnVector keyVec, IntVector valVec, int[] sel, int selCount,
                boolean fullRange, SessionLocal session) {
            int[] vals = valVec.rawValues();
            long[] valBits = SubqueryInner.nullableBits(valVec);
            int[] keyVals = keyVec instanceof IntVector ? ((IntVector) keyVec).rawValues() : null;
            long[] keyValsL = keyVec instanceof LongVector ? ((LongVector) keyVec).rawValues() : null;
            int[] keyCodes = keyVals == null && keyValsL == null ? SubqueryInner.dictCodes(keyVec) : null;
            KeyMembership[] dictKm = keyCodes != null ? dictSets(keyVec, session) : null;
            long[] keyBits = SubqueryInner.nullableBits(keyVec);
            int out = 0;
            if (fullRange) {
                for (int i = 0; i < selCount; i++) {
                    if (matchInt(i, vals, valBits, keyVals, keyValsL, keyCodes, dictKm, keyBits)) {
                        sel[out++] = i;
                    }
                }
            } else {
                for (int i = 0; i < selCount; i++) {
                    int idx = sel[i];
                    if (matchInt(idx, vals, valBits, keyVals, keyValsL, keyCodes, dictKm, keyBits)) {
                        sel[out++] = idx;
                    }
                }
            }
            return out;
        }

        private boolean matchInt(int idx, int[] vals, long[] valBits, int[] keyVals, long[] keyValsL,
                int[] keyCodes, KeyMembership[] dictKm, long[] keyBits) {
            if (valBits != null && (valBits[idx >>> 6] & (1L << (idx & 63))) != 0) {
                return false;
            }
            KeyMembership km = membershipAt(idx, keyVals, keyValsL, keyCodes, dictKm, keyBits);
            return matchLong(km, vals[idx]);
        }

        private int filterLong(ColumnVector keyVec, LongVector valVec, int[] sel, int selCount,
                boolean fullRange, SessionLocal session) {
            long[] vals = valVec.rawValues();
            long[] valBits = SubqueryInner.nullableBits(valVec);
            int[] keyVals = keyVec instanceof IntVector ? ((IntVector) keyVec).rawValues() : null;
            long[] keyValsL = keyVec instanceof LongVector ? ((LongVector) keyVec).rawValues() : null;
            int[] keyCodes = keyVals == null && keyValsL == null ? SubqueryInner.dictCodes(keyVec) : null;
            KeyMembership[] dictKm = keyCodes != null ? dictSets(keyVec, session) : null;
            long[] keyBits = SubqueryInner.nullableBits(keyVec);
            int out = 0;
            if (fullRange) {
                for (int i = 0; i < selCount; i++) {
                    if (matchLongRow(i, vals, valBits, keyVals, keyValsL, keyCodes, dictKm, keyBits)) {
                        sel[out++] = i;
                    }
                }
            } else {
                for (int i = 0; i < selCount; i++) {
                    int idx = sel[i];
                    if (matchLongRow(idx, vals, valBits, keyVals, keyValsL, keyCodes, dictKm, keyBits)) {
                        sel[out++] = idx;
                    }
                }
            }
            return out;
        }

        private boolean matchLongRow(int idx, long[] vals, long[] valBits, int[] keyVals,
                long[] keyValsL, int[] keyCodes, KeyMembership[] dictKm, long[] keyBits) {
            if (valBits != null && (valBits[idx >>> 6] & (1L << (idx & 63))) != 0) {
                return false;
            }
            KeyMembership km = membershipAt(idx, keyVals, keyValsL, keyCodes, dictKm, keyBits);
            return matchLong(km, vals[idx]);
        }

        /** NOT IN 的成员判定：见类注释的三值语义。 */
        private boolean matchLong(KeyMembership km, long val) {
            if (not) {
                if (km == null) {
                    return true;
                }
                return !km.longs.contains(val) && !km.hasNull;
            }
            if (km == null) {
                return false;
            }
            return km.longs.contains(val);
        }

        /** 键列是原始数组或字典时的键查找；键行为 NULL 时返回 {@code null}。 */
        private KeyMembership membershipAt(int idx, int[] keyVals, long[] keyValsL,
                int[] keyCodes, KeyMembership[] dictKm, long[] keyBits) {
            if (keyVals != null) {
                if (keyBits != null && (keyBits[idx >>> 6] & (1L << (idx & 63))) != 0) {
                    return null;
                }
                return sets.get(keyVals[idx]);
            }
            if (keyValsL != null) {
                if (keyBits != null && (keyBits[idx >>> 6] & (1L << (idx & 63))) != 0) {
                    return null;
                }
                return sets.get(keyValsL[idx]);
            }
            int code = keyCodes[idx];
            return code < 0 ? null : dictKm[code];
        }

        /** 按字典下标预解析每个码对应的值集合；缺失的码留 {@code null}，按未命中处理。 */
        private KeyMembership[] dictSets(ColumnVector vec, SessionLocal session) {
            Value[] dict = vec instanceof DictValueVector
                    ? ((DictValueVector) vec).rawDict()
                    : ((DictVarcharVector) vec).rawDict();
            KeyMembership[] result = new KeyMembership[dict.length];
            for (int i = 0; i < dict.length; i++) {
                Value v = dict[i];
                result[i] = v == ValueNull.INSTANCE
                        ? null
                        : sets.get(SubqueryInner.normalizeKey(v, outerKeyType, session));
            }
            return result;
        }

        private boolean matchValue(ColumnVector keyVec, ColumnVector valVec, int idx,
                int[] keyCodes, KeyMembership[] dictKm, SessionLocal session) {
            KeyMembership km;
            if (keyCodes != null) {
                int code = keyCodes[idx];
                km = code < 0 ? null : dictKm[code];
            } else {
                Value k = keyVec.get(idx);
                km = k == ValueNull.INSTANCE ? null
                        : sets.get(SubqueryInner.normalizeKey(k, outerKeyType, session));
            }
            return matchMembership(km, valVec.get(idx));
        }

        /** IN / NOT IN 的成员判定；三值语义的 UNKNOWN 都按不命中处理。 */
        private boolean matchMembership(KeyMembership km, Value v) {
            if (not) {
                if (km == null) {
                    return true;
                }
                if (v == ValueNull.INSTANCE || km.contains(v)) {
                    return false;
                }
                return !km.hasNull;
            }
            if (km == null || v == ValueNull.INSTANCE) {
                return false;
            }
            return km.contains(v);
        }

        @Override
        public boolean evalRow(Value[] row, SessionLocal session, CompareMode compareMode) {
            Value k = row[keyCol];
            KeyMembership km = k == ValueNull.INSTANCE ? null
                    : sets.get(SubqueryInner.normalizeKey(k, outerKeyType, session));
            return matchMembership(km, row[valCol]);
        }
    }
}

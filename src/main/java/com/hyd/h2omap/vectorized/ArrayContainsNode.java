package com.hyd.h2omap.vectorized;

import java.util.BitSet;
import java.util.function.IntConsumer;

import org.h2.engine.SessionLocal;
import org.h2.value.CompareMode;
import org.h2.value.Value;
import org.h2.value.ValueCollectionBase;
import org.h2.value.ValueNull;

import com.hyd.h2omap.storage.ColumnSegment;
import com.hyd.h2omap.vector.ArrayVector;
import com.hyd.h2omap.vector.ColumnVector;
import com.hyd.h2omap.vector.DictVarcharVector;
import com.hyd.h2omap.vector.DoubleVector;
import com.hyd.h2omap.vector.IntVector;
import com.hyd.h2omap.vector.LongVector;
import com.hyd.h2omap.vector.NullBitmap;
import com.hyd.h2omap.vector.VarcharVector;

/**
 * {@code ARRAY_CONTAINS(数组列, 常量)} 与 {@code NOT ARRAY_CONTAINS(...)} 的向量化节点。
 *
 * <p>在元素列上判等：{@code starts} 给出每行元素的区间，按行的区间求「是否有元素相等」，
 * 与 {@link ArrayVector#get(int)} 逐行重建 {@code ValueArray} 相比不产生任何对象。
 * 元素列是 {@code INT}/{@code BIGINT}/{@code DOUBLE}/{@code VARCHAR} 时走原始数组，
 * 字典元素列先在字典上算出匹配位图。元素列由 {@code ColumnVector.create} 建出时是普通向量，
 * 从段快照读回可能是字典向量，两条分支都保留。
 */
final class ArrayContainsNode implements VectorizedFilter.Node {
    private final int col;
    private final Value target;
    private final boolean negate;

    ArrayContainsNode(int col, Value target, boolean negate) {
        this.col = col;
        this.target = target;
        this.negate = negate;
    }

    @Override
    public void collectColumns(IntConsumer sink) {
        sink.accept(col);
    }

    @Override
    public String sig() {
        return "T" + col + ',' + VectorizedFilter.valueSig(target) + ',' + negate;
    }

    @Override
    public int filterSegment(ColumnSegment segment, int[] sel, int selCount, boolean fullRange,
            BitSet masked, SessionLocal session, CompareMode compareMode) {
        ColumnVector vec = segment.getColumn(col);
        if (vec instanceof ArrayVector) {
            return filterArray((ArrayVector) vec, sel, selCount, fullRange, session, compareMode);
        }
        int out = 0;
        for (int i = 0; i < selCount; i++) {
            int idx = fullRange ? i : sel[i];
            if (matchValue(vec.get(idx), session, compareMode) == (negate ? 0 : 1)) {
                sel[out++] = idx;
            }
        }
        return out;
    }

    /**
     * 在元素列上判等：{@code starts} 给出每行元素的区间，按行的区间求“是否有元素相等”。
     *
     * <p>与 {@link ArrayVector#get(int)} 逐行重建 {@code ValueArray} 的区别是这里不产生任何对象。
     */
    private int filterArray(ArrayVector av, int[] sel, int selCount, boolean fullRange,
            SessionLocal session, CompareMode compareMode) {
        ColumnVector elements = av.elements();
        NullBitmap arrayNulls = av.nulls();
        int[] starts = av.rawStarts();
        int targetType = target.getValueType();
        if (target == ValueNull.INSTANCE) {
            NullBitmap elementNulls = elements.nulls();
            int out = 0;
            for (int i = 0; i < selCount; i++) {
                int idx = fullRange ? i : sel[i];
                if (arrayNulls.isNull(idx)) {
                    continue;
                }
                boolean found = false;
                for (int j = starts[idx], end = starts[idx + 1]; j < end; j++) {
                    if (elementNulls.isNull(j)) {
                        found = true;
                        break;
                    }
                }
                if (found != negate) {
                    sel[out++] = idx;
                }
            }
            return out;
        }
        if (elements instanceof IntVector && targetType == Value.INTEGER) {
            IntVector iv = (IntVector) elements;
            int[] vals = iv.rawValues();
            NullBitmap elementNulls = iv.nulls();
            boolean hasNull = elementNulls.hasNull();
            int t = target.getInt();
            int out = 0;
            for (int i = 0; i < selCount; i++) {
                int idx = fullRange ? i : sel[i];
                if (arrayNulls.isNull(idx)) {
                    continue;
                }
                boolean found = false;
                for (int j = starts[idx], end = starts[idx + 1]; j < end; j++) {
                    if ((!hasNull || !elementNulls.isNull(j)) && vals[j] == t) {
                        found = true;
                        break;
                    }
                }
                if (found != negate) {
                    sel[out++] = idx;
                }
            }
            return out;
        }
        if (elements instanceof LongVector && targetType == Value.BIGINT) {
            LongVector lv = (LongVector) elements;
            long[] vals = lv.rawValues();
            NullBitmap elementNulls = lv.nulls();
            boolean hasNull = elementNulls.hasNull();
            long t = target.getLong();
            int out = 0;
            for (int i = 0; i < selCount; i++) {
                int idx = fullRange ? i : sel[i];
                if (arrayNulls.isNull(idx)) {
                    continue;
                }
                boolean found = false;
                for (int j = starts[idx], end = starts[idx + 1]; j < end; j++) {
                    if ((!hasNull || !elementNulls.isNull(j)) && vals[j] == t) {
                        found = true;
                        break;
                    }
                }
                if (found != negate) {
                    sel[out++] = idx;
                }
            }
            return out;
        }
        if (elements instanceof DoubleVector && targetType == Value.DOUBLE) {
            DoubleVector dv = (DoubleVector) elements;
            double[] vals = dv.rawValues();
            NullBitmap elementNulls = dv.nulls();
            boolean hasNull = elementNulls.hasNull();
            double t = target.getDouble();
            int out = 0;
            for (int i = 0; i < selCount; i++) {
                int idx = fullRange ? i : sel[i];
                if (arrayNulls.isNull(idx)) {
                    continue;
                }
                boolean found = false;
                for (int j = starts[idx], end = starts[idx + 1]; j < end; j++) {
                    if ((!hasNull || !elementNulls.isNull(j)) && Double.compare(vals[j], t) == 0) {
                        found = true;
                        break;
                    }
                }
                if (found != negate) {
                    sel[out++] = idx;
                }
            }
            return out;
        }
        if (elements instanceof VarcharVector && targetType == Value.VARCHAR) {
            VarcharVector sv = (VarcharVector) elements;
            NullBitmap elementNulls = sv.nulls();
            boolean hasNull = elementNulls.hasNull();
            String t = target.getString();
            int out = 0;
            for (int i = 0; i < selCount; i++) {
                int idx = fullRange ? i : sel[i];
                if (arrayNulls.isNull(idx)) {
                    continue;
                }
                boolean found = false;
                for (int j = starts[idx], end = starts[idx + 1]; j < end; j++) {
                    if ((!hasNull || !elementNulls.isNull(j))
                            && compareMode.compareString(sv.getString(j), t, false) == 0) {
                        found = true;
                        break;
                    }
                }
                if (found != negate) {
                    sel[out++] = idx;
                }
            }
            return out;
        }
        if (elements instanceof DictVarcharVector && VectorizedFilter.isStringType(targetType)) {
            DictVarcharVector dv = (DictVarcharVector) elements;
            int[] codes = dv.rawCodes();
            Value[] dict = dv.rawDict();
            boolean[] matched = new boolean[dv.dictSize()];
            for (int d = 0; d < matched.length; d++) {
                matched[d] = dict[d].compareTo(target, session, compareMode) == 0;
            }
            int out = 0;
            for (int i = 0; i < selCount; i++) {
                int idx = fullRange ? i : sel[i];
                if (arrayNulls.isNull(idx)) {
                    continue;
                }
                boolean found = false;
                for (int j = starts[idx], end = starts[idx + 1]; j < end; j++) {
                    int code = codes[j];
                    if (code >= 0 && matched[code]) {
                        found = true;
                        break;
                    }
                }
                if (found != negate) {
                    sel[out++] = idx;
                }
            }
            return out;
        }
        int out = 0;
        for (int i = 0; i < selCount; i++) {
            int idx = fullRange ? i : sel[i];
            if (arrayNulls.isNull(idx)) {
                continue;
            }
            boolean found = false;
            for (int j = starts[idx], end = starts[idx + 1]; j < end; j++) {
                if (elements.get(j).compareTo(target, session, compareMode) == 0) {
                    found = true;
                    break;
                }
            }
            if (found != negate) {
                sel[out++] = idx;
            }
        }
        return out;
    }

    /**
     * 整值判等，返回 {@code 1} 命中、{@code 0} 不命中、{@code -1} 结果为 NULL。
     *
     * <p>非数组值按 H2 的定义结果也是 NULL（{@code ArrayFunction} 对既非 ARRAY 也非 ROW 的首参返回 NULL）。
     */
    private int matchValue(Value v, SessionLocal session, CompareMode compareMode) {
        if (v == null || v == ValueNull.INSTANCE) {
            return -1;
        }
        int type = v.getValueType();
        if (type != Value.ARRAY && type != Value.ROW) {
            return -1;
        }
        for (Value element : ((ValueCollectionBase) v).getList()) {
            if (element.compareTo(target, session, compareMode) == 0) {
                return 1;
            }
        }
        return 0;
    }

    @Override
    public boolean evalRow(Value[] row, SessionLocal session, CompareMode compareMode) {
        return matchValue(row[col], session, compareMode) == (negate ? 0 : 1);
    }
}

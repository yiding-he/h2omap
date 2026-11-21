package com.hyd.h2omap.vectorized;

import java.util.BitSet;
import java.util.function.IntConsumer;

import org.h2.engine.SessionLocal;
import org.h2.value.CompareMode;
import org.h2.value.Value;
import org.h2.value.ValueNull;

import com.hyd.h2omap.storage.ColumnSegment;
import com.hyd.h2omap.vector.ColumnVector;
import com.hyd.h2omap.vector.NullBitmap;

/**
 * {@code IS NULL} / {@code IS NOT NULL} 节点，直接读 null 位图的原始 long 数组。
 */
final class NullCheckNode implements VectorizedFilter.Node {
    private final int col;
    private final boolean not;

    NullCheckNode(int col, boolean not) {
        this.col = col;
        this.not = not;
    }

    @Override
    public void collectColumns(IntConsumer sink) {
        sink.accept(col);
    }

    @Override
    public String sig() {
        return "N" + col + ',' + not;
    }

    @Override
    public int filterSegment(ColumnSegment segment, int[] sel, int selCount, boolean fullRange,
            BitSet masked, SessionLocal session, CompareMode compareMode) {
        NullBitmap nulls = segment.getColumn(col).nulls();
        if (!nulls.hasNull()) {
            if (not) {
                if (fullRange) {
                    for (int i = 0; i < selCount; i++) {
                        sel[i] = i;
                    }
                }
                return selCount;
            }
            return 0;
        }
        long[] bits = nulls.rawBits();
        int out = 0;
        if (fullRange) {
            for (int i = 0; i < selCount; i++) {
                boolean isNull = (bits[i >>> 6] & (1L << i)) != 0;
                if (isNull ^ not) {
                    sel[out++] = i;
                }
            }
        } else {
            for (int i = 0; i < selCount; i++) {
                int idx = sel[i];
                boolean isNull = (bits[idx >>> 6] & (1L << idx)) != 0;
                if (isNull ^ not) {
                    sel[out++] = idx;
                }
            }
        }
        return out;
    }

    @Override
    public boolean evalRow(Value[] row, SessionLocal session, CompareMode compareMode) {
        boolean isNull = row[col] == ValueNull.INSTANCE;
        return isNull ^ not;
    }
}

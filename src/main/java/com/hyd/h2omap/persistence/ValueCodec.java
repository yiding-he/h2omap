/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.persistence;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.h2.engine.Database;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.db.ValueDataType;
import org.h2.value.Value;

import com.hyd.h2omap.vector.ColumnVector;
import com.hyd.h2omap.vector.DictValueVector;
import com.hyd.h2omap.vector.DictVarcharVector;
import com.hyd.h2omap.vector.DoubleVector;
import com.hyd.h2omap.vector.IntVector;
import com.hyd.h2omap.vector.LongVector;
import com.hyd.h2omap.vector.NullBitmap;
import com.hyd.h2omap.vector.TimestampVector;
import com.hyd.h2omap.vector.ValueVector;
import com.hyd.h2omap.vector.VarcharVector;

/**
 * H2 {@link Value} 与列向量的二进制编解码器。
 * 全类型 {@link Value} 直接委托给 H2 原生 {@link ValueDataType}，
 * 专用列向量直接按原始数组与位图读写。
 */
final class ValueCodec {

    private static final byte VEC_INT = 1;
    private static final byte VEC_LONG = 2;
    private static final byte VEC_DOUBLE = 3;
    private static final byte VEC_VARCHAR = 4;
    private static final byte VEC_DICT_VARCHAR = 5;
    private static final byte VEC_DICT_VALUE = 6;
    private static final byte VEC_VALUE = 7;
    private static final byte VEC_TIMESTAMP = 8;

    private final ValueDataType valueType;
    private final WriteBuffer writeBuffer = new WriteBuffer(4096);

    ValueCodec(Database database) {
        this.valueType = new ValueDataType(database, null);
    }

    void writeValue(DataOutput out, Value v) throws IOException {
        writeBuffer.clear();
        valueType.write(writeBuffer, v);
        ByteBuffer bb = writeBuffer.getBuffer();
        int len = bb.position();
        out.writeInt(len);
        out.write(bb.array(), 0, len);
    }

    Value readValue(DataInput in) throws IOException {
        int len = in.readInt();
        byte[] bytes = new byte[len];
        in.readFully(bytes);
        return valueType.read(ByteBuffer.wrap(bytes));
    }

    void writeNullBitmap(DataOutput out, NullBitmap bitmap) throws IOException {
        out.writeBoolean(bitmap.hasNull());
        long[] bits = bitmap.rawBits();
        out.writeInt(bits.length);
        for (long w : bits) {
            out.writeLong(w);
        }
    }

    NullBitmap readNullBitmap(DataInput in) throws IOException {
        boolean hasNull = in.readBoolean();
        int len = in.readInt();
        long[] bits = new long[len];
        for (int i = 0; i < len; i++) {
            bits[i] = in.readLong();
        }
        return NullBitmap.of(bits, hasNull);
    }

    void writeVector(DataOutput out, ColumnVector vec) throws IOException {
        if (vec instanceof IntVector iv) {
            out.writeByte(VEC_INT);
            writeNullBitmap(out, iv.nulls());
            int[] arr = iv.rawValues();
            out.writeInt(arr.length);
            for (int v : arr) {
                out.writeInt(v);
            }
        } else if (vec instanceof LongVector lv) {
            out.writeByte(VEC_LONG);
            writeNullBitmap(out, lv.nulls());
            long[] arr = lv.rawValues();
            out.writeInt(arr.length);
            for (long v : arr) {
                out.writeLong(v);
            }
        } else if (vec instanceof DoubleVector dv) {
            out.writeByte(VEC_DOUBLE);
            writeNullBitmap(out, dv.nulls());
            double[] arr = dv.rawValues();
            out.writeInt(arr.length);
            for (double v : arr) {
                out.writeDouble(v);
            }
        } else if (vec instanceof VarcharVector sv) {
            out.writeByte(VEC_VARCHAR);
            NullBitmap nulls = sv.nulls();
            writeNullBitmap(out, nulls);
            String[] arr = sv.rawValues();
            out.writeInt(arr.length);
            for (int i = 0; i < arr.length; i++) {
                if (!nulls.isNull(i) && arr[i] != null) {
                    byte[] utf8 = arr[i].getBytes(StandardCharsets.UTF_8);
                    out.writeInt(utf8.length);
                    out.write(utf8);
                } else {
                    out.writeInt(-1);
                }
            }
        } else if (vec instanceof DictVarcharVector dv) {
            out.writeByte(VEC_DICT_VARCHAR);
            writeNullBitmap(out, dv.nulls());
            int dictSize = dv.dictSize();
            Value[] dict = dv.rawDict();
            out.writeInt(dictSize);
            for (int i = 0; i < dictSize; i++) {
                writeValue(out, dict[i]);
            }
            int[] codes = dv.rawCodes();
            out.writeInt(codes.length);
            for (int code : codes) {
                out.writeInt(code);
            }
        } else if (vec instanceof DictValueVector dv) {
            out.writeByte(VEC_DICT_VALUE);
            writeNullBitmap(out, dv.nulls());
            int dictSize = dv.dictSize();
            Value[] dict = dv.rawDict();
            out.writeInt(dictSize);
            for (int i = 0; i < dictSize; i++) {
                writeValue(out, dict[i]);
            }
            int[] codes = dv.rawCodes();
            out.writeInt(codes.length);
            for (int code : codes) {
                out.writeInt(code);
            }
        } else if (vec instanceof TimestampVector tv) {
            out.writeByte(VEC_TIMESTAMP);
            writeNullBitmap(out, tv.nulls());
            long[] dates = tv.rawDateValues();
            long[] nanos = tv.rawTimeNanos();
            out.writeInt(dates.length);
            for (int i = 0; i < dates.length; i++) {
                out.writeLong(dates[i]);
                out.writeLong(nanos[i]);
            }
        } else if (vec instanceof ValueVector vv) {
            out.writeByte(VEC_VALUE);
            writeNullBitmap(out, vv.nulls());
            List<Value> vals = vv.rawValues();
            out.writeInt(vals.size());
            for (Value v : vals) {
                writeValue(out, v);
            }
        } else {
            throw new IOException("Unsupported ColumnVector type: " + vec.getClass().getName());
        }
    }

    ColumnVector readVector(DataInput in) throws IOException {
        byte kind = in.readByte();
        NullBitmap nulls = readNullBitmap(in);
        switch (kind) {
        case VEC_INT: {
            int len = in.readInt();
            int[] arr = new int[len];
            for (int i = 0; i < len; i++) {
                arr[i] = in.readInt();
            }
            return IntVector.of(arr, nulls);
        }
        case VEC_LONG: {
            int len = in.readInt();
            long[] arr = new long[len];
            for (int i = 0; i < len; i++) {
                arr[i] = in.readLong();
            }
            return LongVector.of(arr, nulls);
        }
        case VEC_DOUBLE: {
            int len = in.readInt();
            double[] arr = new double[len];
            for (int i = 0; i < len; i++) {
                arr[i] = in.readDouble();
            }
            return DoubleVector.of(arr, nulls);
        }
        case VEC_VARCHAR: {
            int len = in.readInt();
            String[] arr = new String[len];
            for (int i = 0; i < len; i++) {
                int slen = in.readInt();
                if (slen >= 0) {
                    byte[] utf8 = new byte[slen];
                    in.readFully(utf8);
                    arr[i] = new String(utf8, StandardCharsets.UTF_8);
                }
            }
            return VarcharVector.of(arr, nulls);
        }
        case VEC_DICT_VARCHAR: {
            int dictSize = in.readInt();
            Value[] dict = new Value[dictSize];
            for (int i = 0; i < dictSize; i++) {
                dict[i] = readValue(in);
            }
            int len = in.readInt();
            int[] codes = new int[len];
            for (int i = 0; i < len; i++) {
                codes[i] = in.readInt();
            }
            return DictVarcharVector.of(codes, dict, nulls);
        }
        case VEC_DICT_VALUE: {
            int dictSize = in.readInt();
            Value[] dict = new Value[dictSize];
            for (int i = 0; i < dictSize; i++) {
                dict[i] = readValue(in);
            }
            int len = in.readInt();
            int[] codes = new int[len];
            for (int i = 0; i < len; i++) {
                codes[i] = in.readInt();
            }
            return DictValueVector.of(codes, dict, nulls);
        }
        case VEC_TIMESTAMP: {
            int len = in.readInt();
            long[] dates = new long[len];
            long[] nanos = new long[len];
            for (int i = 0; i < len; i++) {
                dates[i] = in.readLong();
                nanos[i] = in.readLong();
            }
            return TimestampVector.of(dates, nanos, nulls);
        }
        case VEC_VALUE: {
            int len = in.readInt();
            List<Value> vals = new ArrayList<>(len);
            for (int i = 0; i < len; i++) {
                vals.add(readValue(in));
            }
            return ValueVector.of(vals, nulls);
        }
        default:
            throw new IOException("Unknown vector kind: " + kind);
        }
    }
}

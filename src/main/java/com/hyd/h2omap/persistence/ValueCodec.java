/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.persistence;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.h2.engine.Database;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.db.ValueDataType;
import org.h2.value.TypeInfo;
import org.h2.value.Value;

import com.hyd.h2omap.vector.ArrayVector;
import com.hyd.h2omap.vector.ClobVector;
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

    private static final byte VEC_ARRAY = 9;
    private static final byte VEC_CLOB = 10;

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
        } else if (vec instanceof ClobVector cv) {
            out.writeByte(VEC_CLOB);
            writePackedText(out, cv);
        } else if (vec instanceof VarcharVector sv) {
            out.writeByte(VEC_VARCHAR);
            writePackedText(out, sv);
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
        } else if (vec instanceof ArrayVector av) {
            out.writeByte(VEC_ARRAY);
            writeNullBitmap(out, av.nulls());
            TypeInfo componentType = av.getComponentType();
            out.writeInt(componentType.getValueType());
            out.writeLong(componentType.getPrecision());
            out.writeInt(componentType.getScale());
            int[] starts = av.rawStarts();
            out.writeInt(starts.length - 1);
            for (int s : starts) {
                out.writeInt(s);
            }
            writeVector(out, av.elements());
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

    /** VARCHAR 与 CLOB 共用的打包写法：null 位图 + 行数 + 每行长度加字节。 */
    private void writePackedText(DataOutput out, VarcharVector sv) throws IOException {
        NullBitmap nulls = sv.nulls();
        writeNullBitmap(out, nulls);
        int rows = sv.filledRows();
        int[] starts = sv.rawStarts();
        byte[] data = sv.rawData();
        out.writeInt(rows);
        for (int i = 0; i < rows; i++) {
            if (nulls.isNull(i)) {
                out.writeInt(-1);
            } else {
                int len = starts[i + 1] - starts[i];
                out.writeInt(len);
                out.write(data, starts[i], len);
            }
        }
    }

    /** VARCHAR 与 CLOB 共用的打包读法，返回前裁掉读缓冲尾部。 */
    private ColumnVector readPackedText(DataInput in, NullBitmap nulls, boolean clob) throws IOException {
        int rows = in.readInt();
        int[] starts = new int[rows + 1];
        byte[] data = new byte[Math.max(16, rows * 8)];
        int pos = 0;
        for (int i = 0; i < rows; i++) {
            int len = in.readInt();
            if (len >= 0) {
                if (pos + len > data.length) {
                    data = Arrays.copyOf(data, Math.max(pos + len, data.length * 2));
                }
                in.readFully(data, pos, len);
                pos += len;
            }
            starts[i + 1] = pos;
        }
        return clob ? ClobVector.of(starts, data, nulls) : VarcharVector.of(starts, data, nulls);
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
        case VEC_VARCHAR:
            return readPackedText(in, nulls, false);
        case VEC_CLOB:
            return readPackedText(in, nulls, true);
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
        case VEC_ARRAY: {
            int componentValueType = in.readInt();
            long precision = in.readLong();
            int scale = in.readInt();
            int rowCount = in.readInt();
            int[] starts = new int[rowCount + 1];
            for (int i = 0; i <= rowCount; i++) {
                starts[i] = in.readInt();
            }
            ColumnVector elements = readVector(in);
            return ArrayVector.of(
                    TypeInfo.getTypeInfo(componentValueType, precision, scale, null),
                    starts, elements, nulls);
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

    /**
     * 从内存缓冲解码一个列向量：{@code blockLen} 是该列块的总字节数。
     * 原始数组用 {@link ByteBuffer} 视图一次批量拷贝，替代逐元素的同步单字节拼接；
     * 打包文本按「剩余字节数 - rows*4」一次精确分配，零扩容。
     */
    ColumnVector readVector(ByteBuffer buf, int blockLen) throws IOException {
        final int blockStart = buf.position();
        byte kind = buf.get();
        NullBitmap nulls = readNullBitmap(buf);
        switch (kind) {
        case VEC_INT: {
            int len = buf.getInt();
            return IntVector.of(readInts(buf, len), nulls);
        }
        case VEC_LONG: {
            int len = buf.getInt();
            return LongVector.of(readLongs(buf, len), nulls);
        }
        case VEC_DOUBLE: {
            int len = buf.getInt();
            double[] arr = new double[len];
            if (len > 0) {
                buf.asDoubleBuffer().get(arr);
                buf.position(buf.position() + len * 8);
            }
            return DoubleVector.of(arr, nulls);
        }
        case VEC_VARCHAR:
        case VEC_CLOB:
            return readPackedText(buf, blockLen - (buf.position() - blockStart),
                    kind == VEC_CLOB, nulls);
        case VEC_DICT_VARCHAR:
        case VEC_DICT_VALUE: {
            DataInput vin = new BufferInput(buf);
            int dictSize = vin.readInt();
            Value[] dict = new Value[dictSize];
            for (int i = 0; i < dictSize; i++) {
                dict[i] = readValue(vin);
            }
            int len = buf.getInt();
            int[] codes = readInts(buf, len);
            return kind == VEC_DICT_VARCHAR
                    ? DictVarcharVector.of(codes, dict, nulls)
                    : DictValueVector.of(codes, dict, nulls);
        }
        case VEC_TIMESTAMP: {
            // 落盘布局按行交错（date、nano、date、nano…），批量读也按交错次序取。
            int len = buf.getInt();
            long[] dates = new long[len];
            long[] nanos = new long[len];
            if (len > 0) {
                java.nio.LongBuffer lb = buf.asLongBuffer();
                for (int i = 0; i < len; i++) {
                    dates[i] = lb.get();
                    nanos[i] = lb.get();
                }
                buf.position(buf.position() + len * 16);
            }
            return TimestampVector.of(dates, nanos, nulls);
        }
        case VEC_ARRAY: {
            DataInput vin = new BufferInput(buf);
            int componentValueType = vin.readInt();
            long precision = vin.readLong();
            int scale = vin.readInt();
            int rowCount = vin.readInt();
            int[] starts = readInts(buf, rowCount + 1);
            ColumnVector elements = readVector(buf, blockLen - (buf.position() - blockStart));
            return ArrayVector.of(
                    TypeInfo.getTypeInfo(componentValueType, precision, scale, null),
                    starts, elements, nulls);
        }
        case VEC_VALUE: {
            DataInput vin = new BufferInput(buf);
            int len = vin.readInt();
            List<Value> vals = new ArrayList<>(len);
            for (int i = 0; i < len; i++) {
                vals.add(readValue(vin));
            }
            return ValueVector.of(vals, nulls);
        }
        default:
            throw new IOException("Unknown vector kind: " + kind);
        }
    }

    /** 内存缓冲上的位图读法：hasNull + 字长 + long 批量。 */
    NullBitmap readNullBitmap(ByteBuffer buf) {
        boolean hasNull = buf.get() != 0;
        int len = buf.getInt();
        return NullBitmap.of(readLongs(buf, len), hasNull);
    }

    /** 内存缓冲上的打包文本读法：剩余字节数已知，按「剩余 - 4 - rows*4」一次精确分配。 */
    private ColumnVector readPackedText(ByteBuffer buf, int remaining, boolean clob,
            NullBitmap nulls) throws IOException {
        int rows = buf.getInt();
        int dataBytes = remaining - 4 - rows * 4;
        if (dataBytes < 0) {
            throw new IOException("Packed text block truncated");
        }
        int[] starts = new int[rows + 1];
        byte[] data = new byte[dataBytes];
        int pos = 0;
        for (int i = 0; i < rows; i++) {
            int len = buf.getInt();
            if (len >= 0) {
                buf.get(data, pos, len);
                pos += len;
            }
            starts[i + 1] = pos;
        }
        return clob ? ClobVector.of(starts, data, nulls) : VarcharVector.of(starts, data, nulls);
    }

    private static int[] readInts(ByteBuffer buf, int len) {
        int[] arr = new int[len];
        if (len > 0) {
            buf.asIntBuffer().get(arr);
            buf.position(buf.position() + len * 4);
        }
        return arr;
    }

    private static long[] readLongs(ByteBuffer buf, int len) {
        long[] arr = new long[len];
        if (len > 0) {
            buf.asLongBuffer().get(arr);
            buf.position(buf.position() + len * 8);
        }
        return arr;
    }

    /** 把 {@link ByteBuffer} 适配成 {@link DataInput}，供逐值解码复用 {@link #readValue}。 */
    static final class BufferInput implements DataInput {
        private final ByteBuffer buf;

        BufferInput(ByteBuffer buf) {
            this.buf = buf;
        }

        @Override
        public void readFully(byte[] b) {
            buf.get(b);
        }

        @Override
        public void readFully(byte[] b, int off, int len) {
            buf.get(b, off, len);
        }

        @Override
        public int skipBytes(int n) {
            int skipped = Math.min(n, buf.remaining());
            buf.position(buf.position() + skipped);
            return skipped;
        }

        @Override
        public boolean readBoolean() {
            return buf.get() != 0;
        }

        @Override
        public byte readByte() {
            return buf.get();
        }

        @Override
        public int readUnsignedByte() {
            return buf.get() & 0xff;
        }

        @Override
        public short readShort() {
            return buf.getShort();
        }

        @Override
        public int readUnsignedShort() {
            return buf.getShort() & 0xffff;
        }

        @Override
        public char readChar() {
            return buf.getChar();
        }

        @Override
        public int readInt() {
            return buf.getInt();
        }

        @Override
        public long readLong() {
            return buf.getLong();
        }

        @Override
        public float readFloat() {
            return buf.getFloat();
        }

        @Override
        public double readDouble() {
            return buf.getDouble();
        }

        @Override
        public String readUTF() throws IOException {
            return DataInputStream.readUTF(this);
        }

        @Override
        public String readLine() {
            throw new UnsupportedOperationException("readLine");
        }
    }
}

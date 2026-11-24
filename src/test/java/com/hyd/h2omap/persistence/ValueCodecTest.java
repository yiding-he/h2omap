/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;

import org.h2.engine.Database;
import org.h2.jdbc.JdbcConnection;
import org.h2.value.Value;
import org.h2.value.ValueClob;
import org.h2.value.ValueNull;
import org.h2.value.ValueVarchar;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.hyd.h2omap.vector.ClobVector;
import com.hyd.h2omap.vector.DictVarcharVector;
import com.hyd.h2omap.vector.LongVector;
import com.hyd.h2omap.vector.NullBitmap;
import com.hyd.h2omap.storage.ColumnSegment;
import org.h2.value.CompareMode;
import org.h2.value.TypeInfo;
import org.h2.value.ValueBigint;
import com.hyd.h2omap.vector.ColumnVector;
import com.hyd.h2omap.vector.ValueVector;
import com.hyd.h2omap.vector.VarcharVector;

/**
 * 段文件里打包文本列的编码往返：VEC_CLOB（新）与 VEC_VALUE（旧 CLOB 段）的读兼容。
 */
class ValueCodecTest {

    private static Database database;

    @BeforeAll
    static void setUp() throws Exception {
        Connection c = DriverManager.getConnection("jdbc:h2:mem:codecTest;DB_CLOSE_DELAY=-1", "sa", "");
        database = ((org.h2.engine.SessionLocal) ((JdbcConnection) c).getSession()).getDatabase();
    }

    @AfterAll
    static void tearDown() throws Exception {
        DriverManager.getConnection("jdbc:h2:mem:codecTest;DB_CLOSE_DELAY=-1", "sa", "")
                .createStatement().execute("SHUTDOWN");
    }

    @Test
    void clobVectorRoundTripsThroughVecClob() throws Exception {
        ClobVector vec = new ClobVector(4);
        vec.set(0, ValueClob.createSmall("第一条内容"));
        vec.set(1, ValueNull.INSTANCE);
        vec.set(2, ValueClob.createSmall("c".repeat(300)));
        vec.set(3, ValueClob.createSmall("x"));

        ColumnVector back = roundTrip(vec);
        assertInstanceOf(ClobVector.class, back);
        assertEquals(Value.CLOB, back.get(0).getValueType());
        assertEquals("第一条内容", back.get(0).getString());
        assertEquals(ValueNull.INSTANCE, back.get(1));
        assertEquals("c".repeat(300), back.get(2).getString());
        assertEquals("x", back.get(3).getString());
    }

    /**
     * 旧段文件里 CLOB 列是 VEC_VALUE（逐值 ValueClob）。读路径不变，必须原样读回，
     * 旧库升级后未合并的段继续以逐值形态驻留。
     */
    @Test
    void oldClobSegmentsReadAsValueVector() throws Exception {
        ValueVector vec = new ValueVector(3);
        vec.set(0, ValueClob.createSmall("旧格式的值"));
        vec.set(1, ValueNull.INSTANCE);
        vec.set(2, ValueClob.createSmall("v2"));

        ColumnVector back = roundTrip(vec);
        assertInstanceOf(ValueVector.class, back);
        assertEquals("旧格式的值", back.get(0).getString());
        assertEquals(ValueNull.INSTANCE, back.get(1));
        assertEquals("v2", back.get(2).getString());
    }

    @Test
    void varcharVectorRoundTripsUnchanged() throws Exception {
        VarcharVector vec = new VarcharVector(2);
        vec.set(0, ValueVarchar.get("s0"));
        vec.set(1, ValueVarchar.get("s1"));

        ColumnVector back = roundTrip(vec);
        assertInstanceOf(VarcharVector.class, back);
        assertEquals("s0", back.get(0).getString());
        assertEquals("s1", back.get(1).getString());
    }

    /**
     * 阶段 A：编码态（位打包）向量写盘走 {@code rawValues()}/{@code rawCodes()} 整列解包，
     * 磁盘格式（v3）不变，读回的是可写态且值与编码前完全一致。
     */
    @Test
    void encodedNumericVectorsRoundTripUnchanged() throws Exception {
        long[] longs = new long[100];
        for (int i = 0; i < longs.length; i++) {
            longs[i] = i * 7 % 100;
        }
        LongVector encoded = LongVector.tryEncode(LongVector.of(longs, new NullBitmap(100)));
        assertTrue(encoded.isEncoded());
        ColumnVector back = roundTrip(encoded);
        assertInstanceOf(LongVector.class, back);
        assertFalse(((LongVector) back).isEncoded());
        for (int i = 0; i < longs.length; i++) {
            assertEquals(longs[i], back.get(i).getLong());
        }
    }

    @Test
    void encodedDictCodesRoundTripUnchanged() throws Exception {
        // 值数 100 远小于字典上限，冻结即建成编码态字典向量。
        java.util.List<Value[]> rows = new java.util.ArrayList<>();
        String[] vals = { "a", "b", "c", null, "b", "a" };
        for (int i = 0; i < 6; i++) {
            rows.add(new Value[] { ValueBigint.get(i),
                    vals[i] == null ? ValueNull.INSTANCE : ValueVarchar.get(vals[i]) });
        }
        ColumnSegment seg = ColumnSegment.build(0, rows,
                new TypeInfo[] { TypeInfo.getTypeInfo(Value.VARCHAR) },
                CompareMode.getInstance(null, 0));
        DictVarcharVector encoded = (DictVarcharVector) seg.getColumn(0);
        assertTrue(encoded.isEncoded());
        ColumnVector back = roundTrip(encoded);
        assertInstanceOf(DictVarcharVector.class, back);
        assertEquals("a", back.get(0).getString());
        assertEquals("c", back.get(2).getString());
        assertEquals(ValueNull.INSTANCE, back.get(3));
        assertEquals("a", back.get(5).getString());
    }

    private ColumnVector roundTrip(ColumnVector vec) throws Exception {
        ValueCodec codec = new ValueCodec(database);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            codec.writeVector(out, vec);
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return codec.readVector(in);
        }
    }
}

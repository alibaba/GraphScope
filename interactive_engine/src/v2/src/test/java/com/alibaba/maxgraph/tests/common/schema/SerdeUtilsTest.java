package com.alibaba.maxgraph.tests.common.schema;

import com.alibaba.maxgraph.v2.common.schema.DataType;
import com.alibaba.maxgraph.v2.common.schema.SerdeUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SerdeUtilsTest {

    @Test
    void testSerDe() {
        boolean b = true;
        Assertions.assertEquals(SerdeUtils.bytesToObject(DataType.BOOL, SerdeUtils.objectToBytes(DataType.BOOL, b)), b);
        b = false;
        assertEquals(SerdeUtils.bytesToObject(DataType.BOOL, SerdeUtils.objectToBytes(DataType.BOOL, b)), b);

        char c = 'x';
        assertEquals(SerdeUtils.bytesToObject(DataType.CHAR, SerdeUtils.objectToBytes(DataType.CHAR, c)), c);

        short s = 3;
        assertEquals(SerdeUtils.bytesToObject(DataType.SHORT, SerdeUtils.objectToBytes(DataType.SHORT, s)), s);

        int i = 10;
        assertEquals(SerdeUtils.bytesToObject(DataType.INT, SerdeUtils.objectToBytes(DataType.INT, i)), i);

        long l = 100000000000000L;
        assertEquals(SerdeUtils.bytesToObject(DataType.LONG, SerdeUtils.objectToBytes(DataType.LONG, l)), l);

        float f = 1.234f;
        assertEquals(SerdeUtils.bytesToObject(DataType.FLOAT, SerdeUtils.objectToBytes(DataType.FLOAT, f)), f);

        double d = 123456.7890123;
        assertEquals(SerdeUtils.bytesToObject(DataType.DOUBLE, SerdeUtils.objectToBytes(DataType.DOUBLE, d)), d);

        String str = "abcdefg";
        assertEquals(SerdeUtils.bytesToObject(DataType.STRING, SerdeUtils.objectToBytes(DataType.STRING, str)), str);

        byte[] bts = "bytes".getBytes();
        assertEquals(SerdeUtils.bytesToObject(DataType.BYTES, SerdeUtils.objectToBytes(DataType.BYTES, bts)), bts);

        List<Integer> il = Arrays.asList(1, 2, 3);
        assertEquals(SerdeUtils.bytesToObject(DataType.INT_LIST, SerdeUtils.objectToBytes(DataType.INT_LIST, il)), il);

        List<Long> ll = Arrays.asList(100000000000001L, 100000000000002L, 100000000000003L);
        assertEquals(SerdeUtils.bytesToObject(DataType.LONG_LIST, SerdeUtils.objectToBytes(DataType.LONG_LIST, ll)),
                ll);

        List<Float> fl = Arrays.asList(1.234f, 1.235f, 1.236f);
        assertEquals(SerdeUtils.bytesToObject(DataType.FLOAT_LIST, SerdeUtils.objectToBytes(DataType.FLOAT_LIST, fl)),
                fl);

        List<Double> dl = Arrays.asList(123.456, 123.457, 123.458);
        assertEquals(SerdeUtils.bytesToObject(DataType.DOUBLE_LIST, SerdeUtils.objectToBytes(DataType.DOUBLE_LIST, dl)),
                dl);

        List<String> sl = Arrays.asList("abc", "def", "ghi");
        assertEquals(SerdeUtils.bytesToObject(DataType.STRING_LIST, SerdeUtils.objectToBytes(DataType.STRING_LIST, sl)),
                sl);

    }
}

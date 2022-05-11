package com.alibaba.maxgraph.tests.frontend;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.alibaba.maxgraph.sdkcommon.schema.PropertyValue;
import com.alibaba.maxgraph.sdkcommon.util.PkHashUtils;
import com.alibaba.maxgraph.compiler.api.schema.DataType;

import org.junit.Test;

import java.util.Arrays;

public class PkHashUtilsTest {

    @Test
    public void testHash() {
        byte[] bytes = new PropertyValue(DataType.BOOL, true).getValBytes();
        assertEquals(-5831416668854475863L, PkHashUtils.hash(1, Arrays.asList(bytes)));

        bytes = new PropertyValue(DataType.BOOL, false).getValBytes();
        assertEquals(4016402799948355554L, PkHashUtils.hash(1, Arrays.asList(bytes)));

        bytes = new PropertyValue(DataType.SHORT, 5).getValBytes();
        assertEquals(-6461270943182640449L, PkHashUtils.hash(1, Arrays.asList(bytes)));

        bytes = new PropertyValue(DataType.INT, 6).getValBytes();
        assertEquals(-5566731246168985051L, PkHashUtils.hash(1, Arrays.asList(bytes)));

        bytes = new PropertyValue(DataType.LONG, 7).getValBytes();
        assertEquals(-2037727154783756963L, PkHashUtils.hash(1, Arrays.asList(bytes)));

        bytes = new PropertyValue(DataType.FLOAT, 5.5).getValBytes();
        assertEquals(3718470844984468536L, PkHashUtils.hash(1, Arrays.asList(bytes)));

        bytes = new PropertyValue(DataType.DOUBLE, 11.5).getValBytes();
        assertEquals(-6473588278280513549L, PkHashUtils.hash(1, Arrays.asList(bytes)));

        bytes = new PropertyValue(DataType.BYTES, new byte[] {1, 2, 3}).getValBytes();
        assertEquals(-5358885630460755339L, PkHashUtils.hash(1, Arrays.asList(bytes)));

        bytes = new PropertyValue(DataType.STRING, "abc").getValBytes();
        assertEquals(-1681001599945530356L, PkHashUtils.hash(1, Arrays.asList(bytes)));

        bytes =
                new PropertyValue(DataType.INT_LIST, Arrays.asList(400, 500, 600, 700))
                        .getValBytes();
        assertEquals(-6843607735995935492L, PkHashUtils.hash(1, Arrays.asList(bytes)));

        bytes =
                new PropertyValue(
                                DataType.LONG_LIST,
                                Arrays.asList(
                                        111111111111L, 222222222222L, 333333333333L, 444444444444L))
                        .getValBytes();
        assertEquals(7595853299324856776L, PkHashUtils.hash(1, Arrays.asList(bytes)));

        bytes =
                new PropertyValue(
                                DataType.FLOAT_LIST,
                                Arrays.asList(1.234567F, 12.34567F, 123.4567F, 1234.567F))
                        .getValBytes();
        assertEquals(-866958979581072036L, PkHashUtils.hash(1, Arrays.asList(bytes)));

        bytes =
                new PropertyValue(
                                DataType.DOUBLE_LIST,
                                Arrays.asList(987654.3, 98765.43, 9876.543, 987.6543))
                        .getValBytes();
        assertEquals(-1870641235154602931L, PkHashUtils.hash(1, Arrays.asList(bytes)));

        bytes =
                new PropertyValue(DataType.STRING_LIST, Arrays.asList("English", "中文"))
                        .getValBytes();
        assertEquals(-5965060437586883158L, PkHashUtils.hash(1, Arrays.asList(bytes)));

        assertEquals(
                7757033342887554736L,
                PkHashUtils.hash(
                        1,
                        Arrays.asList(
                                new PropertyValue(DataType.STRING, "aaa").getValBytes(),
                                new PropertyValue(DataType.LONG, 999999999999L).getValBytes())));
    }

    @Test
    public void benchHash() {
        long i = 0;
        int interval = 10000000;
        long last_time = System.nanoTime();
        while (true) {
            i++;
            String testKey = "test_key_" + i;
            byte[] valBytes = new PropertyValue(DataType.STRING, testKey).getValBytes();
            PkHashUtils.hash(1, Arrays.asList(valBytes));
            if (i % interval == 0) {
                long now_time = System.nanoTime();
                long cost = now_time - last_time;
                last_time = now_time;
                System.out.println(
                        "total ["
                                + i
                                + "], cost ["
                                + cost / 1000000
                                + "], interval_speed ["
                                + (int) (1000000000.0 * interval / cost)
                                + "]");
            }
        }
    }
}

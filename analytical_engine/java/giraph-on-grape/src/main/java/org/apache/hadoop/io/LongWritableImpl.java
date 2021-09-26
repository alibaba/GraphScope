package org.apache.hadoop.io;

import java.lang.reflect.Field;
import sun.misc.Unsafe;

public class LongWritableImpl {

    private static Unsafe unsafe;
    private static int POOL_SIZE = 1024 * 1024;

    static {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (Unsafe) field.get(null);
            System.out.println("Got unsafe");
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }

    private long address;

    public LongWritableImpl() {
        address = unsafe.allocateMemory(8);
    }

    public LongWritableImpl(long value) {
        address = unsafe.allocateMemory(8);
        unsafe.putLong(address, value);
    }

    public void set(long value) {
        unsafe.putLong(address, value);
    }

    public long get() {
        return unsafe.getLong(address);
    }
}

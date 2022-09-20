/*
 * Copyright 2022 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.graphscope.stdcxx;

import com.alibaba.fastffi.CXXOperator;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFIPointerImpl;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.llvm4jni.runtime.JavaRuntime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a FFIWrapper for std::vector. The code origins from the generated code via FFI and
 * llvm4jni, with hands-on optimization.
 */
@FFIForeignType(value = "std::vector<char>", factory = FFIByteVectorFactory.class)
@FFITypeAlias("std::vector<char>")
@FFISynthetic("com.alibaba.graphscope.stdcxx.StdVector")
public class FFIByteVector extends FFIPointerImpl implements StdVector<Byte> {

    private static Logger logger = LoggerFactory.getLogger(FFIByteVector.class);

    public static final int SIZE = _elementSize$$$();
    // Actual starting address of data
    private long objAddress;
    public static final int HASH_SHIFT;
    // We don't use reserve to allocate memory ,we use resize.
    // It seems reserved memory isn't allowed to be set directly.
    public long size;

    public FFIByteVector(long address) {
        super(address);
        objAddress = JavaRuntime.getLong(address);
        size = nativeSize(this.address);
    }

    /** update the cached objAddress. */
    public void touch() {
        objAddress = JavaRuntime.getLong(address);
        size = nativeSize(this.address);
    }

    private static final int _elementSize$$$() {
        return 24;
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            FFIByteVector that = (FFIByteVector) o;
            return this.address == that.address;
        } else {
            return false;
        }
    }

    public int hashCode() {
        return (int) (this.address >> HASH_SHIFT);
    }

    public String toString() {
        return this.getClass().getName() + "@" + Long.toHexString(this.address);
    }

    public long capacity() {
        return nativeCapacity(this.address);
    }

    public static long nativeCapacity(long var0) {
        long var2 = JavaRuntime.getLong(var0 + 16L);
        long var4 = JavaRuntime.getLong(var0);
        return var2 - var4;
    }

    public void clear() {
        nativeClear(this.address);
    }

    public static void nativeClear(long var0) {
        long var2 = JavaRuntime.getLong(var0);
        long var4 = var0 + 8L;
        if (JavaRuntime.getLong(var4) != var2) {
            JavaRuntime.putLong(var4, var2);
        }
    }

    public long data() {
        return nativeData(this.address);
    }

    public static long nativeData(long var0) {
        return JavaRuntime.getLong(var0);
    }

    @CXXOperator("delete")
    public void delete() {
        nativeDelete(this.address);
    }

    @CXXOperator("delete")
    public static native void nativeDelete(long var0);

    @CXXOperator("[]")
    @CXXReference
    public Byte get(long arg0) {
        return (byte) JavaRuntime.getByte(objAddress + arg0);
    }

    public byte getRaw(long arg0) {
        return JavaRuntime.getByte(objAddress + arg0);
    }

    public char getRawChar(long arg0) {
        return JavaRuntime.getChar(objAddress + arg0);
    }

    public short getRawShort(long arg0) {
        return JavaRuntime.getShort(objAddress + arg0);
    }

    public int getRawInt(long arg0) {
        return JavaRuntime.getInt(objAddress + arg0);
    }

    public float getRawFloat(long arg0) {
        return JavaRuntime.getFloat(objAddress + arg0);
    }

    public double getRawDouble(long arg0) {
        return JavaRuntime.getDouble(objAddress + arg0);
    }

    public long getRawLong(long arg0) {
        return JavaRuntime.getLong(objAddress + arg0);
    }

    /**
     * Read serveral bytes to a byte array.
     *
     * @param b Receive data
     * @param bOff first place to put
     * @param offset offset in this buffer, from where we read now.
     * @param size how many to read
     */
    public void getRawBytes(byte[] b, int bOff, long offset, int size) {
        JavaRuntime.UNSAFE.copyMemory(
                null,
                objAddress + offset,
                b,
                bOff + JavaRuntime.UNSAFE.arrayBaseOffset(byte[].class),
                size);
    }

    /**
     * This function copy another vector's memory after this vector. Shall be used in InputStream.
     *
     * <p>Check whether there are enough spaces, and then copy [vector.data(), vector.data() +
     * vector.size()] to [readableLimit, readableLimit + vector.size()].
     *
     * @param readAbleLimit The start pos of this copy action.
     * @param vector vector to be appended.
     */
    public void appendVector(long readAbleLimit, FFIByteVector vector) {
        int vecSize = (int) vector.size();
        ensure(readAbleLimit, vecSize);
        // CAUTION: vector.objAddress can be invalid when c++ call resize, call data make sure we
        // are safe.
        JavaRuntime.copyMemory(vector.data() + 0, objAddress + readAbleLimit, vecSize);
    }

    @CXXOperator("[]")
    @CXXReference
    public static byte nativeGet(long var0, long var2) {
        return (byte) JavaRuntime.getByte(JavaRuntime.getLong(var0) + var2);
    }

    public void push_back(@CXXValue Byte arg0) {
        nativePush_back(this.address, arg0);
    }

    public static native void nativePush_back(long var0, byte var2);

    public void reserve(long arg0) {
        nativeReserve(this.address, arg0);
    }

    public static native void nativeReserve(long var0, long var2);

    public void resize(long arg0) {
        nativeResize(this.address, arg0);
        // After resize, we need to update obj address and current size
        objAddress = JavaRuntime.getLong(address);
        size = arg0;
    }

    /**
     * Resize with additional space.
     *
     * @param addSpace more space need.
     */
    public void resizeWithMoreSpace(long addSpace) {
        long newSize = size() + addSpace;
        resize(newSize);
    }

    public static native void nativeResize(long var0, long var2);

    /**
     * Ensure there at at least requiredSize after offset.
     *
     * @param offset offset to check
     * @param requiredSize additional spaced needed.
     */
    public void ensure(long offset, int requiredSize) {
        long minSize = requiredSize + offset;
        if (minSize <= size) {
            return;
        }
        long oldSize = size;
        long newSize = oldSize + (oldSize >> 1);
        if (newSize - minSize < 0) {
            newSize = minSize;
        }
        nativeResize(this.address, newSize);
        this.objAddress = JavaRuntime.getLong(address);
        this.size = newSize;
    }

    @CXXOperator("[]")
    public void set(long arg0, @CXXReference Byte arg1) {
        ensure(arg0, 1);
        JavaRuntime.putByte(objAddress + arg0, arg1);
    }

    public void setRawByte(long arg0, byte arg1) {
        JavaRuntime.putByte(objAddress + arg0, arg1);
    }

    public void setRawShort(long arg0, short arg1) {
        JavaRuntime.putShort(objAddress + arg0, arg1);
    }

    public void setRawChar(long arg0, char arg1) {
        JavaRuntime.putChar(objAddress + arg0, arg1);
    }

    public void setRawInt(long arg0, int arg1) {
        JavaRuntime.putInt(objAddress + arg0, arg1);
    }

    public void setRawLong(long arg0, long arg1) {
        JavaRuntime.putLong(objAddress + arg0, arg1);
    }

    public void setRawFloat(long arg0, float arg1) {
        JavaRuntime.putFloat(objAddress + arg0, arg1);
    }

    public void setRawDouble(long arg0, double arg1) {
        JavaRuntime.putDouble(objAddress + arg0, arg1);
    }

    public void finishSetting(long offset) {
        if (offset > size) {
            logger.error("Impossible ");
            return;
        }
        nativeResize(this.address, offset);
        size = offset;
    }

    @CXXOperator("[]")
    public static void nativeSet(long var0, long var2, byte var4) {
        JavaRuntime.putByte(JavaRuntime.getLong(var0) + var2, var4);
    }

    public void setAddress(long arg0) {
        this.address = arg0;
    }

    public long size() {
        return nativeSize(this.address);
    }

    public static long nativeSize(long var0) {
        long var2 = JavaRuntime.getLong(var0 + 8L);
        long var4 = JavaRuntime.getLong(var0);
        return var2 - var4;
    }

    public static native long nativeCreateFactory0();

    static {
        assert SIZE > 0;

        HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);

        assert HASH_SHIFT > 0;
    }
}

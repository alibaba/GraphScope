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
public class FakeFFIByteVector extends FFIPointerImpl implements StdVector<Byte> {

    private static Logger logger = LoggerFactory.getLogger(FFIByteVector.class);

    public static final int SIZE = _elementSize$$$();
    // Actual starting address of data
    private long objAddress;
    public static final int HASH_SHIFT;
    // We don't use reserve to allocate memory ,we use resize.
    // It seems reserved memory isn't allowed to be set directly.
    public long size;

    public FakeFFIByteVector(long address, long length) {
        super(address);
        objAddress = address;
        size = length;
    }

    private static final int _elementSize$$$() {
        return 24;
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            FakeFFIByteVector that = (FakeFFIByteVector) o;
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
        return size;
    }

    public void clear() {
        throw new IllegalStateException("can not apply to fake vector");
    }

    public long data() {
        return address;
    }

    @CXXOperator("delete")
    public void delete() {
        throw new IllegalStateException("can not apply to fake vector");
    }

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

    @CXXOperator("[]")
    @CXXReference
    public static byte nativeGet(long var0, long var2) {
        return (byte) JavaRuntime.getByte(JavaRuntime.getLong(var0) + var2);
    }

    public void push_back(@CXXValue Byte arg0) {
        throw new IllegalStateException("can not apply to fake vector");
    }

    public void reserve(long arg0) {
        throw new IllegalStateException("can not apply to fake vector");
    }

    public void resize(long arg0) {
        throw new IllegalStateException("can not apply to fake vector");
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

    /**
     * Ensure there at at least requiredSize after offset.
     *
     * @param offset offset to check
     * @param requiredSize additional spaced needed.
     */
    public void ensure(long offset, int requiredSize) {
        throw new IllegalStateException("can not apply to fake vector");
    }

    @CXXOperator("[]")
    public void set(long arg0, @CXXReference Byte arg1) {
        throw new IllegalStateException("can not apply to fake vector");
    }

    public void setRawByte(long arg0, byte arg1) {
        throw new IllegalStateException("can not apply to fake vector");
    }

    public void setRawShort(long arg0, short arg1) {
        throw new IllegalStateException("can not apply to fake vector");
    }

    public void setRawChar(long arg0, char arg1) {
        throw new IllegalStateException("can not apply to fake vector");
    }

    public void setRawInt(long arg0, int arg1) {
        throw new IllegalStateException("can not apply to fake vector");
    }

    public void setRawLong(long arg0, long arg1) {
        throw new IllegalStateException("can not apply to fake vector");
    }

    public void setRawFloat(long arg0, float arg1) {
        throw new IllegalStateException("can not apply to fake vector");
    }

    public void setRawDouble(long arg0, double arg1) {
        throw new IllegalStateException("can not apply to fake vector");
    }

    public void finishSetting(long offset) {
        throw new IllegalStateException("can not apply to fake vector");
    }

    @CXXOperator("[]")
    public static void nativeSet(long var0, long var2, byte var4) {
        throw new IllegalStateException("can not apply to fake vector");
    }

    public void setAddress(long arg0) {
        this.address = arg0;
    }

    public long size() {
        return size;
    }

    public static native long nativeCreateFactory0();

    static {
        assert SIZE > 0;

        HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);

        assert HASH_SHIFT > 0;
    }
}

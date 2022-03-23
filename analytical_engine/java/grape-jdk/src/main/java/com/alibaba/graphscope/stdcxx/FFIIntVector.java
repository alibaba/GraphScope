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

//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package com.alibaba.graphscope.stdcxx;

import com.alibaba.fastffi.CXXOperator;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIPointerImpl;
import com.alibaba.fastffi.llvm4jni.runtime.JavaRuntime;

public class FFIIntVector extends FFIPointerImpl implements StdVector<Integer> {
    public static final int SIZE = _elementSize$$$();
    private long objAddress;
    public static final int HASH_SHIFT;
    // We don't use reserve to allocate memory ,we use resize.
    // It seems reserved memory isn't allowed to be set directly.
    public long size;
    private int curIndex;

    public FFIIntVector(long address) {
        super(address);
        objAddress = JavaRuntime.getLong(address);
        size = nativeSize(this.address);
        curIndex = 0;
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
            FFIIntVector that = (FFIIntVector) o;
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
        return var2 - var4 >> (int) 2L;
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
    public Integer get(long arg0) {
        //        return new Integer(nativeGet(this.address, arg0));
        return new Integer(JavaRuntime.getInt(objAddress + arg0 * 4L));
    }

    @CXXOperator("[]")
    @CXXReference
    public static int nativeGet(long var0, long var2) {
        return JavaRuntime.getInt(JavaRuntime.getLong(var0) + var2 * 4L);
    }

    public void push_back(@CXXValue Integer arg0) {
        if (size == 0) {
            resize(8);
            touch();
        } else if (curIndex >= size) {
            resize(size + (size >> 1));
            touch();
        }
        set(curIndex++, arg0);
        //        nativePush_back(this.address, arg0);
    }

    public void finishSetting() {
        nativeResize(this.address, curIndex);
    }

    public static native void nativePush_back(long var0, int var2);

    public void reserve(long arg0) {
        nativeReserve(this.address, arg0);
    }

    public static native void nativeReserve(long var0, long var2);

    public void resize(long arg0) {
        nativeResize(this.address, arg0);
    }

    public static native void nativeResize(long var0, long var2);

    @CXXOperator("[]")
    public void set(long arg0, @CXXReference Integer arg1) {
        JavaRuntime.putInt(objAddress + arg0 * 4L, arg1);
        //        nativeSet(this.address, arg0, arg1);
    }

    @CXXOperator("[]")
    public static void nativeSet(long var0, long var2, int var4) {
        JavaRuntime.putInt(JavaRuntime.getLong(var0) + var2 * 4L, var4);
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
        return var2 - var4 >> (int) 2L;
    }

    public static native long nativeCreateFactory0();

    static {
        assert SIZE > 0;

        HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);

        assert HASH_SHIFT > 0;
    }
}

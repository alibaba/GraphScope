//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package com.alibaba.graphscope.stdcxx;

import com.alibaba.fastffi.CXXOperator;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFIPointerImpl;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.llvm4jni.runtime.JavaRuntime;

@FFIForeignType(value = "std::vector<std::vector<int32_t>>", factory = FFIIntVecVectorFactory.class)
@FFISynthetic("com.alibaba.graphscope.stdcxx.StdVector")
public class FFIIntVecVector extends FFIPointerImpl implements StdVector<StdVector<Integer>> {

    public static final int SIZE = _elementSize$$$();
    private long objAddress;
    public static final int HASH_SHIFT;

    public FFIIntVecVector(long address) {
        super(address);
        objAddress = JavaRuntime.getLong(address);
    }

    private static final int _elementSize$$$() {
        return 24;
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            FFIIntVecVector that = (FFIIntVecVector) o;
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
        return (var2 - var4) / 24L;
    }

    public void clear() {
        nativeClear(this.address);
    }

    public static native void nativeClear(long var0);

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
    public StdVector<Integer> get(long arg0) {
        //        long ret$ = nativeGet(this.address, arg0);
        long ret$ = objAddress + arg0 * 24;
        return new FFIIntVector(ret$);
    }

    @CXXOperator("[]")
    @CXXReference
    public static long nativeGet(long var0, long var2) {
        return JavaRuntime.getLong(var0) + var2 * 24L;
    }

    public void push_back(@CXXValue StdVector<Integer> arg0) {
        nativePush_back(this.address, ((FFIPointerImpl) arg0).address);
    }

    public static native void nativePush_back(long var0, long var2);

    public void reserve(long arg0) {
        nativeReserve(this.address, arg0);
    }

    public static native void nativeReserve(long var0, long var2);

    public void resize(long arg0) {
        nativeResize(this.address, arg0);
    }

    public static native void nativeResize(long var0, long var2);

    @CXXOperator("[]")
    public void set(long arg0, @CXXReference StdVector<Integer> arg1) {
        nativeSet(this.address, arg0, ((FFIPointerImpl) arg1).address);
    }

    @CXXOperator("[]")
    public static native void nativeSet(long var0, long var2, long var4);

    public void setAddress(long arg0) {
        this.address = arg0;
    }

    public long size() {
        return nativeSize(this.address);
    }

    public static long nativeSize(long var0) {
        long var2 = JavaRuntime.getLong(var0 + 8L);
        long var4 = JavaRuntime.getLong(var0);
        return (var2 - var4) / 24L;
    }

    public static native long nativeCreateFactory0();

    static {
        assert SIZE > 0;

        HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);

        assert HASH_SHIFT > 0;
    }
}

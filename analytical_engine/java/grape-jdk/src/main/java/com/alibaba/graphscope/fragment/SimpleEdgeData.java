package com.alibaba.graphscope.fragment;

import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.graphscope.ds.TypedArray;

public interface SimpleEdgeData<EDATA_T> {
    @FFINameAlias("get_edata_array_accessor")
    @CXXReference
    TypedArray<EDATA_T> getEdataArrayAccessor();
}

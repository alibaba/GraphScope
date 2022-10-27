package com.alibaba.graphscope.fragment;

import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.graphscope.ds.TypedArray;

public interface SimpleVertexData<VDATA_T> {
    @FFINameAlias("get_vdata_array_accessor")
    @CXXReference
    TypedArray<VDATA_T> getVdataArrayAccessor();
}

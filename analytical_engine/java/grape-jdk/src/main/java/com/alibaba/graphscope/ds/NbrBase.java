package com.alibaba.graphscope.ds;

import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFIGetter;
import com.alibaba.fastffi.FFIPointer;

public interface NbrBase<VID_T, EDATA_T> extends FFIPointer {
    Vertex<VID_T> neighbor();

    /**
     * Get the edge data.
     *
     * @return edge data.
     */
    @FFIGetter
    @CXXReference
    EDATA_T data();
}

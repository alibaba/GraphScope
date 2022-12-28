package com.alibaba.graphscope.graphx;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.utils.CppClassName;
import com.alibaba.graphscope.utils.CppHeaderName;

@FFIGen
@CXXHead(CppHeaderName.CORE_JAVA_GRAPHX_RAW_DATA_H)
@FFITypeAlias(CppClassName.GS_GRAPHX_RAW_DATA)
public interface GraphXRawData<OID_T, VID_T, VD_T, ED_T> extends FFIPointer {
    long id();

    @FFINameAlias("GetEdgeNum")
    long getEdgeNum();

    @FFINameAlias("GetVertexNum")
    long getVertexNum();
}

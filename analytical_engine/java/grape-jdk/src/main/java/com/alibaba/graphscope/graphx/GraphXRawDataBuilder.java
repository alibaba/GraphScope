package com.alibaba.graphscope.graphx;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.stdcxx.StdSharedPtr;
import com.alibaba.graphscope.stdcxx.StdVector;
import com.alibaba.graphscope.utils.CppClassName;
import com.alibaba.graphscope.utils.CppHeaderName;

@FFIGen
@CXXHead(CppHeaderName.CORE_JAVA_GRAPHX_RAW_DATA_H)
@FFITypeAlias(CppClassName.GS_GRAPHX_RAW_DATA_BUILDER)
public interface GraphXRawDataBuilder<OID_T, VID_T, VD_T, ED_T> extends FFIPointer {

    @FFINameAlias("MySeal")
    @CXXValue
    StdSharedPtr<GraphXRawData<OID_T, VID_T, VD_T, ED_T>> seal(@CXXReference VineyardClient client);

    @FFIFactory
    interface Factory<OID_T, VID_T, VD_T, ED_T> {

        GraphXRawDataBuilder<OID_T, VID_T, VD_T, ED_T> create(
                @CXXReference VineyardClient client,
                @CXXReference StdVector<OID_T> oids,
                @CXXReference StdVector<VD_T> vdatas,
                @CXXReference StdVector<OID_T> srcOids,
                @CXXReference StdVector<OID_T> dstOids,
                @CXXReference StdVector<ED_T> edatas);
    }
}

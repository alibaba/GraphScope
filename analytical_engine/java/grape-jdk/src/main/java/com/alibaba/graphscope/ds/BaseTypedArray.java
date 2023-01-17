package com.alibaba.graphscope.ds;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFISettablePointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.utils.CppClassName;
import com.alibaba.graphscope.utils.CppHeaderName;

@FFIGen
@CXXHead(CppHeaderName.ARROW_PROJECTED_FRAGMENT_H)
@FFITypeAlias(CppClassName.GS_ARROW_PROJECTED_FRAGMENT_IMPL_TYPED_ARRAY)
public interface BaseTypedArray<DATA_T> extends FFISettablePointer {

    @FFINameAlias("GetLength")
    long getLength();
}

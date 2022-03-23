package com.alibaba.graphscope.ds;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXOperator;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.utils.CppClassName;
import com.alibaba.graphscope.utils.CppHeaderName;
import com.alibaba.graphscope.utils.JNILibraryName;

@FFIGen(library = JNILibraryName.JNI_LIBRARY_NAME)
@CXXHead(CppHeaderName.ARROW_PROJECTED_FRAGMENT_H)
@FFITypeAlias(CppClassName.GS_ARROW_PROJECTED_FRAGMENT_IMPL_TYPED_ARRAY)
public interface TypedArray<DATA_T> extends FFIPointer {
    @CXXValue
    @CXXOperator("[]")
    DATA_T get(long index);
}

package com.alibaba.graphscope.arrow.array;

import static com.alibaba.graphscope.utils.CppClassName.GS_ARROW_STRING_ARRAY_BUILDER;
import static com.alibaba.graphscope.utils.CppHeaderName.CORE_JAVA_TYPE_ALIAS_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIConst;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.arrow.Status;
import com.alibaba.graphscope.stdcxx.CCharPointer;
import com.alibaba.graphscope.utils.JNILibraryName;

@FFIGen(library = JNILibraryName.JNI_LIBRARY_NAME)
@CXXHead(CORE_JAVA_TYPE_ALIAS_H)
@FFITypeAlias(GS_ARROW_STRING_ARRAY_BUILDER)
public interface ArrowStringArrayBuilder extends FFIPointer {

    @FFINameAlias("Reserve")
    @CXXValue
    Status reserve(long additionalCapacity);

    @FFINameAlias("ReserveData")
    @CXXValue
    Status reserveData(long additionalBytes);

    @FFINameAlias("UnsafeAppend")
    void unsafeAppend(@FFIConst @CXXReference CCharPointer ptr, int length);

    @FFIFactory
    interface Factory {

        ArrowStringArrayBuilder create();
    }
}

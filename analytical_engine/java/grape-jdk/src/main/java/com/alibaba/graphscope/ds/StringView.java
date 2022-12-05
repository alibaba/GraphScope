package com.alibaba.graphscope.ds;

import static com.alibaba.graphscope.utils.CppHeaderName.VINEYARD_ARROW_UTILS_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXOperator;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFIStringProvider;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.utils.JNILibraryName;

@FFIGen(library = JNILibraryName.JNI_LIBRARY_NAME)
@CXXHead(VINEYARD_ARROW_UTILS_H)
@FFITypeAlias("vineyard::arrow_string_view")
public interface StringView extends FFIStringProvider, FFIPointer {

    long data();

    @CXXOperator("[]")
    byte byteAt(long index);

    long size();

    boolean empty();
}

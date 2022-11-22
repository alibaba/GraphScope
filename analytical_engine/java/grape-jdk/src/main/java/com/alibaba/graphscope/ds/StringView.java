package com.alibaba.graphscope.ds;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXOperator;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFIStringProvider;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.utils.JNILibraryName;

@FFIGen(library = JNILibraryName.JNI_LIBRARY_NAME)
@CXXHead("arrow/util/string_view.h")
@FFITypeAlias("vineyard::arrow_string_view")
public interface StringView extends FFIStringProvider, FFIPointer {

    long data();

    @CXXOperator("[]")
    byte byteAt(long index);

    long size();

    boolean empty();
}

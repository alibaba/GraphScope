package com.alibaba.graphscope.arrow.array;

import static com.alibaba.graphscope.utils.CppClassName.GS_ARROW_STRING_ARRAY;
import static com.alibaba.graphscope.utils.CppHeaderName.CORE_JAVA_TYPE_ALIAS_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIByteString;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.ds.StringView;
import com.alibaba.graphscope.utils.JNILibraryName;

@FFIGen(library = JNILibraryName.JNI_LIBRARY_NAME)
@CXXHead(CORE_JAVA_TYPE_ALIAS_H)
@FFITypeAlias(GS_ARROW_STRING_ARRAY)
public interface StringArrowArray extends BaseArrowArray<FFIByteString> {

    @FFINameAlias("GetString")
    @CXXValue
    FFIByteString getString(long index);

    @FFINameAlias("GetView")
    @CXXValue
    StringView getView(long index);
}

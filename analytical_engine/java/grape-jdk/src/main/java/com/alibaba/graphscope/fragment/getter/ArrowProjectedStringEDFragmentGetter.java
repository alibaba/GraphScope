package com.alibaba.graphscope.fragment.getter;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.fragment.ArrowProjectedStringEDFragment;
import com.alibaba.graphscope.graphx.VineyardClient;
import com.alibaba.graphscope.stdcxx.StdSharedPtr;
import com.alibaba.graphscope.utils.CppClassName;
import com.alibaba.graphscope.utils.CppHeaderName;
import com.alibaba.graphscope.utils.JNILibraryName;

@FFIGen(library = JNILibraryName.JNI_LIBRARY_NAME)
@CXXHead(CppHeaderName.CORE_JAVA_TYPE_ALIAS_H)
@CXXHead(CppHeaderName.CORE_JAVA_FRAGMENT_GETTER_H)
@FFITypeAlias(CppClassName.CPP_ARROW_PROJECTED_STRING_ED_FRAGMENT_GETTER)
public interface ArrowProjectedStringEDFragmentGetter<OID, VID, VD> extends FFIPointer {

    @FFINameAlias("Get")
    @CXXValue
    StdSharedPtr<ArrowProjectedStringEDFragment<OID, VID, VD>> get(
            @CXXReference VineyardClient client, long objectID);

    @FFIFactory
    interface Factory<OID, VID, VD> {
        ArrowProjectedStringEDFragmentGetter<OID, VID, VD> create();
    }
}

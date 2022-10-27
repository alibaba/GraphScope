package com.alibaba.graphscope.fragment.mapper;

import static com.alibaba.graphscope.utils.CppClassName.CPP_ARROW_PROJECTED_STRING_VD_FRAGMENT_MAPPER;
import static com.alibaba.graphscope.utils.CppHeaderName.ARROW_PROJECTED_FRAGMENT_MAPPER_H;
import static com.alibaba.graphscope.utils.CppHeaderName.CORE_JAVA_TYPE_ALIAS_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.arrow.array.ArrowArrayBuilder;
import com.alibaba.graphscope.arrow.array.ArrowStringArrayBuilder;
import com.alibaba.graphscope.fragment.ArrowFragment;
import com.alibaba.graphscope.fragment.ArrowProjectedStringVDFragment;
import com.alibaba.graphscope.graphx.VineyardClient;
import com.alibaba.graphscope.stdcxx.StdSharedPtr;
import com.alibaba.graphscope.utils.JNILibraryName;

@FFIGen(library = JNILibraryName.JNI_LIBRARY_NAME)
@CXXHead(ARROW_PROJECTED_FRAGMENT_MAPPER_H)
@CXXHead(CORE_JAVA_TYPE_ALIAS_H)
@FFITypeAlias(CPP_ARROW_PROJECTED_STRING_VD_FRAGMENT_MAPPER)
public interface ArrowProjectedStringVDFragmentMapper<OID_T, VID_T, NEW_E_T> extends FFIPointer {

    @FFINameAlias("Map")
    @CXXValue
    StdSharedPtr<ArrowProjectedStringVDFragment<OID_T, VID_T, NEW_E_T>> map(
            @CXXReference StdSharedPtr<ArrowFragment<OID_T>> oldFrag,
            int vLabel,
            int eLabel,
            @CXXReference ArrowStringArrayBuilder vdBuilder,
            @CXXReference ArrowArrayBuilder<NEW_E_T> edBuilder,
            @CXXReference VineyardClient client);

    // only vd
    @FFINameAlias("Map")
    @CXXValue
    StdSharedPtr<ArrowProjectedStringVDFragment<OID_T, VID_T, NEW_E_T>> map(
            @CXXReference StdSharedPtr<ArrowFragment<OID_T>> oldFrag,
            int vLabe,
            int oldEPropId,
            @CXXReference ArrowStringArrayBuilder vdBuilder,
            @CXXReference VineyardClient client);

    @FFIFactory
    interface Factory<OID_T, VID_T, NEW_E_T> {

        ArrowProjectedStringVDFragmentMapper<OID_T, VID_T, NEW_E_T> create();
    }
}

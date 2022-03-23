package com.alibaba.graphscope.fragment;

import static com.alibaba.graphscope.utils.CppClassName.CPP_ARROW_PROJECTED_STRING_VD_FRAGMENT;
import static com.alibaba.graphscope.utils.CppHeaderName.ARROW_PROJECTED_FRAGMENT_H;
import static com.alibaba.graphscope.utils.CppHeaderName.CORE_JAVA_TYPE_ALIAS_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.impl.CXXStdString;
import com.alibaba.graphscope.ds.ProjectedAdjList;
import com.alibaba.graphscope.ds.StringTypedArray;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.utils.JNILibraryName;

/**
 * Java wrapper for <a href=
 * "https://github.com/alibaba/GraphScope/blob/main/analytical_engine/core/fragment/arrow_projected_fragment.h#L338">ArrowProjectedFragment</a>
 *
 * @param <OID_T> original id type
 * @param <VID_T> vertex id type
 * @param <EDATA_T> edge data type
 */
@FFIGen(library = JNILibraryName.JNI_LIBRARY_NAME)
@CXXHead(ARROW_PROJECTED_FRAGMENT_H)
@CXXHead(CORE_JAVA_TYPE_ALIAS_H)
@CXXHead(system = "stdint.h")
@FFITypeAlias(CPP_ARROW_PROJECTED_STRING_VD_FRAGMENT)
public interface ArrowProjectedStringVDFragment<OID_T, VID_T, EDATA_T>
        extends BaseArrowProjectedFragment<OID_T, VID_T, CXXStdString, EDATA_T>,
                SimpleEdgeData<EDATA_T> {

    @FFINameAlias("GetIncomingAdjList")
    @CXXValue
    ProjectedAdjList<VID_T, EDATA_T> getIncomingAdjList(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetOutgoingAdjList")
    @CXXValue
    ProjectedAdjList<VID_T, EDATA_T> getOutgoingAdjList(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetData")
    @CXXValue
    CXXStdString getData(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("get_vdata_array_accessor")
    @CXXReference
    StringTypedArray getVdataArrayAccessor();
}

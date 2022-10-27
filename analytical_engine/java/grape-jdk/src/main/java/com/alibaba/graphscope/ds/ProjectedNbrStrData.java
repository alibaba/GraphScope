package com.alibaba.graphscope.ds;

import static com.alibaba.graphscope.utils.CppClassName.PROJECTED_NBR_STR_DATA;
import static com.alibaba.graphscope.utils.CppHeaderName.ARROW_PROJECTED_FRAGMENT_H;
import static com.alibaba.graphscope.utils.CppHeaderName.CORE_JAVA_TYPE_ALIAS_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXOperator;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.impl.CXXStdString;
import com.alibaba.graphscope.utils.JNILibraryName;

/**
 * Definition of Nbr for projected fragment <a href="https://github.com/alibaba/GraphScope/blob/main/analytical_engine/core/fragment/arrow_projected_fragment.h#L121">ProjectedNbr</a>.
 * Representing the neighboring vertex.
 *
 * @param <VID_T> vertex id type.
 */
@FFIGen(library = JNILibraryName.JNI_LIBRARY_NAME)
@CXXHead(ARROW_PROJECTED_FRAGMENT_H)
@CXXHead(CORE_JAVA_TYPE_ALIAS_H)
@FFITypeAlias(PROJECTED_NBR_STR_DATA)
public interface ProjectedNbrStrData<VID_T> extends NbrBase<VID_T, CXXStdString> {

    /**
     * Get the neighbor vertex.
     *
     * @return vertex.
     */
    @Override
    @CXXValue
    Vertex<VID_T> neighbor();

    /**
     * Edge id for this Nbr, inherited from property graph.
     *
     * @return edge id.
     */
    @FFINameAlias("edge_id")
    long edgeId();

    /**
     * Get the edge data.
     *
     * @return edge data.
     */
    @CXXValue
    CXXStdString data();

    /**
     * Self increment.
     *
     * @return increated pointer.
     */
    @CXXOperator("++")
    @CXXReference
    ProjectedNbrStrData<VID_T> inc();

    /**
     * Check equality.
     *
     * @param rhs Nbr to be compared with
     * @return true if is the same pointer.
     */
    @CXXOperator("==")
    boolean eq(@CXXReference ProjectedNbrStrData<VID_T> rhs);

    /**
     * Self decrement.
     *
     * @return decreased pointer
     */
    @CXXOperator("--")
    @CXXReference
    ProjectedNbrStrData<VID_T> dec();
}

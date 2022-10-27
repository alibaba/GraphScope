package com.alibaba.graphscope.ds;

import static com.alibaba.graphscope.utils.CppClassName.PROJECTED_ADJ_LIST_STR_DATA;
import static com.alibaba.graphscope.utils.CppHeaderName.ARROW_PROJECTED_FRAGMENT_H;
import static com.alibaba.graphscope.utils.CppHeaderName.CORE_JAVA_TYPE_ALIAS_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.utils.JNILibraryName;

import java.util.Iterator;

/**
 * AdjList used by {@link com.alibaba.graphscope.fragment.ArrowProjectedFragment}, java wrapper for
 * <a href="https://github.com/alibaba/GraphScope/blob/main/analytical_engine/core/fragment/arrow_projected_fragment.h#L260">ProjectedAdjList</a>.
 *
 * @param <VID_T> vertex id type.
 */
@FFIGen(library = JNILibraryName.JNI_LIBRARY_NAME)
@CXXHead(ARROW_PROJECTED_FRAGMENT_H)
@CXXHead(CORE_JAVA_TYPE_ALIAS_H)
@FFITypeAlias(PROJECTED_ADJ_LIST_STR_DATA)
public interface ProjectedAdjListStrData<VID_T> extends FFIPointer {

    /**
     * Get the the first Nbr.
     *
     * @return first Nbr.
     */
    @CXXValue
    ProjectedNbrStrData<VID_T> begin();

    /**
     * Get the last Nbr.
     *
     * @return last Nbr.
     */
    @CXXValue
    ProjectedNbrStrData<VID_T> end();

    /**
     * Size for this AdjList, i.e. number of nbrs.
     *
     * @return size.
     */
    @FFINameAlias("Size")
    long size();

    /**
     * Check empty.
     *
     * @return true if no nbr.
     */
    @FFINameAlias("Empty")
    boolean empty();

    /**
     * Check no-empty.
     *
     * @return false if empty.
     */
    @FFINameAlias("NotEmpty")
    boolean notEmpty();

    /**
     * The iterator for ProjectedAdjList. You can use enhanced for loop instead of directly using
     * this.
     *
     * @return the iterator.
     */
    default Iterable<ProjectedNbrStrData<VID_T>> iterable() {
        return () ->
                new Iterator<ProjectedNbrStrData<VID_T>>() {
                    ProjectedNbrStrData<VID_T> cur = begin().dec();
                    ProjectedNbrStrData<VID_T> end = end();
                    boolean flag = false;

                    @Override
                    public boolean hasNext() {
                        if (!flag) {
                            cur = cur.inc();
                            flag = !cur.eq(end);
                        }
                        return flag;
                    }

                    @Override
                    public ProjectedNbrStrData<VID_T> next() {
                        flag = false;
                        return cur;
                    }
                };
    }
}

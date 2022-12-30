package com.alibaba.graphscope.arrow.array;

import static com.alibaba.graphscope.utils.CppClassName.GS_ARROW_ARRAY;
import static com.alibaba.graphscope.utils.CppHeaderName.CORE_JAVA_TYPE_ALIAS_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;

/**
 * Base interface for PrimitiveArrowArray and StringArrowArray. Will cause duplicated code generated,
 * but can solve our problem of use specialized class while conform to same method signature.
 * @param <T> type template.
 */
@FFIGen
@CXXHead(CORE_JAVA_TYPE_ALIAS_H)
@FFITypeAlias(GS_ARROW_ARRAY)
public interface BaseArrowArray<T> extends FFIPointer {}

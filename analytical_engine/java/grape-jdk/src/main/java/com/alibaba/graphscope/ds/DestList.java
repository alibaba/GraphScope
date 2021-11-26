/*
 * Copyright 2021 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.ds;

import static com.alibaba.graphscope.utils.CppClassName.GRAPE_DEST_LIST;
import static com.alibaba.graphscope.utils.CppHeaderName.GRAPE_ADJ_LIST_H;
import static com.alibaba.graphscope.utils.JNILibraryName.JNI_LIBRARY_NAME;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXPointer;
import com.alibaba.fastffi.CXXPointerRange;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFIGetter;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;

/**
 * Java Wrapper for <a
 * href="https://github.com/alibaba/libgrape-lite/blob/master/grape/graph/adj_list.h">grape::DestList
 * </a>, contains two fid pointer, the begin and the end.
 */
@FFIGen(library = JNI_LIBRARY_NAME)
@CXXHead(GRAPE_ADJ_LIST_H)
@FFITypeAlias(GRAPE_DEST_LIST)
public interface DestList extends FFIPointer, CXXPointer, CXXPointerRange<FidPointer> {

    /**
     * Fetch the begin fid pointer.
     *
     * @return fetch the end fid pointer.
     */
    @FFIGetter
    FidPointer begin();

    /**
     * Fetch the end fid pointer
     *
     * @return the end fid pointer.
     */
    @FFIGetter
    FidPointer end();

    /**
     * Check empty DestList or not.
     *
     * @return empty or not.
     */
    default boolean empty() {
        return begin().equals(end());
    }

    /**
     * Check if not empty.
     *
     * @return true for not empty.
     */
    default boolean notEmpty() {
        return !empty();
    }
}

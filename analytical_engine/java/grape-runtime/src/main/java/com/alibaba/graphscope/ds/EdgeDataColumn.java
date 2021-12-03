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

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXOperator;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXTemplate;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIConst;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.utils.CppClassName;
import com.alibaba.graphscope.utils.CppHeaderName;
import com.alibaba.graphscope.utils.JNILibraryName;

/**
 * Column abstraction for edge data.
 *
 * @param <DATA_T> edge data type.
 */
@FFIGen(library = JNILibraryName.JNI_LIBRARY_NAME)
@CXXHead(CppHeaderName.CORE_JAVA_TYPE_ALIAS_H)
@FFITypeAlias(CppClassName.EDGE_DATA_COLUMN)
@CXXTemplate(
        cxx = {"int64_t"},
        java = {"Long"})
@CXXTemplate(
        cxx = {"double"},
        java = {"Double"})
@CXXTemplate(
        cxx = {"int32_t"},
        java = {"Integer"})
public interface EdgeDataColumn<DATA_T> extends FFIPointer {

    @CXXOperator(value = "[]")
    @CXXValue
    DATA_T get(
            @FFIConst @CXXReference @FFITypeAlias(CppClassName.PROPERTY_NBR_UNIT + "<uint64_t>")
                    PropertyNbrUnit<Long> nbr);
}

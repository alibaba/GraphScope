/*
 * Copyright 2022 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.graphscope.arrow.array;

import static com.alibaba.graphscope.utils.CppClassName.GS_ARROW_ARRAY_BUILDER;
import static com.alibaba.graphscope.utils.CppHeaderName.CORE_JAVA_TYPE_ALIAS_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.arrow.Status;
import com.alibaba.graphscope.utils.JNILibraryName;

@FFIGen(library = JNILibraryName.JNI_LIBRARY_NAME)
@CXXHead(CORE_JAVA_TYPE_ALIAS_H)
@FFITypeAlias(GS_ARROW_ARRAY_BUILDER)
public interface ArrowArrayBuilder<T> extends FFIPointer {

    @FFINameAlias("Reserve")
    @CXXValue
    Status reserve(long additionalCapacity);

    @FFINameAlias("UnsafeAppend")
    void unsafeAppend(T value);

    @FFINameAlias("GetValue")
    long getValue(long index);

    @FFIFactory
    interface Factory<T> {

        ArrowArrayBuilder<T> create();
    }
}

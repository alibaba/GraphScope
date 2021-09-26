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

import static com.alibaba.graphscope.utils.CppClassName.GRAPE_EMPTY_TYPE;
import static com.alibaba.graphscope.utils.CppHeaderName.GRAPE_TYPES_H;
import static com.alibaba.graphscope.utils.JNILibraryName.JNI_LIBRARY_NAME;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFITypeFactory;

/**
 * Empty type is an type placeholder to put in a template class or template method, it is a wrapper
 * for <a
 * href="https://github.com/alibaba/libgrape-lite/blob/master/grape/types.h">grape::EmptyType</a>.
 */
@FFIGen(library = JNI_LIBRARY_NAME)
@CXXHead(GRAPE_TYPES_H)
@FFITypeAlias(GRAPE_EMPTY_TYPE)
public interface EmptyType extends FFIPointer {
    Factory factory = FFITypeFactory.getFactory(Factory.class, EmptyType.class);

    /** Factor for EmptyType. */
    @FFIFactory
    interface Factory {

        /**
         * Create a new instance.
         *
         * @return a new instance.
         */
        EmptyType create();
    }
}

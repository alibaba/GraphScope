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
package com.alibaba.graphscope.parallel.message;

import static com.alibaba.graphscope.utils.CppClassName.GS_PRIMITIVE_MESSAGE;
import static com.alibaba.graphscope.utils.CppHeaderName.CORE_JAVA_JAVA_MESSAGES_H;
import static com.alibaba.graphscope.utils.JNILibraryName.JNI_LIBRARY_NAME;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;

@FFIGen(library = JNI_LIBRARY_NAME)
@CXXHead(CORE_JAVA_JAVA_MESSAGES_H)
@FFITypeAlias(GS_PRIMITIVE_MESSAGE)
public interface PrimitiveMessage<T> extends FFIPointer {
    T getData();

    void setData(T val);

    @FFIFactory
    interface Factory<T> {
        /**
         * Create an uninitialized DoubleMsg.
         *
         * @return msg instance.
         */
        PrimitiveMessage<T> create();

        /**
         * Create a DoubleMsg with initial value.
         *
         * @param inData element type
         * @return msg instance.
         */
        PrimitiveMessage<T> create(T inData);
    }
}

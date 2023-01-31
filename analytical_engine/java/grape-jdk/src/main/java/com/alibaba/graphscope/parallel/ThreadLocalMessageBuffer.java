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

package com.alibaba.graphscope.parallel;

import static com.alibaba.graphscope.utils.CppClassName.GRAPE_PARALLEL_MESSAGE_MANAGER;
import static com.alibaba.graphscope.utils.CppClassName.GRAPE_PARALLEL_THREAD_LOCAL_MESSAGE_BUFFER;
import static com.alibaba.graphscope.utils.CppHeaderName.GRAPE_PARALLEL_THREAD_LOCAL_MESSAGE_BUFFER_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;

@FFIGen
@FFITypeAlias(GRAPE_PARALLEL_THREAD_LOCAL_MESSAGE_BUFFER)
@CXXHead({GRAPE_PARALLEL_THREAD_LOCAL_MESSAGE_BUFFER_H, GRAPE_PARALLEL_MESSAGE_MANAGER})
public interface ThreadLocalMessageBuffer<T> extends FFIPointer {

    @FFINameAlias("SendToFragment")
    <MSG_T> void sendToFragment(int dstFid, @CXXReference MSG_T msg);
}

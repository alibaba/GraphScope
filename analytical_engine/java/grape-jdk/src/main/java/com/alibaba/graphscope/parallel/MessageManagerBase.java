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

package com.alibaba.graphscope.parallel;

import com.alibaba.fastffi.FFIPointer;

/** Base interface for all messageManagers. */
public interface MessageManagerBase extends FFIPointer {

    void Start();

    void StartARound();

    void FinishARound();

    void Finalize();

    boolean ToTerminate();

    long GetMsgSize();

    void ForceContinue();
    // void sumDouble(double msg_in, @CXXReference @FFITypeAlias("grape::DoubleMsg") DoubleMsg
    // msg_out);
}

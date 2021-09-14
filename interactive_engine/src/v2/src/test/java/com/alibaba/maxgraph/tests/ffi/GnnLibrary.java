/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.maxgraph.tests.ffi;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Pointer;

public interface GnnLibrary extends Library {
    GnnLibrary INSTANCE = Native.load("lgraph", GnnLibrary.class);

    void setPartitionGraph(Pointer partitionGraph);
    Pointer runLocalTests();
    boolean getTestResultFlag(Pointer r);
    Pointer getTestResultInfo(Pointer r);
    void freeTestResult(Pointer r);

    class TestResult implements AutoCloseable {
        private final Pointer result_handle;

        public TestResult(Pointer r) {
            result_handle = r;
        }

        @Override
        public void close() {
            GnnLibrary.INSTANCE.freeTestResult(result_handle);
        }

        public boolean getFlag() {
            return GnnLibrary.INSTANCE.getTestResultFlag(result_handle);
        }

        public String getInfo() {
            return GnnLibrary.INSTANCE.getTestResultInfo(result_handle).getString(0);
        }
    }
}

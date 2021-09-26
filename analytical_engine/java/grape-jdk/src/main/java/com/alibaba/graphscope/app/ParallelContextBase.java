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

package com.alibaba.graphscope.app;

import com.alibaba.fastffi.FFIByteString;
import com.alibaba.graphscope.context.ContextBase;
import com.alibaba.graphscope.fragment.ImmutableEdgecutFragment;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.parallel.ParallelMessageManager;
import com.alibaba.graphscope.stdcxx.StdVector;

/**
 * ParallelContextBase is the base class for user-defined contexts for parallel apps. A context
 * manages data through the whole computation. The data won't be cleared during supersteps.
 *
 * <p>Apart from data structures provided by {@link java.lang}, you can also use java wrappers for *
 * grape data structures provided {@link com.alibaba.grape.ds} and {@link com.alibaba.grape.stdcxx}.
 *
 * @param <OID_T> original id type
 * @param <VID_T> vertex id type
 * @param <VDATA_T> vertex data type
 * @param <EDATA_T> edge data type
 */
public interface ParallelContextBase<OID_T, VID_T, VDATA_T, EDATA_T>
        extends ContextBase<OID_T, VID_T, VDATA_T, EDATA_T> {
    /**
     * Called by grape framework, before any PEval. You can initiating data structures need during
     * super steps here.
     *
     * @param frag The graph fragment providing accesses to graph data.
     * @param messageManager The message manger which manages messages between fragments.
     * @param args String args from cmdline.
     * @see ImmutableEdgecutFragment
     * @see DefaultMessageManager
     * @see StdVector
     */
    void Init(
            ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T> frag,
            ParallelMessageManager messageManager,
            StdVector<FFIByteString> args);

    /**
     * Output will be executed when the computations finalizes. Data maintained in this context
     * shall be outputted here.
     *
     * @param frag The graph fragment contains the graph info.
     * @see ImmutableEdgecutFragment
     */
    void Output(ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T> frag);
}

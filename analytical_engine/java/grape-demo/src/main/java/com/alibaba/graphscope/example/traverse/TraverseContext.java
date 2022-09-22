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

package com.alibaba.graphscope.example.traverse;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.graphscope.context.ParallelContextBase;
import com.alibaba.graphscope.context.VertexDataContext;
import com.alibaba.graphscope.ds.GSVertexArray;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.parallel.ParallelMessageManager;

public class TraverseContext extends VertexDataContext<IFragment<Long, Long, Double, Long>, Long>
        implements ParallelContextBase<Long, Long, Double, Long> {

    public GSVertexArray<Long> vertexArray;
    public int maxIteration;

    @Override
    public void Init(
            IFragment<Long, Long, Double, Long> frag,
            ParallelMessageManager messageManager,
            JSONObject jsonObject) {
        createFFIContext(frag, Long.class, false);
        // This vertex Array is created by our framework. Data stored in this array will be
        // available
        // after execution, you can receive them by invoking method provided in Python Context.
        vertexArray = data();
        maxIteration = 10;
        if (jsonObject.containsKey("maxIteration")) {
            maxIteration = jsonObject.getInteger("maxIteration");
        }
    }

    @Override
    public void Output(IFragment<Long, Long, Double, Long> frag) {
        // You can also write output logic in this function.
    }
}

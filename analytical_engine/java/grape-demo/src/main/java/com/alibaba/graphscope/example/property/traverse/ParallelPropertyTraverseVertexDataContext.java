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

package com.alibaba.graphscope.example.property.traverse;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.graphscope.context.LabeledVertexDataContext;
import com.alibaba.graphscope.context.PropertyParallelContextBase;
import com.alibaba.graphscope.fragment.ArrowFragment;
import com.alibaba.graphscope.parallel.ParallelPropertyMessageManager;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParallelPropertyTraverseVertexDataContext
        extends LabeledVertexDataContext<Long, Double>
        implements PropertyParallelContextBase<Long> {
    private static Logger logger =
            LoggerFactory.getLogger(ParallelPropertyTraverseVertexDataContext.class.getName());
    public int threadNum;
    public int chunkSize;
    public int maxSteps;
    public int curSteps;
    public ExecutorService executor;
    public List<Long> neighboringVertices;

    @Override
    public void init(
            ArrowFragment<Long> fragment,
            ParallelPropertyMessageManager messageManager,
            JSONObject jsonObject) {
        createFFIContext(fragment, Long.class, Double.class);
        threadNum = 1;
        if (jsonObject.containsKey("threadNum")) {
            threadNum = jsonObject.getInteger("threadNum");
        }

        chunkSize = 1024;
        if (jsonObject.containsKey("chunkSize")) {
            chunkSize = jsonObject.getInteger("chunkSize");
        }

        maxSteps = 50;
        if (jsonObject.containsKey("maxSteps")) {
            maxSteps = jsonObject.getInteger("maxSteps");
        }

        executor = Executors.newFixedThreadPool(threadNum);
        neighboringVertices = new ArrayList<>(threadNum);
        for (int i = 0; i < threadNum; ++i) {
            neighboringVertices.add(0L);
        }
        curSteps = 0;
    }

    public void Output(ArrowFragment<Long> fragment) {
        logger.info("Output is skipped");
    }
}

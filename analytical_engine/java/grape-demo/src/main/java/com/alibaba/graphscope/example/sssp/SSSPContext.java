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

package com.alibaba.graphscope.example.sssp;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.graphscope.context.ParallelContextBase;
import com.alibaba.graphscope.context.VertexDataContext;
import com.alibaba.graphscope.ds.GSVertexArray;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexSet;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.parallel.ParallelMessageManager;
import com.alibaba.graphscope.utils.AtomicLongArrayWrapper;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SSSPContext extends VertexDataContext<IFragment<Long, Long, Long, Long>, Double>
        implements ParallelContextBase<Long, Long, Long, Long> {

    private static Logger logger = LoggerFactory.getLogger(SSSPContext.class);

    public ExecutorService executor;
    public AtomicLongArrayWrapper partialResults;
    public VertexSet curModified;
    public VertexSet nextModified;

    public double execTime = 0.0;
    public double sendMessageTime = 0.0;
    public double receiveMessageTime = 0.0;

    public Long sourceOid;
    public int threadNum;
    public int chunkSize;

    public Long getSourceOid() {
        return sourceOid;
    }

    @Override
    public void Init(
            IFragment<Long, Long, Long, Long> frag,
            ParallelMessageManager mm,
            JSONObject jsonObject) {
        createFFIContext(frag, Double.class, false);
        if (!jsonObject.containsKey("src")) {
            logger.error("No src in params");
            return;
        }
        sourceOid = jsonObject.getLong("threadNum");
        if (!jsonObject.containsKey("threadNum")) {
            logger.warn("No threadNum in params");
            threadNum = 1;
        } else {
            threadNum = jsonObject.getInteger("threadNum");
        }
        Long allVertexNum = frag.getVerticesNum();
        partialResults = new AtomicLongArrayWrapper(allVertexNum.intValue(), Long.MAX_VALUE);
        curModified = new VertexSet(0, allVertexNum.intValue());
        nextModified = new VertexSet(0, allVertexNum.intValue());

        executor = Executors.newFixedThreadPool(threadNum);
        chunkSize = 1024;
    }

    @Override
    public void Output(IFragment<Long, Long, Long, Long> frag) {
        executor.shutdown();

        logger.info("frag: " + frag.fid() + " sendMessageTime: " + sendMessageTime / 1000000000);
        logger.info(
                "frag: " + frag.fid() + " receiveMessageTime: " + receiveMessageTime / 1000000000);
        logger.info("frag: " + frag.fid() + " execTime: " + execTime / 1000000000);

        GSVertexArray<Double> vertexArray = data();
        Vertex<Long> cur = FFITypeFactoryhelper.newVertexLong();
        for (long vid = 0; vid < frag.getInnerVerticesNum(); ++vid) {
            cur.setValue(vid);
            vertexArray.setValue(cur, (double) partialResults.get(vid));
        }
    }

    public int thread_num() {
        return threadNum;
    }
}

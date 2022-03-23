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

package com.alibaba.graphscope.example.bfs;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.graphscope.context.ParallelContextBase;
import com.alibaba.graphscope.context.VertexDataContext;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexRange;
import com.alibaba.graphscope.ds.VertexSet;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.parallel.ParallelMessageManager;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import com.alibaba.graphscope.utils.IntArrayWrapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BFSContext extends VertexDataContext<IFragment<Long, Long, Double, Long>, Long>
        implements ParallelContextBase<Long, Long, Double, Long> {

    private static Logger logger = LoggerFactory.getLogger(BFSContext.class);

    public long sourceOid;
    public IntArrayWrapper partialResults;
    public VertexSet currentInnerUpdated, nextInnerUpdated;
    public int currentDepth;
    public int threadNum;
    public ExecutorService executor;

    @Override
    public void Init(
            IFragment<Long, Long, Double, Long> frag,
            ParallelMessageManager messageManager,
            JSONObject jsonObject) {
        createFFIContext(frag, Long.class, false);
        if (!jsonObject.containsKey("src")) {
            logger.error("No src arg found");
            return;
        }
        sourceOid = jsonObject.getLong("src");
        if (!jsonObject.containsKey("threadNum")) {
            logger.warn("No threadNum arg found");
            threadNum = 1;
        } else {
            threadNum = jsonObject.getInteger("threadNum");
        }
        partialResults = new IntArrayWrapper(frag.getVerticesNum().intValue(), Integer.MAX_VALUE);
        currentInnerUpdated = new VertexSet(frag.innerVertices());
        nextInnerUpdated = new VertexSet(frag.innerVertices());
        currentDepth = 0;
        executor = Executors.newFixedThreadPool(threadNum);
        messageManager.initChannels(threadNum);
    }

    @Override
    public void Output(IFragment<Long, Long, Double, Long> frag) {
        String prefix = "/tmp/bfs_parallel_output";
        logger.info("depth " + currentDepth);
        String filePath = prefix + "_frag_" + frag.fid();
        try {
            FileWriter fileWritter = new FileWriter(filePath);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWritter);
            VertexRange<Long> innerNodes = frag.innerVertices();

            Vertex<Long> cur = FFITypeFactoryhelper.newVertexLong();
            for (long index = 0; index < frag.getInnerVerticesNum(); ++index) {
                cur.SetValue(index);
                Long oid = frag.getId(cur);
                bufferedWriter.write(oid + "\t" + partialResults.get(index) + "\n");
            }
            bufferedWriter.close();
            logger.info("writing output to " + filePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

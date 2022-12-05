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

package com.alibaba.graphscope.example.wcc;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.graphscope.context.ParallelContextBase;
import com.alibaba.graphscope.context.VertexDataContext;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexRange;
import com.alibaba.graphscope.ds.VertexSet;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.parallel.ParallelMessageManager;
import com.alibaba.graphscope.utils.AtomicLongArrayWrapper;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class WCCContext extends VertexDataContext<IFragment<Long, Long, Long, Double>, Double>
        implements ParallelContextBase<Long, Long, Long, Double> {

    private static Logger logger = LoggerFactory.getLogger(WCCContext.class);

    public VertexSet currModified;
    public VertexSet nextModified;
    public AtomicLongArrayWrapper comp_id;
    public int threadNum;
    public ExecutorService executor;
    public int innerVerticesNum;

    @Override
    public void Init(
            IFragment<Long, Long, Long, Double> frag,
            ParallelMessageManager messageManager,
            JSONObject jsonObject) {
        createFFIContext(frag, Double.class, false);
        if (!jsonObject.containsKey("threadNum")) {
            threadNum = 1;
        } else {
            threadNum = jsonObject.getInteger("threadNum");
        }
        logger.info("thread num " + threadNum);
        comp_id = new AtomicLongArrayWrapper(frag.getVerticesNum().intValue(), Long.MAX_VALUE);
        currModified = new VertexSet(frag.vertices());
        nextModified = new VertexSet(frag.vertices());
        executor = Executors.newFixedThreadPool(threadNum);
        innerVerticesNum = (int) frag.getInnerVerticesNum();
    }

    @Override
    public void Output(IFragment<Long, Long, Long, Double> frag) {
        String prefix = "/tmp/wcc_parallel_output";
        String filePath = prefix + "_frag_" + frag.fid();
        try {
            FileWriter fileWritter = new FileWriter(filePath);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWritter);
            VertexRange<Long> innerNodes = frag.innerVertices();

            Vertex<Long> cur = FFITypeFactoryhelper.newVertexLong();
            for (long index = 0; index < frag.getInnerVerticesNum(); ++index) {
                cur.setValue(index);
                Long oid = frag.getId(cur);
                bufferedWriter.write(oid + "\t" + comp_id.get(index) + "\n");
            }
            bufferedWriter.close();
            logger.info("writing output to " + filePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

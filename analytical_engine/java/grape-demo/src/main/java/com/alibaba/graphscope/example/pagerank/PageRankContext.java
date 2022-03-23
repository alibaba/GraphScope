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

package com.alibaba.graphscope.example.pagerank;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.graphscope.context.ParallelContextBase;
import com.alibaba.graphscope.context.VertexDataContext;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexRange;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.parallel.MessageInBuffer;
import com.alibaba.graphscope.parallel.ParallelMessageManager;
import com.alibaba.graphscope.utils.DoubleArrayWrapper;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import com.alibaba.graphscope.utils.IntArrayWrapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class PageRankContext extends VertexDataContext<IFragment<Long, Long, Long, Double>, Double>
        implements ParallelContextBase<Long, Long, Long, Double> {

    private static Logger logger = LoggerFactory.getLogger(PageRankContext.class);

    public double alpha;
    public int maxIteration;
    public int superStep;
    public double danglingSum;

    public DoubleArrayWrapper pagerank;
    public DoubleArrayWrapper nextResult;
    public IntArrayWrapper degree;
    public int thread_num;
    public ExecutorService executor;
    public MessageInBuffer.Factory bufferFactory;
    public int chunkSize;
    public double sumDoubleTime = 0.0;
    public double swapTime = 0.0;
    public int danglingVNum;

    @Override
    public void Init(
            IFragment<Long, Long, Long, Double> frag,
            ParallelMessageManager javaParallelMessageManager,
            JSONObject jsonObject) {
        createFFIContext(frag, Double.class, false);
        if (!jsonObject.containsKey("alpha")) {
            logger.error("expect alpha in params");
            return;
        }
        alpha = jsonObject.getDouble("alpha");

        if (!jsonObject.containsKey("maxIteration")) {
            logger.error("expect alpha in params");
            return;
        }
        maxIteration = jsonObject.getInteger("maxIteration");

        if (!jsonObject.containsKey("threadNum")) {
            logger.warn("expect threadNum in params");
            thread_num = 1;
        } else {
            thread_num = jsonObject.getInteger("threadNum");
        }

        bufferFactory = FFITypeFactoryhelper.newMessageInBuffer();
        logger.info(
                "alpha: ["
                        + alpha
                        + "], max iteration: ["
                        + maxIteration
                        + "], thread num "
                        + thread_num);
        pagerank = new DoubleArrayWrapper((frag.getVerticesNum().intValue()), 0.0);
        nextResult = new DoubleArrayWrapper((int) frag.getInnerVerticesNum(), 0.0);
        degree = new IntArrayWrapper((int) frag.getInnerVerticesNum(), 0);
        executor = Executors.newFixedThreadPool(thread_num());
        chunkSize = 1024;
        danglingVNum = 0;
    }

    @Override
    public void Output(IFragment<Long, Long, Long, Double> frag) {
        String prefix = "/tmp/pagerank_parallel_output";
        logger.info("sum double " + sumDoubleTime / 10e9 + " swap time " + swapTime / 10e9);
        String filePath = prefix + "_frag_" + String.valueOf(frag.fid());
        try {
            FileWriter fileWritter = new FileWriter(new File(filePath));
            BufferedWriter bufferedWriter = new BufferedWriter(fileWritter);
            VertexRange<Long> innerNodes = frag.innerVertices();

            Vertex<Long> cur = FFITypeFactoryhelper.newVertexLong();
            for (long index = 0; index < frag.getInnerVerticesNum(); ++index) {
                cur.SetValue(index);
                Long oid = frag.getId(cur);
                bufferedWriter.write(
                        cur.GetValue() + "\t" + oid + "\t" + pagerank.get(index) + "\n");
            }
            bufferedWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int thread_num() {
        return thread_num;
    }
}

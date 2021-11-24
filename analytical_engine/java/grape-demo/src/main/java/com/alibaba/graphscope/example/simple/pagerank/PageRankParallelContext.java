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

package com.alibaba.graphscope.example.simple.pagerank;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.graphscope.app.ParallelContextBase;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexRange;
import com.alibaba.graphscope.fragment.SimpleFragment;
import com.alibaba.graphscope.parallel.MessageInBuffer;
import com.alibaba.graphscope.parallel.ParallelMessageManager;
import com.alibaba.graphscope.utils.DoubleArrayWrapper;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import com.alibaba.graphscope.utils.IntArrayWrapper;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PageRankParallelContext implements ParallelContextBase<Long, Long, Long, Double> {
    private static Logger logger = LoggerFactory.getLogger(PageRankParallelContext.class);

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
            SimpleFragment<Long, Long, Long, Double> frag,
            ParallelMessageManager javaParallelMessageManager,
            JSONObject jsonObject) {
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
            logger.error("expect threadNum in params");
            return;
        }
        maxIteration = jsonObject.getInteger("threadNum");

        bufferFactory = FFITypeFactoryhelper.newMessageInBuffer();
        System.out.println(
                "alpha: ["
                        + alpha
                        + "], max iteration: ["
                        + maxIteration
                        + "], thread num "
                        + thread_num);
        pagerank = new DoubleArrayWrapper((frag.getVerticesNum().intValue()), 0.0);
        nextResult = new DoubleArrayWrapper((frag.getInnerVerticesNum().intValue()), 0.0);
        degree = new IntArrayWrapper(frag.getInnerVerticesNum().intValue(), 0);
        executor = Executors.newFixedThreadPool(thread_num());
        chunkSize = 1024;
        danglingVNum = 0;
    }

    @Override
    public void Output(SimpleFragment<Long, Long, Long, Double> frag) {
        String prefix = "/tmp/pagerank_parallel_output";
        System.out.println("sum double " + sumDoubleTime / 10e9 + " swap time " + swapTime / 10e9);
        String filePath = prefix + "_frag_" + String.valueOf(frag.fid());
        try {
            FileWriter fileWritter = new FileWriter(new File(filePath));
            BufferedWriter bufferedWriter = new BufferedWriter(fileWritter);
            VertexRange<Long> innerNodes = frag.innerVertices();

            Vertex<Long> cur = innerNodes.begin();
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

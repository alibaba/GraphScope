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

package com.alibaba.graphscope.example.property.pagerank;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.graphscope.context.LabeledVertexDataContext;
import com.alibaba.graphscope.context.PropertyParallelContextBase;
import com.alibaba.graphscope.ds.GSVertexArray;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexRange;
import com.alibaba.graphscope.fragment.ArrowFragment;
import com.alibaba.graphscope.parallel.MessageInBuffer;
import com.alibaba.graphscope.parallel.ParallelPropertyMessageManager;
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

public class ParallelPropertyPageRankVertexDataContext
        extends LabeledVertexDataContext<Long, Double>
        implements PropertyParallelContextBase<Long> {
    private static Logger logger =
            LoggerFactory.getLogger(ParallelPropertyPageRankVertexDataContext.class.getName());
    public double delta;
    public int maxIteration;
    public int superStep;
    public double danglingSum;
    public int threadNum;
    public int chunkSize;
    public int danglingVNum;

    public GSVertexArray<Double> pagerank;
    public GSVertexArray<Double> nextResult;
    public IntArrayWrapper degree;
    public ExecutorService executor;
    public MessageInBuffer.Factory bufferFactory;

    @Override
    public void init(
            ArrowFragment<Long> fragment,
            ParallelPropertyMessageManager messageManager,
            JSONObject jsonObject) {
        createFFIContext(fragment, Long.class, Double.class);
        bufferFactory = FFITypeFactoryhelper.newMessageInBuffer();

        if (jsonObject.containsKey("delta")) {
            delta = jsonObject.getDouble("delta");
        } else {
            logger.error("delta not set in parameter.");
            return;
        }
        maxIteration = 20;
        if (jsonObject.containsKey("maxRound")) {
            maxIteration = jsonObject.getInteger("maxRound");
        } else {
            logger.info("Using default maxiteration: 20");
        }

        threadNum = 1;
        if (jsonObject.containsKey("threadNum")) {
            threadNum = jsonObject.getInteger("threadNum");
        } else {
            logger.info("Using default threadNum: 1");
        }

        chunkSize = 1024;
        if (jsonObject.containsKey("chunkSize")) {
            chunkSize = jsonObject.getInteger("chunkSize");
        } else {
            logger.info("Using default chunkSize: 1024");
        }
        logger.info(
                "alpha: ["
                        + delta
                        + "], max iteration: ["
                        + maxIteration
                        + "], thread num "
                        + threadNum);
        pagerank = data().get(0);
        pagerank.setValue(0.0);
        nextResult = FFITypeFactoryhelper.newGSVertexArray(Double.class);
        nextResult.init(fragment.vertices(0));
        nextResult.setValue(0.0);
        degree = new IntArrayWrapper((int) fragment.getInnerVerticesNum(0), 0);

        executor = Executors.newFixedThreadPool(threadNum);
        danglingVNum = 0;
        superStep = 0;

        //        receiveMsgTime = sendMsgTime = calculationTime = swapTime = 0;
    }

    public void Output(ArrowFragment<Long> frag) {
        String prefix = "/tmp/pagerank_parallel_output" + (System.currentTimeMillis() / 1000);
        String filePath = prefix + "_frag_" + String.valueOf(frag.fid());
        try {
            for (int i = 0; i < frag.vertexLabelNum(); ++i) {
                filePath += "_" + i;
                FileWriter fileWritter = new FileWriter(new File(filePath));
                BufferedWriter bufferedWriter = new BufferedWriter(fileWritter);
                VertexRange<Long> innerNodes = frag.innerVertices(i);
                Vertex<Long> cur = innerNodes.begin();
                for (long index = 0; index < frag.getInnerVerticesNum(i); ++index) {
                    cur.SetValue(index);
                    Long oid = frag.getOid(cur);
                    if (degree.get(cur) == 0) {
                        bufferedWriter.write(oid + "\t" + pagerank.get(cur) + "\n");
                    } else {
                        bufferedWriter.write(
                                oid + "\t" + degree.get(cur) * pagerank.get(cur) + "\n");
                    }
                }
                bufferedWriter.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

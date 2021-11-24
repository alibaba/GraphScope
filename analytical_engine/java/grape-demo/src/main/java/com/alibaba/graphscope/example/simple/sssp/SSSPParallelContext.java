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

package com.alibaba.graphscope.example.simple.sssp;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.graphscope.app.ParallelContextBase;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexRange;
import com.alibaba.graphscope.ds.VertexSet;
import com.alibaba.graphscope.fragment.SimpleFragment;
import com.alibaba.graphscope.parallel.ParallelMessageManager;
import com.alibaba.graphscope.utils.AtomicDoubleArrayWrapper;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SSSPParallelContext implements ParallelContextBase<Long, Long, Long, Double> {
    private static Logger logger = LoggerFactory.getLogger(SSSPDefaultContext.class);

    public ExecutorService executor;
    public AtomicDoubleArrayWrapper partialResults;
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
            SimpleFragment<Long, Long, Long, Double> frag,
            ParallelMessageManager mm,
            JSONObject jsonObject) {
        if (!jsonObject.containsKey("src")) {
            logger.error("No src in params");
            return;
        }
        sourceOid = jsonObject.getLong("threadNum");
        if (!jsonObject.containsKey("threadNum")) {
            logger.error("No threadNum in params");
            return;
        }
        threadNum = jsonObject.getInteger("threadNum");
        Long allVertexNum = frag.getVerticesNum();
        // partialResults = new AtomicDouble(allVertexNum.intValue(), Double.MAX_VALUE);
        partialResults = new AtomicDoubleArrayWrapper(allVertexNum.intValue(), Double.MAX_VALUE);
        curModified = new VertexSet(0, allVertexNum.intValue());
        nextModified = new VertexSet(0, allVertexNum.intValue());

        executor = Executors.newFixedThreadPool(threadNum);
        chunkSize = 1024;
    }

    @Override
    public void Output(SimpleFragment<Long, Long, Long, Double> frag) {
        executor.shutdown();

        System.out.println(
                "frag: " + frag.fid() + " sendMessageTime: " + sendMessageTime / 1000000000);
        System.out.println(
                "frag: " + frag.fid() + " receiveMessageTime: " + receiveMessageTime / 1000000000);
        System.out.println("frag: " + frag.fid() + " execTime: " + execTime / 1000000000);
        String prefix = "/tmp/sssp_parallel_output_threadNum_" + threadNum + "_";
        String filePath = prefix + "_frag_" + String.valueOf(frag.fid());
        try {
            FileWriter fileWritter = new FileWriter(new File(filePath));
            BufferedWriter bufferedWriter = new BufferedWriter(fileWritter);
            VertexRange<Long> innerNodes = frag.innerVertices();

            // ArrayListWrapper<Long> partialResults = this.getPartialResults();
            System.out.println(
                    frag.getInnerVerticesNum()
                            + " "
                            + innerNodes.begin().GetValue()
                            + " "
                            + innerNodes.end().GetValue());
            // for (Vertex<Long> cur = innerNodes.begin(); cur.GetValue() !=
            // innerNodes.end().GetValue();
            // cur.inc()) {
            Vertex<Long> cur = innerNodes.begin();
            for (long index = 0; index < frag.getInnerVerticesNum(); ++index) {
                cur.SetValue(index);
                Long oid = frag.getId(cur);
                bufferedWriter.write(
                        cur.GetValue() + "\t" + oid + "\t" + partialResults.get(index) + "\n");
            }
            bufferedWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int thread_num() {
        return threadNum;
    }
}

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

package com.alibaba.graphscope.example.property.bfs;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.graphscope.context.LabeledVertexDataContext;
import com.alibaba.graphscope.context.PropertyParallelContextBase;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexRange;
import com.alibaba.graphscope.ds.VertexSet;
import com.alibaba.graphscope.fragment.ArrowFragment;
import com.alibaba.graphscope.parallel.ParallelPropertyMessageManager;
import com.alibaba.graphscope.utils.AtomicLongArrayWrapper;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParallelPropertyBfsVertexDataContext extends LabeledVertexDataContext<Long, Double>
        implements PropertyParallelContextBase<Long> {
    private static Logger logger =
            LoggerFactory.getLogger(ParallelPropertyBfsVertexDataContext.class.getName());
    public int threadNum;
    public int chunkSize;
    public long sourceOid;
    public long curDepth;
    public ExecutorService executor;
    public VertexSet curModified;
    public VertexSet nextModified;
    public AtomicLongArrayWrapper depth;

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

        sourceOid = 6;
        if (jsonObject.containsKey("src")) {
            sourceOid = jsonObject.getLong("src");
        }

        VertexRange<Long> innerVertices = fragment.innerVertices(0);
        depth = new AtomicLongArrayWrapper(fragment.getTotalVerticesNum(0), Long.MAX_VALUE);
        curModified = new VertexSet(innerVertices);
        nextModified = new VertexSet(innerVertices);
        executor = Executors.newFixedThreadPool(threadNum);
        curDepth = 0;
    }

    public void Output(ArrowFragment<Long> fragment) {
        String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date());
        String prefix = "/tmp/bfs_parallel_output_" + timeStamp;
        String filePath = prefix + "_frag_" + String.valueOf(fragment.fid());
        try {
            for (int i = 0; i < fragment.vertexLabelNum(); ++i) {
                filePath += "_" + i;
                FileWriter fileWritter = new FileWriter(new File(filePath));
                BufferedWriter bufferedWriter = new BufferedWriter(fileWritter);
                VertexRange<Long> innerNodes = fragment.innerVertices(i);
                Vertex<Long> cur = innerNodes.begin();
                for (long index = 0; index < fragment.getInnerVerticesNum(i); ++index) {
                    cur.SetValue(index);
                    Long oid = fragment.getOid(cur);
                    bufferedWriter.write(oid + "\t" + depth.get(index) + "\n");
                }
                bufferedWriter.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

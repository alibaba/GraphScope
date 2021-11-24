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

package com.alibaba.graphscope.example.simple.traverse;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.graphscope.app.ParallelContextBase;
import com.alibaba.graphscope.fragment.SimpleFragment;
import com.alibaba.graphscope.parallel.ParallelMessageManager;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TraverseParallelContext implements ParallelContextBase<Long, Long, Long, Double> {

    private static Logger logger = LoggerFactory.getLogger(TraverseParallelContext.class);

    public int step;
    public int maxStep;
    public long fake_vid;
    public double fake_edata;
    public ExecutorService executor;
    public int threadNum;
    public long chunkSize;

    @Override
    public void Init(
            SimpleFragment<Long, Long, Long, Double> immutableEdgecutFragment,
            ParallelMessageManager messageManager,
            JSONObject jsonObject) {
        if (!jsonObject.containsKey("maxStep")) {
            logger.error("no maxStep in params");
            return;
        }
        maxStep = jsonObject.getInteger("maxStep");

        if (!jsonObject.containsKey("threadNum")) {
            logger.error("no threadNum in params");
            return;
        }
        threadNum = jsonObject.getInteger("threadNum");
        executor = Executors.newFixedThreadPool(threadNum);
        long innerVerticesNum = immutableEdgecutFragment.getInnerVerticesNum();
        // chunkSize = (innerVerticesNum + threadNum - 1) / threadNum;
        chunkSize = 1024;
        step = 0;
    }

    @Override
    public void Output(SimpleFragment<Long, Long, Long, Double> edgecutFragment) {}
}

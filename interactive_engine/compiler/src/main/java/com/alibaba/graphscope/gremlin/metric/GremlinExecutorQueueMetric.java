/*
 *
 *  * Copyright 2020 Alibaba Group Holding Limited.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.alibaba.graphscope.gremlin.metric;

import com.alibaba.graphscope.common.metric.Metric;
import com.alibaba.graphscope.gremlin.Utils;

import org.apache.tinkerpop.gremlin.server.GremlinServer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

public class GremlinExecutorQueueMetric implements Metric<Integer> {
    private final ExecutorService executorService;

    public GremlinExecutorQueueMetric(GremlinServer server) {
        this.executorService =
                Utils.getFieldValue(GremlinServer.class, server, "gremlinExecutorService");
    }

    @Override
    public Key getKey() {
        return KeyFactory.GREMLIN_EXECUTOR_QUEUE;
    }

    @Override
    public Integer getValue() {
        return (executorService instanceof ThreadPoolExecutor)
                ? ((ThreadPoolExecutor) executorService).getQueue().size()
                : ValueFactory.INVALID_INT;
    }
}

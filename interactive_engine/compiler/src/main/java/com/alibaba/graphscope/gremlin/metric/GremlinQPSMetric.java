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

import com.alibaba.graphscope.common.manager.RateLimitExecutor;
import com.alibaba.graphscope.common.metric.Metric;
import com.alibaba.graphscope.gremlin.Utils;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.tinkerpop.gremlin.server.GremlinServer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class GremlinQPSMetric implements Metric<Long> {
    private final ExecutorService executorService;

    public GremlinQPSMetric(GremlinServer server) {
        this.executorService =
                Utils.getFieldValue(GremlinServer.class, server, "gremlinExecutorService");
    }

    @Override
    public Key getKey() {
        return KeyFactory.GREMLIN_QPS;
    }

    @Override
    public Long getValue() {
        try {
            if (executorService instanceof RateLimitExecutor) {
                long startCounter = ((RateLimitExecutor) executorService).getQueryCounter();
                StopWatch watch = StopWatch.createStarted();
                Thread.sleep(2000);
                long endCounter = ((RateLimitExecutor) executorService).getQueryCounter();
                long elapsed = watch.getTime(TimeUnit.MILLISECONDS);
                // the counter may be reset to 0, so we need to handle this case
                startCounter = (endCounter >= startCounter) ? startCounter : 0;
                return (endCounter - startCounter) * 1000 / elapsed;
            }
            return ValueFactory.INVALID_LONG;
        } catch (InterruptedException t) {
            throw new RuntimeException(t);
        }
    }
}

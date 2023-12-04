/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.common.manager;

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.FrontendConfig;
import com.google.common.util.concurrent.RateLimiter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public class RateLimitExecutor extends ThreadPoolExecutor {
    private static final Logger logger = LoggerFactory.getLogger(RateLimitExecutor.class);
    private final RateLimiter rateLimiter;

    public RateLimitExecutor(
            Configs configs,
            int corePoolSize,
            int maximumPoolSize,
            long keepAliveTime,
            TimeUnit unit,
            BlockingQueue<Runnable> workQueue,
            ThreadFactory threadFactory,
            RejectedExecutionHandler handler) {
        super(
                corePoolSize,
                maximumPoolSize,
                keepAliveTime,
                unit,
                workQueue,
                threadFactory,
                handler);
        int permitsPerSecond = FrontendConfig.QUERY_PER_SECOND_LIMIT.get(configs);
        this.rateLimiter = RateLimiter.create(permitsPerSecond);
    }

    public Future<?> submit(Runnable task) {
        if (rateLimiter.tryAcquire()) {
            return super.submit(task);
        }
        throw new RejectedExecutionException(
                "rate limit exceeded, current limit is "
                        + rateLimiter.getRate()
                        + " per second. Please increase the QPS limit by the config"
                        + " 'query.per.second.limit' or slow down the query sending speed");
    }
}

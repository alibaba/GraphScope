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

package com.alibaba.graphscope.gremlin.service;

import com.alibaba.graphscope.gremlin.Utils;
import com.codahale.metrics.Timer;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

// collect metrics per gremlin query and print to logs
public class MetricsPrinter {
    private static final Logger logger = LoggerFactory.getLogger(MetricsPrinter.class);
    private final long queryId;
    private final String query;
    private final Timer.Context timeContext;
    private final @Nullable Logger extraLogger;

    public MetricsPrinter(long queryId, String query, Timer timer, @Nullable Logger extraLogger) {
        this.queryId = queryId;
        this.query = query;
        this.timeContext = timer.time();
        this.extraLogger = extraLogger;
    }

    public void stop(boolean isSucceed) {
        long startMillis =
                TimeUnit.NANOSECONDS.toMillis(
                        Utils.getFieldValue(Timer.Context.class, timeContext, "startTime"));
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(timeContext.stop());
        logger.info("query \"{}\" total execution time is {} ms", query, elapsedMillis);
        if (extraLogger != null) {
            extraLogger.info(
                    "{} | {} | {} | {} | {}",
                    queryId,
                    query,
                    isSucceed,
                    elapsedMillis,
                    startMillis);
        }
    }
}

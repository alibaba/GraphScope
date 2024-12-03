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

package com.alibaba.graphscope.common.metric;

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.FrontendConfig;
import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class MetricsTool {
    private static Logger logger = LoggerFactory.getLogger("PerfMetricLog");
    private final List<Metric> metrics;
    private final ScheduledExecutorService service;
    private final long intervalMS;

    public MetricsTool(Configs configs) {
        this.metrics = Lists.newArrayList();
        this.service = new ScheduledThreadPoolExecutor(1);
        this.intervalMS = FrontendConfig.METRICS_TOOL_INTERVAL_MS.get(configs);
        if (this.intervalMS > 0) {
            this.service.scheduleAtFixedRate(
                    () -> printMetrics(), intervalMS, intervalMS, TimeUnit.MILLISECONDS);
        }
    }

    public MetricsTool registerMetric(Metric metric) {
        if (metrics.stream().anyMatch(k -> k.getKey().equals(metric.getKey()))) {
            logger.warn("metric {} already exists", metric.getKey());
            return this;
        }
        metrics.add(metric);
        return this;
    }

    private void printMetrics() {
        try {
            StringBuilder builder = new StringBuilder();
            metrics.forEach(
                    k -> {
                        builder.append(k.getKey()).append(":").append(k.getValue()).append("\n");
                    });
            logger.info("print perf metrics per {} ms:\n{} \n\n", intervalMS, builder);
        } catch (Throwable t) {
            logger.error("print perf metrics failed", t);
        }
    }
}

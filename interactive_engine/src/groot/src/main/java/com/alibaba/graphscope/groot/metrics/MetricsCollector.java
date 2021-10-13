/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.groot.metrics;

import com.alibaba.maxgraph.common.config.CommonConfig;
import com.alibaba.maxgraph.common.config.Configs;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MetricsCollector {

    private Set<String> registeredMetricKeys = new HashSet<>();
    private List<MetricsAgent> metricsAgents = new CopyOnWriteArrayList<>();
    private ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    private long updateIntervalMs;

    public MetricsCollector(Configs configs) {
        this.updateIntervalMs = CommonConfig.METRIC_UPDATE_INTERVAL_MS.get(configs);
    }

    public void register(MetricsAgent metricsAgent, Runnable... updateRunnable) {
        String[] metricKeys = metricsAgent.getMetricKeys();
        for (String metricKey : metricKeys) {
            if (!this.registeredMetricKeys.add(metricKey)) {
                throw new IllegalArgumentException("duplicate metricKey [" + metricKey + "]");
            }
        }
        this.metricsAgents.add(metricsAgent);
        for (Runnable runnable : updateRunnable) {
            scheduler.scheduleWithFixedDelay(runnable, updateIntervalMs, updateIntervalMs, TimeUnit.MILLISECONDS);
        }
    }

    public Map<String, String> collectMetrics() {
        Map<String, String> metrics = new HashMap<>();
        for (MetricsAgent metricsAgent : this.metricsAgents) {
            metrics.putAll(metricsAgent.getMetrics());
        }
        return metrics;
    }

}

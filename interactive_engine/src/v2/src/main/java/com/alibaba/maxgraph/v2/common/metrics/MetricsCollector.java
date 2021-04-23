package com.alibaba.maxgraph.v2.common.metrics;

import com.alibaba.maxgraph.v2.common.config.CommonConfig;
import com.alibaba.maxgraph.v2.common.config.Configs;

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

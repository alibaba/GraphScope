package com.alibaba.maxgraph.v2.common.metrics;

import java.util.Map;

public interface MetricsAgent {

    void initMetrics();

    Map<String, String> getMetrics();

    String[] getMetricKeys();
}

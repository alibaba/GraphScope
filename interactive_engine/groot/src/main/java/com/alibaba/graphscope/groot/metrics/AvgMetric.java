package com.alibaba.graphscope.groot.metrics;

import java.util.concurrent.atomic.AtomicLong;

public class AvgMetric {
    private AtomicLong totalVal;
    private volatile long lastUpdateVal;
    private volatile double metricVal;

    public AvgMetric() {
        this.totalVal = new AtomicLong(0L);
        this.lastUpdateVal = 0L;
        this.metricVal = 0L;
    }

    public void add(long val) {
        this.totalVal.addAndGet(val);
    }

    public void update(long intervalNano) {
        long tmp = this.totalVal.get();
        this.metricVal = 1.0 * (tmp - this.lastUpdateVal) / intervalNano;
        this.lastUpdateVal = tmp;
    }

    public double getAvg() {
        return this.metricVal;
    }

    public long getLastUpdateTotal() {
        return this.lastUpdateVal;
    }
}

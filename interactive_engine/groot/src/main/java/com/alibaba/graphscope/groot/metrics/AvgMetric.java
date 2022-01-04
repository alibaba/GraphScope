package com.alibaba.graphscope.groot.metrics;

public class AvgMetric {
    private volatile long totalVal;
    private volatile long lastUpdateVal;
    private volatile long metricVal;

    public AvgMetric() {
        this.totalVal = 0L;
        this.lastUpdateVal = 0L;
        this.metricVal = 0L;
    }

    public void add(long val) {
        this.totalVal += val;
    }

    public void update(long intervalNano) {
        long tmp = this.totalVal;
        this.metricVal = (tmp - this.lastUpdateVal) / intervalNano;
        this.lastUpdateVal = tmp;
    }

    public long get() {
        return this.metricVal;
    }
}

package com.alibaba.maxgraph.v2.frontend.compiler.optimizer;

import java.util.List;

public class DataStatistics {
    private static final long DEFAULT_VERTEX_VALUE_SIZE = 12;
    private static final long DEFAULT_EDGE_VALUE_SIZE = 12;

    private static final double DEFAULT_OUT_NUM_FACTOR = 10;
    private static final double DEFAULT_IN_NUM_FACTOR = 10;
    private static final double DEFAULT_UNFOLD_NUM_FACTOR = 10;

    public long getLabelVertexCount(List<String> vertexLabelList) {
        return 0;
    }

    public long getLabelEdgeCount(List<String> collect) {
        return 0;
    }

    public double getOutNumFactor() {
        return DEFAULT_OUT_NUM_FACTOR;
    }

    public double getInNumFactor() {
        return DEFAULT_IN_NUM_FACTOR;
    }

    public double getFilterFactor() {
        return 0.5;
    }

    public double getDedupFactor() {
        return 0.8;
    }

    public double getUnfoldFactor() {
        return DEFAULT_UNFOLD_NUM_FACTOR;
    }

    public long getVertexValueSize() {
        return DEFAULT_VERTEX_VALUE_SIZE;
    }

    public long getLongValueSize() {
        return 8;
    }

    public long getEdgeValueSize() {
        return DEFAULT_EDGE_VALUE_SIZE;
    }
}

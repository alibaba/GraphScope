package com.alibaba.maxgraph.v2.frontend.graph;

public class CancelDataflow {
    private String queryId;

    public CancelDataflow(String queryId) {
        this.queryId = queryId;
    }

    public String getQueryId() {
        return this.queryId;
    }
}

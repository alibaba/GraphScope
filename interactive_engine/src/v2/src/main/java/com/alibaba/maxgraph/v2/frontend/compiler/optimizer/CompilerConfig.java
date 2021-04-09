package com.alibaba.maxgraph.v2.frontend.compiler.optimizer;

public class CompilerConfig {
    private String odpsEndpoint;
    private String odpsAccessId;
    private String odpsAccessKey;

    public String getOdpsEndpoint() {
        return odpsEndpoint;
    }

    public void setOdpsEndpoint(String odpsEndpoint) {
        this.odpsEndpoint = odpsEndpoint;
    }

    public String getOdpsAccessId() {
        return odpsAccessId;
    }

    public void setOdpsAccessId(String odpsAccessId) {
        this.odpsAccessId = odpsAccessId;
    }

    public String getOdpsAccessKey() {
        return odpsAccessKey;
    }

    public void setOdpsAccessKey(String odpsAccessKey) {
        this.odpsAccessKey = odpsAccessKey;
    }
}

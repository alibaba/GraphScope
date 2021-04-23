package com.alibaba.maxgraph.v2.common;

import com.alibaba.maxgraph.v2.common.schema.GraphDef;

public class SnapshotWithSchema {

    private long snapshotId;
    private GraphDef graphDef;

    public SnapshotWithSchema(long snapshotId, GraphDef graphDef) {
        this.snapshotId = snapshotId;
        this.graphDef = graphDef;
    }

    public long getSnapshotId() {
        return snapshotId;
    }

    public GraphDef getGraphDef() {
        return graphDef;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilder(SnapshotWithSchema snapshotWithSchema) {
        return new Builder(snapshotWithSchema);
    }

    public static class Builder {

        private long snapshotId;
        private GraphDef graphDef;

        public Builder() {
            this.snapshotId = -1L;
        }

        public Builder(SnapshotWithSchema snapshotWithSchema) {
            this.snapshotId = snapshotWithSchema.getSnapshotId();
            this.graphDef = snapshotWithSchema.getGraphDef();
        }

        public Builder setSnapshotId(long snapshotId) {
            this.snapshotId = snapshotId;
            return this;
        }

        public Builder setGraphDef(GraphDef graphDef) {
            this.graphDef = graphDef;
            return this;
        }

        public SnapshotWithSchema build() {
            return new SnapshotWithSchema(snapshotId, graphDef);
        }
    }
}

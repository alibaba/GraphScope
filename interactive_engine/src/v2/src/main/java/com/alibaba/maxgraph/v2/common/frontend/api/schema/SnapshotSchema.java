package com.alibaba.maxgraph.v2.common.frontend.api.schema;

import com.alibaba.maxgraph.v2.frontend.graph.memory.schema.DefaultGraphSchema;

/**
 * The snapshot and schema
 */
public class SnapshotSchema {
    private long snapshotId;
    private GraphSchema schema;

    public SnapshotSchema(long snapshotId, GraphSchema schema) {
        this.snapshotId = snapshotId;
        this.schema = schema;
    }

    public long getSnapshotId() {
        return snapshotId;
    }

    public GraphSchema getSchema() {
        if (null == schema) {
            return new DefaultGraphSchema();
        }
        return schema;
    }
}

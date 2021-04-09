package com.alibaba.maxgraph.v2.common.frontend.api.schema;

public interface SchemaFetcher {
    /**
     * Get schema and snapshot id
     *
     * @return The schema and snapshot id
     */
    SnapshotSchema fetchSchema();
}

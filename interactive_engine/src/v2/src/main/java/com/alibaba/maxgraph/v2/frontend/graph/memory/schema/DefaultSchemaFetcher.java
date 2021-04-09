package com.alibaba.maxgraph.v2.frontend.graph.memory.schema;

import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.SchemaFetcher;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.SnapshotSchema;

/**
 * Default schema fetcher for testing
 */
public class DefaultSchemaFetcher implements SchemaFetcher {
    private static final long SNAPSHOT = 1;
    private GraphSchema schema;

    public DefaultSchemaFetcher(GraphSchema schema) {
        this.schema = schema;
    }


    @Override
    public SnapshotSchema fetchSchema() {
        return new SnapshotSchema(SNAPSHOT, schema);
    }
}

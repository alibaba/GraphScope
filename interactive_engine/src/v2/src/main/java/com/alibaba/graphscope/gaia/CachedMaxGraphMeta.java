package com.alibaba.graphscope.gaia;

import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.api.schema.SchemaFetcher;
import org.apache.commons.lang3.tuple.Pair;

public class CachedMaxGraphMeta {
    private SchemaFetcher schemaFetcher;
    private GraphSchema graphSchema;
    private Long snapshotId;

    public CachedMaxGraphMeta(SchemaFetcher schemaFetcher) {
        this.schemaFetcher = schemaFetcher;
    }

    public synchronized void update() {
        Pair<GraphSchema, Long> pair = this.schemaFetcher.getSchemaSnapshotPair();
        if (pair != null) {
            this.graphSchema = pair.getLeft();
            this.snapshotId = pair.getRight();
        }
    }

    public GraphSchema getGraphSchema() {
        return graphSchema;
    }

    public Long getSnapshotId() {
        return snapshotId;
    }
}

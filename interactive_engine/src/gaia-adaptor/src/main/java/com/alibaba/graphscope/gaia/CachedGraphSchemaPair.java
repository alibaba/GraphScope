package com.alibaba.graphscope.gaia;

import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.api.schema.SchemaFetcher;
import org.apache.commons.lang3.tuple.Pair;

public class CachedGraphSchemaPair {
    private SchemaFetcher schemaFetcher;
    private Pair<GraphSchema, Long> cachedSchemaPair;

    public CachedGraphSchemaPair(SchemaFetcher schemaFetcher) {
        this.schemaFetcher = schemaFetcher;
        update();
    }

    public synchronized void update() {
        this.cachedSchemaPair = schemaFetcher.getSchemaSnapshotPair();
    }

    public Pair<GraphSchema, Long> get() {
        return this.cachedSchemaPair;
    }
}

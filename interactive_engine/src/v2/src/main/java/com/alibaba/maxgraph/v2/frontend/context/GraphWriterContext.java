package com.alibaba.maxgraph.v2.frontend.context;

import com.alibaba.maxgraph.v2.common.schema.ddl.DdlExecutors;
import com.alibaba.maxgraph.v2.frontend.RealtimeWriter;
import com.alibaba.maxgraph.v2.frontend.SchemaWriter;
import com.alibaba.maxgraph.v2.frontend.SnapshotCache;

public class GraphWriterContext {
    private RealtimeWriter realtimeWriter;
    private SchemaWriter schemaWriter;
    private DdlExecutors ddlExecutors;
    private boolean autoCommit;
    private SnapshotCache snapshotCache;

    public GraphWriterContext(RealtimeWriter realtimeWriter, SchemaWriter schemaWriter, DdlExecutors ddlExecutors, SnapshotCache snapshotCache, boolean autoCommit) {
        this.realtimeWriter = realtimeWriter;
        this.schemaWriter = schemaWriter;
        this.ddlExecutors = ddlExecutors;
        this.snapshotCache = snapshotCache;
        this.autoCommit = autoCommit;
    }

    public RealtimeWriter getRealtimeWriter() {
        return realtimeWriter;
    }

    public SchemaWriter getSchemaWriter() {
        return schemaWriter;
    }

    public DdlExecutors getDdlExecutors() {
        return ddlExecutors;
    }

    public boolean isAutoCommit() {
        return autoCommit;
    }

    public SnapshotCache getSchemaCache() {
        return this.snapshotCache;
    }
}

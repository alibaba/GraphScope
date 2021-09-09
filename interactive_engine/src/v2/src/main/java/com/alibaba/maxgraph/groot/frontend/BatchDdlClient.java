package com.alibaba.maxgraph.v2.frontend;

import com.alibaba.maxgraph.v2.common.schema.GraphDef;
import com.alibaba.maxgraph.v2.common.schema.ddl.DdlExecutors;
import com.alibaba.maxgraph.v2.common.schema.request.DdlException;
import com.alibaba.maxgraph.v2.common.schema.request.DdlRequestBatch;
import com.alibaba.maxgraph.v2.common.util.UuidUtils;
import com.google.protobuf.InvalidProtocolBufferException;

public class BatchDdlClient {

    private DdlExecutors ddlExecutors;
    private SnapshotCache snapshotCache;
    private SchemaWriter schemaWriter;

    public BatchDdlClient(DdlExecutors ddlExecutors, SnapshotCache snapshotCache, SchemaWriter schemaWriter) {
        this.ddlExecutors = ddlExecutors;
        this.snapshotCache = snapshotCache;
        this.schemaWriter = schemaWriter;
    }

    public long batchDdl(DdlRequestBatch ddlRequestBatch) {
        GraphDef graphDef = this.snapshotCache.getSnapshotWithSchema().getGraphDef();
        try {
            this.ddlExecutors.executeDdlRequestBatch(ddlRequestBatch, graphDef, 0);
        } catch (InvalidProtocolBufferException e) {
            throw new DdlException(e);
        }
        return this.schemaWriter.submitBatchDdl(UuidUtils.getBase64UUIDString(), "ddl", ddlRequestBatch);
    }
}

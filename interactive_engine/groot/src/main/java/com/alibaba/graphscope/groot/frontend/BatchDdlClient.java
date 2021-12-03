package com.alibaba.graphscope.groot.frontend;

import com.alibaba.graphscope.groot.SnapshotCache;
import com.alibaba.maxgraph.common.util.UuidUtils;
import com.alibaba.graphscope.groot.schema.GraphDef;
import com.alibaba.graphscope.groot.schema.ddl.DdlExecutors;
import com.alibaba.graphscope.groot.schema.request.DdlException;
import com.alibaba.graphscope.groot.schema.request.DdlRequestBatch;
import com.google.protobuf.InvalidProtocolBufferException;

public class BatchDdlClient {

    private DdlExecutors ddlExecutors;
    private SnapshotCache snapshotCache;
    private SchemaWriter schemaWriter;

    public BatchDdlClient(
            DdlExecutors ddlExecutors, SnapshotCache snapshotCache, SchemaWriter schemaWriter) {
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
        return this.schemaWriter.submitBatchDdl(
                UuidUtils.getBase64UUIDString(), "ddl", ddlRequestBatch);
    }
}

package com.alibaba.graphscope.groot.frontend;

import com.alibaba.graphscope.groot.common.exception.DdlException;
import com.alibaba.graphscope.groot.common.schema.wrapper.GraphDef;
import com.alibaba.graphscope.groot.common.util.UuidUtils;
import com.alibaba.graphscope.groot.rpc.RoleClients;
import com.alibaba.graphscope.groot.schema.ddl.DdlExecutors;
import com.alibaba.graphscope.groot.schema.request.DdlRequestBatch;
import com.google.protobuf.InvalidProtocolBufferException;

public class BatchDdlClient {

    private final DdlExecutors ddlExecutors;
    private final SnapshotCache snapshotCache;
    private final RoleClients<SchemaClient> schemaWriter;

    public BatchDdlClient(
            DdlExecutors ddlExecutors,
            SnapshotCache snapshotCache,
            RoleClients<SchemaClient> schemaWriter) {
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
        return this.schemaWriter
                .getClient(0)
                .submitBatchDdl(UuidUtils.getBase64UUIDString(), "ddl", ddlRequestBatch.toProto());
    }
}

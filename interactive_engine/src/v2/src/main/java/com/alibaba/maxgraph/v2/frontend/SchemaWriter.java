package com.alibaba.maxgraph.v2.frontend;

import com.alibaba.maxgraph.v2.common.rpc.RoleClients;
import com.alibaba.maxgraph.v2.common.schema.request.DdlRequestBatch;

public class SchemaWriter {

    private RoleClients<SchemaClient> schemaClients;

    public SchemaWriter(RoleClients<SchemaClient> schemaClients) {
        this.schemaClients = schemaClients;
    }

    public long submitBatchDdl(String requestId, String sessionId, DdlRequestBatch ddlRequestBatch) {
        return this.schemaClients.getClient(0).submitBatchDdl(requestId, sessionId, ddlRequestBatch);
    }
}

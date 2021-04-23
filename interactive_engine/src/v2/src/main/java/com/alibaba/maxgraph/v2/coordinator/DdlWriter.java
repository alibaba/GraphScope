package com.alibaba.maxgraph.v2.coordinator;

import com.alibaba.maxgraph.v2.common.BatchId;
import com.alibaba.maxgraph.v2.common.OperationBatch;
import com.alibaba.maxgraph.v2.common.rpc.RoleClients;
import com.alibaba.maxgraph.v2.frontend.IngestorWriteClient;

public class DdlWriter {

    private RoleClients<IngestorWriteClient> ingestorWriteClients;
    private int queueId = 0;

    public DdlWriter(RoleClients<IngestorWriteClient> ingestorWriteClients) {
        this.ingestorWriteClients = ingestorWriteClients;
    }

    public BatchId writeOperations(String requestId, OperationBatch operationBatch) {
        return this.ingestorWriteClients.getClient(queueId).writeIngestor(requestId, queueId, operationBatch);
    }
}

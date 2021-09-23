/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.groot.coordinator;

import com.alibaba.maxgraph.groot.common.BatchId;
import com.alibaba.maxgraph.groot.common.OperationBatch;
import com.alibaba.maxgraph.groot.common.rpc.RoleClients;
import com.alibaba.maxgraph.groot.frontend.IngestorWriteClient;

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

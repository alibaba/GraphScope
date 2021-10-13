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
package com.alibaba.maxgraph.groot.frontend;

import com.alibaba.maxgraph.proto.v2.DdlRequestBatchPb;
import com.alibaba.maxgraph.groot.common.rpc.RoleClients;
import com.alibaba.maxgraph.groot.common.schema.request.DdlRequestBatch;

public class SchemaWriter {

    private RoleClients<SchemaClient> schemaClients;

    public SchemaWriter(RoleClients<SchemaClient> schemaClients) {
        this.schemaClients = schemaClients;
    }

    public long submitBatchDdl(String requestId, String sessionId, DdlRequestBatch ddlRequestBatch) {
        return this.submitBatchDdl(requestId, sessionId, ddlRequestBatch.toProto());
    }

    public long submitBatchDdl(String requestId, String sessionId, DdlRequestBatchPb ddlRequestBatchPb) {
        return this.schemaClients.getClient(0).submitBatchDdl(requestId, sessionId, ddlRequestBatchPb);
    }
}

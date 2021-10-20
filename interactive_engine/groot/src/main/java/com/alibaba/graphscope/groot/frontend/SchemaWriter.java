/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.groot.frontend;

import com.alibaba.maxgraph.proto.groot.DdlRequestBatchPb;
import com.alibaba.graphscope.groot.rpc.RoleClients;
import com.alibaba.graphscope.groot.schema.request.DdlRequestBatch;

public class SchemaWriter {

    private RoleClients<SchemaClient> schemaClients;

    public SchemaWriter(RoleClients<SchemaClient> schemaClients) {
        this.schemaClients = schemaClients;
    }

    public long submitBatchDdl(
            String requestId, String sessionId, DdlRequestBatch ddlRequestBatch) {
        return this.submitBatchDdl(requestId, sessionId, ddlRequestBatch.toProto());
    }

    public long submitBatchDdl(
            String requestId, String sessionId, DdlRequestBatchPb ddlRequestBatchPb) {
        return this.schemaClients
                .getClient(0)
                .submitBatchDdl(requestId, sessionId, ddlRequestBatchPb);
    }
}

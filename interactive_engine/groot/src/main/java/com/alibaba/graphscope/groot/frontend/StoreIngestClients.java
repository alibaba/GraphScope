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

import com.alibaba.graphscope.groot.CompletionCallback;
import com.alibaba.maxgraph.common.RoleType;
import com.alibaba.graphscope.groot.rpc.ChannelManager;
import com.alibaba.graphscope.groot.rpc.RoleClients;
import io.grpc.ManagedChannel;

import java.util.function.Function;

public class StoreIngestClients extends RoleClients<StoreIngestClient> implements StoreIngestor {

    public StoreIngestClients(
            ChannelManager channelManager,
            RoleType targetRole,
            Function<ManagedChannel, StoreIngestClient> clientBuilder) {
        super(channelManager, targetRole, clientBuilder);
    }

    @Override
    public void ingest(int storeId, String path, CompletionCallback<Void> callback) {
        this.getClient(storeId).storeIngest(path, callback);
    }
}

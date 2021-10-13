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
package com.alibaba.maxgraph.groot.ingestor;

import com.alibaba.maxgraph.groot.common.CompletionCallback;
import com.alibaba.maxgraph.groot.common.StoreDataBatch;
import com.alibaba.maxgraph.common.RoleType;
import com.alibaba.maxgraph.groot.common.rpc.ChannelManager;
import com.alibaba.maxgraph.groot.common.rpc.RoleClients;
import io.grpc.ManagedChannel;

import java.util.function.Function;

public class StoreWriteClients extends RoleClients<StoreWriteClient> implements StoreWriter {

    public StoreWriteClients(ChannelManager channelManager, RoleType targetRole,
                             Function<ManagedChannel, StoreWriteClient> clientBuilder) {
        super(channelManager, targetRole, clientBuilder);
    }

    @Override
    public void write(int storeId, StoreDataBatch storeDataBatch, CompletionCallback<Integer> callback) {
        this.getClient(storeId).writeStore(storeDataBatch, callback);
    }
}

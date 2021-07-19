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
package com.alibaba.maxgraph.v2.ingestor;

import com.alibaba.maxgraph.v2.common.discovery.RoleType;
import com.alibaba.maxgraph.v2.common.rpc.ChannelManager;
import com.alibaba.maxgraph.v2.common.rpc.RoleClients;
import io.grpc.ManagedChannel;

import java.util.List;
import java.util.function.Function;

public class RemoteIngestProgressFetcher extends RoleClients<IngestProgressClient> implements IngestProgressFetcher {

    public RemoteIngestProgressFetcher(ChannelManager channelManager) {
        super(channelManager, RoleType.COORDINATOR, IngestProgressClient::new);
    }

    @Override
    public List<Long> getTailOffsets(List<Integer> queueIds) {
        return getClient(0).getTailOffsets(queueIds);
    }
}

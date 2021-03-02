/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.compiler.demo.server.broadcast;

import com.alibaba.pegasus.RpcClient;
import com.alibaba.pegasus.service.proto.PegasusClient;
import com.compiler.demo.server.result.GremlinResultProcessor;
import org.apache.tinkerpop.gremlin.server.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class RpcBroadcastProcessor extends BroadcastProcessor {
    private static final Logger logger = LoggerFactory.getLogger(RpcBroadcastProcessor.class);
    private List<RpcClient> rpcClients;

    public RpcBroadcastProcessor(String hostFile) {
        super(hostFile);
        rpcClients = new ArrayList<>();
        hostAddresses.forEach(k -> {
            String host = k.getLeft();
            int port = k.getRight();
            rpcClients.add(new RpcClient(host, port));
        });
    }

    @Override
    public void broadcast(PegasusClient.JobRequest request, Context writeResult) {
        try {
            for (RpcClient client : rpcClients) {
                client.submit(request, new GremlinResultProcessor(writeResult));
            }
        } catch (Exception e) {
            throw new RuntimeException("broadcast exception", e);
        }
    }
}

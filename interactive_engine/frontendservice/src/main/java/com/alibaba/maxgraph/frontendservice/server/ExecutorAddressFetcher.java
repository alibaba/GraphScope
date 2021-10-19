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
package com.alibaba.maxgraph.frontendservice.server;

import com.alibaba.maxgraph.common.rpc.RpcAddress;
import com.alibaba.maxgraph.sdkcommon.client.Endpoint;
import com.alibaba.maxgraph.frontendservice.ClientManager;
import com.alibaba.maxgraph.common.rpc.RpcAddressFetcher;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ExecutorAddressFetcher implements RpcAddressFetcher {
    private static final Logger logger = LoggerFactory.getLogger(ExecutorAddressFetcher.class);

    private ClientManager clientManager;

    public ExecutorAddressFetcher(ClientManager clientManager) {
        this.clientManager = clientManager;
    }

    @Override
    public List<RpcAddress> getAddressList() {
        List<RpcAddress> rpcAddressList = Lists.newArrayList();
        for (int i = 1; i <= clientManager.getExecutorCount(); i++) {
            Endpoint endpoint = clientManager.getExecutor(i);
            rpcAddressList.add(new RpcAddress(endpoint.getIp(), endpoint.getPort()));
        }

        return rpcAddressList;
    }

    @Override
    public List<Endpoint> getServiceAddress() {
        List<List<Endpoint>> endpointGroups = clientManager.getEndpointGroups();
        if (endpointGroups == null || endpointGroups.isEmpty()) {
            throw new IllegalArgumentException("There's no routing server");
        }
        List<Endpoint> endpoints = endpointGroups.get(RandomUtils.nextInt(0, endpointGroups.size()));
        if (null == endpoints || endpoints.isEmpty() || endpoints.size() < clientManager.getExecutorCount()) {
            throw new IllegalArgumentException("There's no routing server");
        }
        logger.info("find [" + endpoints.size() +"]endpoint address: " + endpoints.toString());
        return endpoints;
    }

    @Override
    public int getAddressCount() {
        return clientManager.getExecutorCount();
    }
}

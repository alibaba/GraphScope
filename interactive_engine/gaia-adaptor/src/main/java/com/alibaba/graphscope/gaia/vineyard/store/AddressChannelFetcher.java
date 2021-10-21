/*
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
package com.alibaba.graphscope.gaia.vineyard.store;

import com.alibaba.graphscope.gaia.broadcast.channel.AsyncRpcChannelFetcher;
import com.alibaba.maxgraph.common.rpc.RpcAddressFetcher;
import com.alibaba.maxgraph.sdkcommon.client.Endpoint;
import com.alibaba.pegasus.RpcChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

public class AddressChannelFetcher extends AsyncRpcChannelFetcher {
    private static final Logger logger = LoggerFactory.getLogger(AddressChannelFetcher.class);
    private RpcAddressFetcher addressFetcher;

    public AddressChannelFetcher(RpcAddressFetcher addressFetcher) {
        this.addressFetcher = addressFetcher;
    }

    @Override
    public List<RpcChannel> refresh() {
        List<Endpoint> endpoints = addressFetcher.getServiceAddress();
        logger.info("endpoints are {}", endpoints);
        return endpoints.stream().map(k -> new RpcChannel(k.getIp(), k.getRuntimeCtrlAndAsyncPort())).collect(Collectors.toList());
    }
}

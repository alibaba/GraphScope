/*
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

package com.alibaba.graphscope.ir.maxgraph;

import com.alibaba.graphscope.common.client.RpcChannelFetcher;
import com.alibaba.maxgraph.common.rpc.RpcAddressFetcher;
import com.alibaba.maxgraph.sdkcommon.client.Endpoint;
import com.alibaba.pegasus.RpcChannel;

import java.util.List;
import java.util.stream.Collectors;

public class RpcAddressChannelFetcher implements RpcChannelFetcher {
    private RpcAddressFetcher addressFetcher;

    public RpcAddressChannelFetcher(RpcAddressFetcher addressFetcher) {
        this.addressFetcher = addressFetcher;
    }

    @Override
    public List<RpcChannel> fetch() {
        List<Endpoint> endpoints = addressFetcher.getServiceAddress();
        return endpoints.stream().map(k -> new RpcChannel(k.getIp(), k.getRuntimeCtrlAndAsyncPort())).collect(Collectors.toList());
    }

    @Override
    public boolean isDynamic() {
        return true;
    }
}

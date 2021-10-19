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
package com.alibaba.maxgraph.common.rpc;

import com.alibaba.maxgraph.sdkcommon.client.Endpoint;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

public class DefaultRpcAddressFetcher implements RpcAddressFetcher {
    private List<RpcAddress> rpcAddressList;
    private List<Endpoint> serviceAddress;

    private DefaultRpcAddressFetcher(List<RpcAddress> rpcAddressList, RpcAddress serviceAddress) {
        this.rpcAddressList = rpcAddressList;
        this.serviceAddress = new ArrayList<>();
        for(RpcAddress rpcAddress : rpcAddressList) {
            this.serviceAddress.add(new Endpoint(serviceAddress.getHost(), serviceAddress.getPort()));
        }
    }

    @Override
    public List<RpcAddress> getAddressList() {
        return rpcAddressList;
    }

    @Override
    public List<Endpoint> getServiceAddress() {
        return serviceAddress;
    }

    @Override
    public int getAddressCount() {
        return rpcAddressList.size();
    }

    public static DefaultRpcAddressFetcher fromAddressList(List<RpcAddress> rpcAddressList) {
        return new DefaultRpcAddressFetcher(rpcAddressList, rpcAddressList.get(0));
    }

    public static DefaultRpcAddressFetcher fromAddressList(List<RpcAddress> rpcAddressList, RpcAddress serviceAddress) {
        return new DefaultRpcAddressFetcher(rpcAddressList, serviceAddress);
    }

    public static DefaultRpcAddressFetcher fromHostList(List<String> hostList) {
        List<RpcAddress> rpcAddressList = Lists.newArrayList();
        for (String host : hostList) {
            rpcAddressList.add(new RpcAddress(host));
        }

        return fromAddressList(rpcAddressList);
    }

    public static DefaultRpcAddressFetcher fromHostList(List<String> hostList, String serviceHost) {
        List<RpcAddress> rpcAddressList = Lists.newArrayList();
        for (String host : hostList) {
            rpcAddressList.add(new RpcAddress(host));
        }

        return fromAddressList(rpcAddressList, new RpcAddress(serviceHost));

    }
}

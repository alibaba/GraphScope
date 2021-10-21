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
package com.alibaba.maxgraph.server.query;

import com.alibaba.maxgraph.common.rpc.DefaultRpcAddressFetcher;
import com.alibaba.maxgraph.common.rpc.RpcAddressFetcher;
import com.alibaba.maxgraph.common.rpc.RpcConfig;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.sdkcommon.client.Endpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/** Connect to timely service var grpc, submit query and receive the response. */
public class TimelyRpcConnector extends RpcConnector {
    private static final Logger LOG = LoggerFactory.getLogger(TimelyRpcConnector.class);

    public TimelyRpcConnector(List<String> hosts, GraphSchema schema) {
        this(hosts, new RpcConfig(), schema);
    }

    public TimelyRpcConnector(List<String> hosts, RpcConfig rpcConfig, GraphSchema schema) {
        super(DefaultRpcAddressFetcher.fromHostList(hosts), rpcConfig);
    }

    public TimelyRpcConnector(RpcAddressFetcher addressFetcher, RpcConfig rpcConfig) {
        super(addressFetcher, rpcConfig);
    }

    @Override
    protected List<Endpoint> getTargetExecutorAddrs() {
        return super.addressFetcher.getServiceAddress().subList(0, 1);
    }
}

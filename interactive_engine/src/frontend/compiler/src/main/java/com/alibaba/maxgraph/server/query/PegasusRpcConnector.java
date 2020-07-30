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
package com.alibaba.maxgraph.server.query;

import com.alibaba.maxgraph.common.rpc.DefaultRpcAddressFetcher;
import com.alibaba.maxgraph.common.rpc.RpcAddressFetcher;
import com.alibaba.maxgraph.common.rpc.RpcConfig;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.sdkcommon.client.Endpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @Author: peaker.lgf
 * @Date: 2019-12-12 14:14
 **/
public class PegasusRpcConnector extends RpcConnector {
    private static final Logger LOG = LoggerFactory.getLogger(PegasusRpcConnector.class);

    public PegasusRpcConnector(List<String> hosts, GraphSchema schema) {
        this(hosts, new RpcConfig(), schema);
    }

    public PegasusRpcConnector(List<String> hosts, RpcConfig rpcConfig, GraphSchema schema) {
        super(DefaultRpcAddressFetcher.fromHostList(hosts), rpcConfig);
    }

    public PegasusRpcConnector(RpcAddressFetcher addressFetcher, RpcConfig rpcConfig) {
        super(addressFetcher, rpcConfig);
    }

    @Override
    protected List<Endpoint> getTargetExecutorAddrs() {
        return super.addressFetcher.getServiceAddress();
    }
}

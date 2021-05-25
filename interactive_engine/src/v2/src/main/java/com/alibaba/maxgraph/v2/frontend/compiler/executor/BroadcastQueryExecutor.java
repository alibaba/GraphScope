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
package com.alibaba.maxgraph.v2.frontend.compiler.executor;

import com.alibaba.maxgraph.v2.common.config.Configs;
import com.alibaba.maxgraph.v2.common.rpc.RoleClients;
import com.alibaba.maxgraph.v2.frontend.compiler.client.QueryExecuteRpcClient;
import com.alibaba.maxgraph.v2.frontend.compiler.client.QueryManageRpcClient;

import java.util.ArrayList;
import java.util.List;

/**
 * MaxGraph query will be broadcast to every runtime server
 */
public class BroadcastQueryExecutor extends QueryExecutor {

    public BroadcastQueryExecutor(Configs configs, RoleClients<QueryExecuteRpcClient> queryRpcClients,
                                  RoleClients<QueryManageRpcClient> manageRpcClients, int executorCount) {
        super(configs, queryRpcClients, manageRpcClients, executorCount);
    }

    @Override
    protected List<QueryExecuteRpcClient> getTargetClients() {
        List<QueryExecuteRpcClient> clients = new ArrayList<>(this.executorCount);
        for (int i = 0; i < this.executorCount; i++) {
            clients.add(this.queryRpcClients.getClient(i));
        }
        return clients;
    }

}

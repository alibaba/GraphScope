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

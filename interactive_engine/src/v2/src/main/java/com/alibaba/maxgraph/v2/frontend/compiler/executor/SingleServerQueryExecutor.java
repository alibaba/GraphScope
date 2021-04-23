package com.alibaba.maxgraph.v2.frontend.compiler.executor;

import com.alibaba.maxgraph.v2.common.config.Configs;
import com.alibaba.maxgraph.v2.common.rpc.RoleClients;
import com.alibaba.maxgraph.v2.frontend.compiler.client.QueryExecuteRpcClient;
import com.alibaba.maxgraph.v2.frontend.compiler.client.QueryManageRpcClient;

import java.util.Collections;
import java.util.List;

/**
 * MaxGraph query will be sent to single server in runtime
 */
public class SingleServerQueryExecutor extends QueryExecutor {

    public SingleServerQueryExecutor(Configs configs, RoleClients<QueryExecuteRpcClient> queryRpcClients,
                                     RoleClients<QueryManageRpcClient> manageRpcClients, int executorCount) {
        super(configs, queryRpcClients, manageRpcClients, executorCount);
    }

    @Override
    protected List<QueryExecuteRpcClient> getTargetClients() {
        return Collections.singletonList(this.queryRpcClients.getClient(0));
    }


}

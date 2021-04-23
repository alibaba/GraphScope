package com.alibaba.maxgraph.v2.store.executor;

import com.alibaba.maxgraph.v2.common.discovery.LocalNodeProvider;
import com.alibaba.maxgraph.v2.common.discovery.NodeDiscovery;

public class ExecutorDiscoveryManager {
    private LocalNodeProvider engineServerProvider;
    private NodeDiscovery engineServerDiscovery;
    private LocalNodeProvider storeQueryProvider;
    private NodeDiscovery storeQueryDiscovery;
    private LocalNodeProvider queryExecuteProvider;
    private NodeDiscovery queryExecuteDiscovery;
    private LocalNodeProvider queryManageProvider;
    private NodeDiscovery queryManageDiscovery;

    public ExecutorDiscoveryManager(LocalNodeProvider engineServerProvider,
                                    NodeDiscovery engineServerDiscovery,
                                    LocalNodeProvider storeQueryProvider,
                                    NodeDiscovery storeQueryDiscovery,
                                    LocalNodeProvider queryExecuteProvider,
                                    NodeDiscovery queryExecuteDiscovery,
                                    LocalNodeProvider queryManageProvider,
                                    NodeDiscovery queryManageDiscovery) {
        this.engineServerProvider = engineServerProvider;
        this.engineServerDiscovery = engineServerDiscovery;
        this.storeQueryProvider = storeQueryProvider;
        this.storeQueryDiscovery = storeQueryDiscovery;
        this.queryExecuteProvider = queryExecuteProvider;
        this.queryExecuteDiscovery = queryExecuteDiscovery;
        this.queryManageProvider = queryManageProvider;
        this.queryManageDiscovery = queryManageDiscovery;
    }

    public LocalNodeProvider getEngineServerProvider() {
        return engineServerProvider;
    }

    public NodeDiscovery getEngineServerDiscovery() {
        return engineServerDiscovery;
    }

    public LocalNodeProvider getStoreQueryProvider() {
        return storeQueryProvider;
    }

    public NodeDiscovery getStoreQueryDiscovery() {
        return storeQueryDiscovery;
    }

    public LocalNodeProvider getQueryExecuteProvider() {
        return queryExecuteProvider;
    }

    public NodeDiscovery getQueryExecuteDiscovery() {
        return queryExecuteDiscovery;
    }

    public LocalNodeProvider getQueryManageProvider() {
        return queryManageProvider;
    }

    public NodeDiscovery getQueryManageDiscovery() {
        return queryManageDiscovery;
    }
}

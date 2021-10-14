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
package com.alibaba.maxgraph.servers.maxgraph;

import com.alibaba.graphscope.groot.discovery.LocalNodeProvider;
import com.alibaba.graphscope.groot.discovery.NodeDiscovery;

public class ExecutorDiscoveryManager {
    private LocalNodeProvider engineServerProvider;
    private NodeDiscovery engineServerDiscovery;
    private LocalNodeProvider storeQueryProvider;
    private NodeDiscovery storeQueryDiscovery;
    private LocalNodeProvider queryExecuteProvider;
    private NodeDiscovery queryExecuteDiscovery;
    private LocalNodeProvider queryManageProvider;
    private NodeDiscovery queryManageDiscovery;

    public ExecutorDiscoveryManager(
            LocalNodeProvider engineServerProvider,
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

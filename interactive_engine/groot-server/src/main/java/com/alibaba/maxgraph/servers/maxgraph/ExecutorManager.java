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

import com.alibaba.maxgraph.common.config.Configs;
import com.alibaba.maxgraph.servers.jna.ExecutorLibrary;
import com.alibaba.maxgraph.servers.jna.JnaEngineServerResponse;
import com.alibaba.maxgraph.servers.jna.JnaRpcServerPortResponse;
import com.sun.jna.Pointer;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class ExecutorManager {
    private ExecutorDiscoveryManager discoveryManager;
    private Pointer executorHandler;
    private int nodeCount;
    private AtomicBoolean engineFlag = new AtomicBoolean(false);
    private Configs configs;

    public ExecutorManager(Configs configs, ExecutorDiscoveryManager discoveryManager) {
        this.configs = configs;
        this.discoveryManager = discoveryManager;
    }

    public void initialExecutor(Pointer executorHandler, int nodeCount) {
        this.executorHandler = executorHandler;
        this.nodeCount = nodeCount;
    }

    public void startEngineServer() {
        JnaEngineServerResponse engineServerResponse =
                ExecutorLibrary.INSTANCE.startEngineServer(this.executorHandler);
        if (engineServerResponse.errCode == 0) {
            discoveryManager
                    .getEngineServerProvider()
                    .apply(Integer.parseInt(engineServerResponse.address));
            discoveryManager.getEngineServerDiscovery().start();
        } else {
            throw new RuntimeException(engineServerResponse.errMsg);
        }
    }

    public void connectEngineServerList(List<String> serverAddresses) {
        ExecutorLibrary.INSTANCE.connectEngineServerList(
                this.executorHandler, String.join(",", serverAddresses));
        engineFlag.set(true);
    }

    public boolean checkEngineServersConnect() {
        return this.engineFlag.get();
    }

    public int getNodeCount() {
        return this.nodeCount;
    }

    public void startRpcServer() {
        JnaRpcServerPortResponse rpcServerPort =
                ExecutorLibrary.INSTANCE.startRpcServer(this.executorHandler);
        this.discoveryManager.getStoreQueryProvider().apply(rpcServerPort.storeQueryPort);
        this.discoveryManager.getQueryExecuteProvider().apply(rpcServerPort.queryExecutePort);
        this.discoveryManager.getQueryManageProvider().apply(rpcServerPort.queryManagePort);

        this.discoveryManager.getStoreQueryDiscovery().start();
        this.discoveryManager.getQueryExecuteDiscovery().start();
        this.discoveryManager.getQueryManageDiscovery().start();
    }

    public ExecutorDiscoveryManager getDiscoveryManager() {
        return this.discoveryManager;
    }

    public void stopEngineServer() {
        ExecutorLibrary.INSTANCE.stopEngineServer(this.executorHandler);
    }

    public void stopRpcServer() {
        ExecutorLibrary.INSTANCE.stopRpcServer(this.executorHandler);
    }
}

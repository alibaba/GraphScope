package com.alibaba.maxgraph.v2.store.executor;

import com.alibaba.maxgraph.proto.v2.ExecutorRpcConfigPb;
import com.alibaba.maxgraph.v2.common.config.Configs;
import com.alibaba.maxgraph.v2.store.executor.jna.ExecutorLibrary;
import com.alibaba.maxgraph.v2.store.executor.jna.JnaEngineServerResponse;
import com.alibaba.maxgraph.v2.store.executor.jna.JnaRpcServerPortResponse;
import com.sun.jna.Pointer;
import org.apache.commons.lang3.tuple.Pair;

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

    public void initialExecutor(Pointer executorHandler,
                                int nodeCount) {
        this.executorHandler = executorHandler;
        this.nodeCount = nodeCount;
    }

    public void startEngineServer() {
        JnaEngineServerResponse engineServerResponse = ExecutorLibrary.INSTANCE.startEngineServer(this.executorHandler);
        if (engineServerResponse.errCode == 0) {
            discoveryManager.getEngineServerProvider().apply(Integer.parseInt(engineServerResponse.address));
            discoveryManager.getEngineServerDiscovery().start();
        } else {
            throw new RuntimeException(engineServerResponse.errMsg);
        }
    }

    public void connectEngineServerList(List<String> serverAddresses) {
        ExecutorLibrary.INSTANCE.connectEngineServerList(this.executorHandler, String.join(",", serverAddresses));
        engineFlag.set(true);
    }

    public boolean checkEngineServersConnect() {
        return this.engineFlag.get();
    }

    public int getNodeCount() {
        return this.nodeCount;
    }

    public void startRpcServer() {
        ExecutorRpcConfigPb executorRpcConfigPb = ExecutorRpcConfigPb.newBuilder()
                .setQueryExecuteCount(ExecutorConfig.EXECUTOR_QUERY_THREAD_COUNT.get(this.configs))
                .setQueryManageCount(ExecutorConfig.EXECUTOR_QUERY_MANAGER_THREAD_COUNT.get(this.configs))
                .setStoreThreadCount(ExecutorConfig.EXECUTOR_QUERY_STORE_THREAD_COUNT.get(this.configs))
                .build();
        byte[] executorConfigBytes = executorRpcConfigPb.toByteArray();
        JnaRpcServerPortResponse rpcServerPort = ExecutorLibrary.INSTANCE.startRpcServer(this.executorHandler, executorConfigBytes, executorConfigBytes.length);
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

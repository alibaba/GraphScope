package com.alibaba.maxgraph.v2.store.executor.jna;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Pointer;

public interface ExecutorLibrary extends Library {
    ExecutorLibrary INSTANCE = Native.load("maxgraph_runtime", ExecutorLibrary.class);

    /**
     * Open executor server and get handler with given config
     *
     * @param config The given config
     * @param len    The len of config bytes
     * @return The executor handler
     */
    Pointer openExecutorServer(byte[] config, int len);

    /**
     * Add graph partition to given executor handler
     *
     * @param executor    The given executor handler
     * @param partitionId The partition id
     * @param graph       The graph handler
     */
    void addGraphPartition(Pointer executor, int partitionId, Pointer graph);

    /**
     * Add partition id --> worker id mapping
     *
     * @param executor    The given executor handler
     * @param partitionId The partition id
     * @param workerId    The worker id
     */
    void addPartitionWorkerMapping(Pointer executor, int partitionId, int workerId);

    /**
     * Start engine server and get server address
     *
     * @param executor The given executor handler
     * @return The engine server response
     */
    JnaEngineServerResponse startEngineServer(Pointer executor);

    /**
     * Connect current engine server to others
     *
     * @param executor  The given executor handler
     * @param addresses The address list
     */
    void connectEngineServerList(Pointer executor, String addresses);

    /**
     * Stop the given engine server
     *
     * @param executor The given executor handler
     */
    void stopEngineServer(Pointer executor);

    /**
     * Start rpc server in executor
     *
     * @param executor The given executor
     * @param config   The given config of rpc
     * @param len      The len of config
     * @return The rpc server response
     */
    JnaRpcServerPortResponse startRpcServer(Pointer executor, byte[] config, int len);

    /**
     * Stop the rpc server in given executor
     *
     * @param executor The given executor handler
     */
    void stopRpcServer(Pointer executor);

    /**
     * Drop jna engine server response
     */
    void dropJnaServerResponse(JnaEngineServerResponse response);
}

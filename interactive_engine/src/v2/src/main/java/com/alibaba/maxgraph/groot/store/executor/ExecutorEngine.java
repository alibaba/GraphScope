package com.alibaba.maxgraph.groot.store.executor;

import com.alibaba.maxgraph.groot.common.discovery.NodeDiscovery;
import com.alibaba.maxgraph.groot.store.GraphPartition;

public interface ExecutorEngine extends NodeDiscovery.Listener {

    void init();

    void addPartition(GraphPartition partition);

    void updatePartitionRouting(int partitionId, int serverId);

    void start();

    void stop();
}

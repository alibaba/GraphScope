package com.alibaba.maxgraph.v2.store.executor;

import com.alibaba.maxgraph.v2.common.discovery.NodeDiscovery;
import com.alibaba.maxgraph.v2.store.GraphPartition;

public interface ExecutorEngine extends NodeDiscovery.Listener {

    void init();

    void addPartition(GraphPartition partition);

    void updatePartitionRouting(int partitionId, int serverId);

    void start();

    void stop();
}

package com.alibaba.maxgraph.servers.gaia;

import com.alibaba.graphscope.groot.discovery.NodeDiscovery;
import com.alibaba.graphscope.groot.store.GraphPartition;

public interface ExecutorEngine extends NodeDiscovery.Listener {

    void init();

    void addPartition(GraphPartition partition);

    void updatePartitionRouting(int partitionId, int serverId);

    void start();

    void stop();
}

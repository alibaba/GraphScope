package com.alibaba.maxgraph.v2.common.discovery;

import java.util.Map;

public interface NodeDiscovery {

    void start();

    void stop();

    void addListener(Listener listener);

    void removeListener(Listener listener);

    MaxGraphNode getLocalNode();

    interface Listener {
        void nodesJoin(RoleType role, Map<Integer, MaxGraphNode> nodes);

        void nodesLeft(RoleType role, Map<Integer, MaxGraphNode> nodes);
    }
}

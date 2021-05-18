package com.alibaba.maxgraph.v2.common.discovery;


import com.alibaba.maxgraph.v2.common.config.CommonConfig;
import com.alibaba.maxgraph.v2.common.config.Configs;
import com.alibaba.maxgraph.v2.common.config.DiscoveryConfig;
import com.alibaba.maxgraph.v2.common.exception.MaxGraphException;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class LocalNodeProvider implements Function<Integer, MaxGraphNode> {

    private Configs configs;
    private RoleType roleType;
    private AtomicReference<MaxGraphNode> localNodeRef = new AtomicReference<>();

    public LocalNodeProvider(Configs configs) {
        this(RoleType.fromName(CommonConfig.ROLE_NAME.get(configs)), configs);
    }

    public LocalNodeProvider(RoleType roleType, Configs configs) {
        this.roleType = roleType;
        this.configs = configs;
    }

    @Override
    public MaxGraphNode apply(Integer port) {
        boolean suc = localNodeRef.compareAndSet(null, MaxGraphNode.createGraphNode(roleType, configs, port));
        if (!suc) {
            if (!CommonConfig.DISCOVERY_MODE.get(this.configs).equalsIgnoreCase("file")) {
                throw new MaxGraphException("localNode can only be set once");
            }
        }
        return localNodeRef.get();
    }

    public MaxGraphNode get() {
        return localNodeRef.get();
    }
}

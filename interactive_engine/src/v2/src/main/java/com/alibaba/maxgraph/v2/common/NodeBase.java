package com.alibaba.maxgraph.v2.common;

import com.alibaba.maxgraph.v2.common.config.CommonConfig;
import com.alibaba.maxgraph.v2.common.config.Configs;
import com.alibaba.maxgraph.v2.common.discovery.RoleType;

import java.io.Closeable;

public abstract class NodeBase implements Closeable {

    protected RoleType roleType;
    protected int idx;

    public NodeBase() {
        this.roleType = RoleType.UNKNOWN;
        this.idx = 0;
    }

    public NodeBase(Configs configs) {
        this.roleType = RoleType.fromName(CommonConfig.ROLE_NAME.get(configs));
        this.idx = CommonConfig.NODE_IDX.get(configs);
    }

    protected Configs reConfig(Configs configs) {
        int storeCount = CommonConfig.STORE_NODE_COUNT.get(configs);
        int ingestorCount = CommonConfig.INGESTOR_NODE_COUNT.get(configs);
        return Configs.newBuilder(configs)
                .put(String.format(CommonConfig.NODE_COUNT_FORMAT, RoleType.EXECUTOR_ENGINE.getName()), String.valueOf(storeCount))
                .put(String.format(CommonConfig.NODE_COUNT_FORMAT, RoleType.EXECUTOR_GRAPH.getName()), String.valueOf(storeCount))
                .put(String.format(CommonConfig.NODE_COUNT_FORMAT, RoleType.EXECUTOR_MANAGE.getName()), String.valueOf(storeCount))
                .put(String.format(CommonConfig.NODE_COUNT_FORMAT, RoleType.EXECUTOR_QUERY.getName()), String.valueOf(storeCount))
                .put(CommonConfig.INGESTOR_QUEUE_COUNT.getKey(), String.valueOf(ingestorCount))
                .build();
    }

    public abstract void start();

    public String getName() {
        return roleType + "#" + idx;
    }
}

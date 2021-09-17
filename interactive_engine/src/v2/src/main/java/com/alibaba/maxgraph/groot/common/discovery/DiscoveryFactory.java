package com.alibaba.maxgraph.groot.common.discovery;

import com.alibaba.maxgraph.common.config.CommonConfig;
import com.alibaba.maxgraph.common.config.Configs;
import com.alibaba.maxgraph.common.util.CuratorUtils;
import org.apache.curator.framework.CuratorFramework;

public class DiscoveryFactory {

    private Configs configs;

    public DiscoveryFactory(Configs configs) {
        this.configs = configs;
    }

    private CuratorFramework curator = null;

    private CuratorFramework getCurator() {
        if (this.curator == null) {
            this.curator = CuratorUtils.makeCurator(this.configs);
            this.curator.start();
        }
        return this.curator;
    }

    public NodeDiscovery makeDiscovery(LocalNodeProvider localNodeProvider) {
        String discoveryMode = CommonConfig.DISCOVERY_MODE.get(this.configs);
        switch (discoveryMode) {
            case "file":
                return new FileDiscovery(this.configs);
            case "zookeeper":
                return new ZkDiscovery(this.configs, localNodeProvider, getCurator());
            default:
                throw new IllegalArgumentException("invalid discovery mode [" + discoveryMode + "]");
        }
    }
}

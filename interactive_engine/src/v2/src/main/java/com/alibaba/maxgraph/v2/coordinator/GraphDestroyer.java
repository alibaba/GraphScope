package com.alibaba.maxgraph.v2.coordinator;

import com.alibaba.maxgraph.v2.common.config.Configs;
import com.alibaba.maxgraph.v2.common.config.ZkConfig;
import com.alibaba.maxgraph.v2.common.wal.LogService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphDestroyer {
    private static final Logger logger = LoggerFactory.getLogger(GraphDestroyer.class);

    private Configs configs;
    private CuratorFramework curator;
    private LogService logService;

    public GraphDestroyer(Configs configs, CuratorFramework curator, LogService logService) {
        this.configs = configs;
        this.curator = curator;
        this.logService = logService;
    }

    public void destroyAll() throws Exception {
        logger.info("start destroy graph");
        if (this.logService.initialized()) {
            this.logService.destroy();
            logger.info("logService destroyed");
        }
        String zkRoot = ZkConfig.ZK_BASE_PATH.get(configs);
        Stat stat = this.curator.checkExists().forPath(zkRoot);
        if (stat == null) {
            return;
        }
        this.curator.delete().deletingChildrenIfNeeded().forPath(zkRoot);
        logger.info("zk destroyed");
    }
}

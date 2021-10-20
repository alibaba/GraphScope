/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.groot.coordinator;

import com.alibaba.maxgraph.common.config.Configs;
import com.alibaba.maxgraph.common.config.ZkConfig;
import com.alibaba.graphscope.groot.wal.LogService;
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

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
package com.alibaba.graphscope.groot.servers;

import com.alibaba.graphscope.groot.CuratorUtils;
import com.alibaba.graphscope.groot.OTELUtils;
import com.alibaba.graphscope.groot.Utils;
import com.alibaba.graphscope.groot.common.RoleType;
import com.alibaba.graphscope.groot.common.config.*;
import com.alibaba.graphscope.groot.common.exception.InternalException;
import com.alibaba.graphscope.groot.common.exception.InvalidArgumentException;

import io.opentelemetry.api.OpenTelemetry;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GrootGraph {
    private static final Logger logger = LoggerFactory.getLogger(GrootGraph.class);

    public static void main(String[] args) throws Exception {
        String configFile = System.getProperty("config.file");
        Configs conf = new Configs(configFile);
        conf = reConfig(conf);
        logger.info("Configs {}", conf);
        OpenTelemetry sdk = OTELUtils.openTelemetry();
        NodeBase node;
        if (args.length == 0 || args[0].equals("all-in-one")) {
            logger.warn("No role type, use MaxNode");
            try {
                node = new MaxNode(conf);
            } catch (Exception e) {
                throw new InternalException(e);
            }
        } else {
            String roleName = args[0];
            RoleType roleType = RoleType.fromName(roleName);
            switch (roleType) {
                case FRONTEND:
                    node = new Frontend(conf);
                    break;
                case STORE:
                    node = new Store(conf);
                    break;
                case COORDINATOR:
                    node = new Coordinator(conf);
                    break;
                default:
                    throw new InvalidArgumentException("invalid roleType [" + roleType + "]");
            }

            boolean writeHAEnabled = CommonConfig.WRITE_HA_ENABLED.get(conf);
            LeaderLatch latch;
            if (writeHAEnabled && roleType == RoleType.STORE) {
                int nodeID = CommonConfig.NODE_IDX.get(conf);
                String latchPath = ZkConfig.ZK_BASE_PATH.get(conf) + "/store/leader/" + nodeID;
                CuratorFramework curator = CuratorUtils.makeCurator(conf);
                curator.start();
                try {
                    while (true) {
                        latch = new LeaderLatch(curator, latchPath);
                        latch.start();
                        logger.info(
                                "latch id: {}, leader: {}, state: {}",
                                latch.getId(),
                                latch.getLeader(),
                                latch.getState());
                        latch.await();
                        // Sleep 10s before check the lock to prevent the leader has not
                        // released the resource yet.
                        Thread.sleep(10000);
                        if (Utils.isLockAvailable(conf) && !Utils.isMetaFreshEnough(conf, 9000)) {
                            logger.info("LOCK is available and meta stop updating, node starting");
                            break;
                        }
                        latch.close();
                        logger.info(
                                "LOCK is unavailable or the meta is still updating, the leader may"
                                        + " still exists");
                        // The leader has lost connection but still alive,
                        // give it another chance
                        Thread.sleep(60000);
                    }
                } catch (Exception e) {
                    logger.error("Exception while leader election", e);
                    throw e;
                }
                // curator.close();
            }
        }
        NodeLauncher launcher = new NodeLauncher(node);
        launcher.start();
        logger.info("node started. [" + node.getName() + "]");
    }

    private static Configs reConfig(Configs in) {
        Configs.Builder out = Configs.newBuilder(in);
        if (CommonConfig.SECONDARY_INSTANCE_ENABLED.get(in)) {
            out.put(StoreConfig.STORE_STORAGE_ENGINE.getKey(), "rocksdb_as_secondary");
        }
        if (CommonConfig.WRITE_HA_ENABLED.get(in)) {
            logger.info("Write HA mode needs discovery mode to be 'zookeeper'");
            out.put(CommonConfig.DISCOVERY_MODE.getKey(), "zookeeper");
        }

        return out.build();
    }
}

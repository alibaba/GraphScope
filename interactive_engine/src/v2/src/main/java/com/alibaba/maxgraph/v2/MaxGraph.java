/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.v2;

import com.alibaba.maxgraph.v2.common.NodeBase;
import com.alibaba.maxgraph.v2.common.NodeLauncher;
import com.alibaba.maxgraph.v2.common.config.Configs;
import com.alibaba.maxgraph.v2.common.discovery.RoleType;
import com.alibaba.maxgraph.v2.common.exception.MaxGraphException;
import com.alibaba.maxgraph.v2.coordinator.Coordinator;
import com.alibaba.maxgraph.v2.grafting.frontend.Frontend;
import com.alibaba.maxgraph.v2.ingestor.Ingestor;
import com.alibaba.maxgraph.v2.store.GaiaStore;
import com.alibaba.maxgraph.v2.store.Store;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MaxGraph {
    private static final Logger logger = LoggerFactory.getLogger(MaxGraph.class);

    public static void main(String[] args) throws IOException {
        String configFile = System.getProperty("config.file");
        Configs conf = new Configs(configFile);
        NodeBase node;
        if (args.length == 0) {
            logger.warn("No role type, use MaxNode");
            try {
                node = new MaxNode(conf);
            } catch (Exception e) {
                throw new MaxGraphException(e);
            }
        } else {
            String roleName = args[0];
            if (roleName.equalsIgnoreCase("store-gaia")) {
                node = new GaiaStore(conf);
            } else if(roleName.equalsIgnoreCase("frontend-gaia")) {
                node = new com.alibaba.graphscope.gaia.Frontend(conf);
            } else {
                RoleType roleType = RoleType.fromName(roleName);
                switch (roleType) {
                    case FRONTEND:
                        node = new Frontend(conf);
                        break;
                    case INGESTOR:
                        node = new Ingestor(conf);
                        break;
                    case STORE:
                        node = new Store(conf);
                        break;
                    case COORDINATOR:
                        node = new Coordinator(conf);
                        break;
                    default:
                        throw new IllegalArgumentException("invalid roleType [" + roleType + "]");
                }
            }
        }
        new NodeLauncher(node).start();
        logger.info("node started. [" + node.getName() + "]");
    }
}

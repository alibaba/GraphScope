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

import com.alibaba.graphscope.compiler.api.exception.GrootException;
import com.alibaba.graphscope.groot.common.RoleType;
import com.alibaba.graphscope.groot.common.config.Configs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class GrootGraph {
    private static final Logger logger = LoggerFactory.getLogger(GrootGraph.class);

    public static void main(String[] args) throws IOException {
        String configFile = System.getProperty("config.file");
        Configs conf = new Configs(configFile);

        NodeBase node;
        if (args.length == 0) {
            logger.warn("No role type, use MaxNode");
            try {
                node = new MaxNode(conf);
            } catch (Exception e) {
                throw new GrootException(e);
            }
        } else {
            String roleName = args[0];
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
        new NodeLauncher(node).start();
        logger.info("node started. [" + node.getName() + "]");
    }
}

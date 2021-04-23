package com.alibaba.maxgraph.v2;

import com.alibaba.maxgraph.v2.common.NodeBase;
import com.alibaba.maxgraph.v2.common.NodeLauncher;
import com.alibaba.maxgraph.v2.common.config.Configs;
import com.alibaba.maxgraph.v2.common.discovery.RoleType;
import com.alibaba.maxgraph.v2.common.exception.MaxGraphException;
import com.alibaba.maxgraph.v2.coordinator.Coordinator;
import com.alibaba.maxgraph.v2.grafting.frontend.Frontend;
import com.alibaba.maxgraph.v2.ingestor.Ingestor;
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

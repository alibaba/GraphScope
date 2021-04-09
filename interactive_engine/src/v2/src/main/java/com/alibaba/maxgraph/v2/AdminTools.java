package com.alibaba.maxgraph.v2;

import com.alibaba.maxgraph.v2.common.config.Configs;
import com.alibaba.maxgraph.v2.common.exception.MaxGraphException;
import com.alibaba.maxgraph.v2.common.util.CuratorUtils;
import com.alibaba.maxgraph.v2.common.wal.LogService;
import com.alibaba.maxgraph.v2.common.wal.kafka.KafkaLogService;
import com.alibaba.maxgraph.v2.coordinator.GraphDestroyer;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class AdminTools {
    private static final Logger logger = LoggerFactory.getLogger(AdminTools.class);

    private String[] args;
    private Configs conf;

    public AdminTools(String[] args, Configs conf) {
        this.args = args;
        this.conf = conf;
    }

    public void exetute() {
        String cmd = this.args[0];
        if (cmd.equalsIgnoreCase("destroy-graph")) {
            CuratorFramework curator = CuratorUtils.makeCurator(this.conf);
            curator.start();
            LogService logService = new KafkaLogService(this.conf);
            try {
                new GraphDestroyer(this.conf, curator, logService).destroyAll();
                logger.info("graph destroyed");
            } catch (Exception e) {
                throw new MaxGraphException(e);
            }
        } else {
            throw new UnsupportedOperationException(cmd);
        }
    }

    public static void main(String[] args) throws IOException {
        String configFile = System.getProperty("config.file");
        Configs conf = new Configs(configFile);
        try {
            new AdminTools(args, conf).exetute();
            System.exit(0);
        } catch (Exception e) {
            logger.error("execute admin tools failed", e);
            System.exit(-1);
        }
    }
}

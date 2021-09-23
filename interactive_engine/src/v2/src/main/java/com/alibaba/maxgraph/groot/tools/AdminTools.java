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
package com.alibaba.maxgraph.groot.tools;

import com.alibaba.maxgraph.common.config.Configs;
import com.alibaba.maxgraph.compiler.api.exception.MaxGraphException;
import com.alibaba.maxgraph.common.util.CuratorUtils;
import com.alibaba.maxgraph.groot.common.wal.LogService;
import com.alibaba.maxgraph.groot.common.wal.kafka.KafkaLogService;
import com.alibaba.maxgraph.groot.coordinator.GraphDestroyer;
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

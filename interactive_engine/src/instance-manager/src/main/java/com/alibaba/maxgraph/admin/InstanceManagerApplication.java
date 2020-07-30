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
package com.alibaba.maxgraph.admin;

import com.alibaba.maxgraph.admin.config.InstanceProperties;
import com.alibaba.maxgraph.admin.memory.FrontendMemoryStorage;
import com.alibaba.maxgraph.admin.memory.InstanceEntity;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Instance manager application starter
 */
@SpringBootApplication(scanBasePackages = {"com.alibaba.maxgraph.admin"})
@EnableConfigurationProperties({InstanceProperties.class})
public class InstanceManagerApplication {
    private static final Logger logger = LoggerFactory.getLogger(InstanceManagerApplication.class);

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(InstanceManagerApplication.class);
        application.run(args);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Start to close all the instances alive in the manager");
            for (Map.Entry<String, InstanceEntity> entry : FrontendMemoryStorage.getFrontendStorage().getGraphFrontendEndpoint().entrySet()) {
                logger.info("Start to close instance of graph " + entry.getKey());
                try {
                    List<String> closeCommandList = new ArrayList<>();
                    closeCommandList.add(entry.getValue().getCloseScript());
                    closeCommandList.add(entry.getKey());
                    closeCommandList.add(entry.getValue().getPodNameList());
                    closeCommandList.add(entry.getValue().getContainerNameList());
                    String command = StringUtils.join(closeCommandList, " ");
                    logger.info("start to close instance with command " + command);
                    Process process = Runtime.getRuntime().exec(command);
                    List<String> errorValueList = IOUtils.readLines(process.getErrorStream(), "UTF-8");
                    List<String> infoValueList = IOUtils.readLines(process.getInputStream(), "UTF-8");
                    infoValueList.addAll(errorValueList);
                    process.waitFor();
                } catch (Exception e) {
                    logger.warn("close instance " + entry.getKey() + " failed", e);
                }
            }
        }));
    }
}

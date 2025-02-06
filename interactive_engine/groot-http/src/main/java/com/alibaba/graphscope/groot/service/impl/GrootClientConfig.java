/*
 * Copyright 2025 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.graphscope.groot.service.impl;

import com.alibaba.graphscope.groot.sdk.GrootClient;
import com.alibaba.graphscope.groot.sdk.GrootClient.GrootClientBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

@Configuration
public class GrootClientConfig {
    private static final Logger logger = LoggerFactory.getLogger(GrootClientConfig.class);

    @Autowired private Environment env;

    @Bean
    public GrootClient grootClient() {

        String username = env.getProperty("auth.username", "");
        String password = env.getProperty("auth.password", "");
        int port = env.getProperty("frontend.service.port", Integer.class, 55556);
        logger.info(
                "Groot HTTP Configs username: {}, password: {}, port {}", username, password, port);

        GrootClientBuilder builder = GrootClient.newBuilder().addHost("localhost", port);
        if (!username.isEmpty() && !password.isEmpty()) {
            builder.setUsername(username).setPassword(password);
        }
        return builder.build();
    }
}

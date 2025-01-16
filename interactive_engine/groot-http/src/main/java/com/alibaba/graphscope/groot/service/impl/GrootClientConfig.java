package com.alibaba.graphscope.groot.service.impl;

import com.alibaba.graphscope.groot.common.config.Configs;
import com.alibaba.graphscope.groot.common.config.FrontendConfig;
import com.alibaba.graphscope.groot.sdk.GrootClient;
import com.alibaba.graphscope.groot.sdk.GrootClient.GrootClientBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

@Configuration
public class GrootClientConfig {
    private static final Logger logger = LoggerFactory.getLogger(GrootClientConfig.class);

    @Bean
    public GrootClient grootClient() {

        String configFile = System.getProperty("config.file");
        if (configFile == null || configFile.isEmpty()) {
            logger.error("Config file is not specified or is empty. Use default config.");
            return defaultClient();
        }
        Configs conf;
        try {
            conf = new Configs(configFile);
        } catch (IOException e) {
            logger.error("Failed to load config file {}. Use default config.", configFile);
            return defaultClient();
        }
        logger.info("Groot HTTP Configs {}", conf);
        int port = FrontendConfig.FRONTEND_SERVICE_PORT.get(conf);
        String username = FrontendConfig.AUTH_USERNAME.get(conf);
        String password = FrontendConfig.AUTH_PASSWORD.get(conf);

        GrootClientBuilder builder = GrootClient.newBuilder().addHost("localhost", port);
        if (!username.isEmpty() && !password.isEmpty()) {
            logger.info("Groot HTTP Configs username: {}, password: {}", username, password);
            builder.setUsername(username).setPassword(password);
        }

        return builder.build();
    }

    private GrootClient defaultClient() {
        return GrootClient.newBuilder().addHost("localhost", 55556).build();
    }
}

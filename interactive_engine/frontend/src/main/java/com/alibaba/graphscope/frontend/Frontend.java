package com.alibaba.graphscope.frontend;

import com.alibaba.graphscope.common.client.HostsChannelFetcher;
import com.alibaba.graphscope.common.client.RpcChannelFetcher;
import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.FrontendConfig;
import com.alibaba.graphscope.common.config.GraphConfig;
import com.alibaba.graphscope.common.manager.IrMetaQueryCallback;
import com.alibaba.graphscope.common.store.IrMetaFetcher;
import com.alibaba.graphscope.gremlin.integration.result.TestGraphFactory;
import com.alibaba.graphscope.gremlin.service.IrGremlinServer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class Frontend implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(Frontend.class);
    private IrGremlinServer server;
    private Configs configs;

    public Frontend(String configFile) throws IOException {
        this(new Configs(configFile));
    }

    public Frontend(Configs configs) {
        this.configs = configs;
    }

    public void start() throws Exception {
        logger.info("Configs {}", configs.toString());
        String vineyardSchemaPath = GraphConfig.GRAPH_SCHEMA.get(configs);
        logger.info("Read schema from vineyard schema file {}", vineyardSchemaPath);
        IrMetaFetcher irMetaFetcher = new VineyardMetaFetcher(vineyardSchemaPath);
        RpcChannelFetcher channelFetcher = new HostsChannelFetcher(configs);
        int port = FrontendConfig.FRONTEND_SERVICE_PORT.get(configs);
        IrMetaQueryCallback queryCallback = new IrMetaQueryCallback(irMetaFetcher);
        server = new IrGremlinServer(port);
        server.start(
                configs, irMetaFetcher, channelFetcher, queryCallback, TestGraphFactory.VINEYARD);
    }

    @Override
    public void close() throws Exception {
        if (server != null) {
            server.close();
        }
    }

    public static void main(String[] args) {
        logger.info("start to run Frontend.");
        if (args == null || args.length < 1) {
            logger.error("Please give the path of config file.");
            System.exit(1);
        }
        try (Frontend frontend = new Frontend(args[0])) {
            frontend.start();
            CountDownLatch shutdown = new CountDownLatch(1);
            shutdown.await();
        } catch (Throwable t) {
            logger.error("Error in frontend main:", t);
            System.exit(1);
        }
        System.exit(0);
    }
}

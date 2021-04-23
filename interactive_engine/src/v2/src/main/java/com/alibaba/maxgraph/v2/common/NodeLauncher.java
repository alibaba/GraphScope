package com.alibaba.maxgraph.v2.common;

import com.alibaba.maxgraph.v2.common.exception.MaxGraphException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class NodeLauncher {
    private static final Logger logger = LoggerFactory.getLogger(NodeLauncher.class);

    private NodeBase node;

    private Thread keepAliveThread;
    private CountDownLatch keepAliveLatch = new CountDownLatch(1);

    public NodeLauncher(NodeBase node) {
        this.node = node;
        keepAliveThread = new Thread(() -> {
            try {
                keepAliveLatch.await();
            } catch (InterruptedException e) {
                // Ignore
            }
        }, "keep-alive");
        keepAliveThread.setDaemon(false);
    }

    public void start() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                this.node.close();
            } catch (Exception e) {
                logger.error("failed to stop node", e);
            }
        }));
        Runtime.getRuntime().addShutdownHook(new Thread(() -> keepAliveLatch.countDown()));
        try {
            this.node.start();
        } catch (Exception e) {
            logger.error("start node failed", e);
            throw new MaxGraphException(e);
        }
        this.keepAliveThread.start();
    }
}

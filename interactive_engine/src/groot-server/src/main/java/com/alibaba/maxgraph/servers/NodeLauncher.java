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
package com.alibaba.maxgraph.servers;

import com.alibaba.maxgraph.compiler.api.exception.MaxGraphException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class NodeLauncher {
    private static final Logger logger = LoggerFactory.getLogger(NodeLauncher.class);

    private NodeBase node;

    private Thread keepAliveThread;
    private CountDownLatch keepAliveLatch = new CountDownLatch(1);

    public NodeLauncher(NodeBase node) {
        this.node = node;
        keepAliveThread =
                new Thread(
                        () -> {
                            try {
                                keepAliveLatch.await();
                            } catch (InterruptedException e) {
                                // Ignore
                            }
                        },
                        "keep-alive");
        keepAliveThread.setDaemon(false);
    }

    public void start() {
        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread(
                                () -> {
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

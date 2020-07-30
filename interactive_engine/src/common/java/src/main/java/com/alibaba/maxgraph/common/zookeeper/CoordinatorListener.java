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
package com.alibaba.maxgraph.common.zookeeper;

import com.alibaba.maxgraph.sdkcommon.client.Endpoint;
import com.alibaba.maxgraph.common.util.CommonUtil;
import com.alibaba.maxgraph.sdkcommon.util.JSON;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.Consumer;


/**
 * Created by beimian.mcq on 16/10/27.
 */
public class CoordinatorListener implements NodeCacheListener {
    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorListener.class);
    private Endpoint coordinatorEndpoint;
    private final CopyOnWriteArrayList<Consumer> updatedListenerTasks = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<Runnable> missedLintenerTasks = new CopyOnWriteArrayList<>();
    private final NodeCache nodeCache;
    private final ThreadPoolExecutor executor;
    private final int serverId;
    // 在实际应用中并不会出现一个进程中有两个server,此处使用Map结构主要是用于测试环境.
    private static Map<Integer, CoordinatorListener> instances = new HashMap<>();

    public synchronized static CoordinatorListener getInstance(final int serverId) {
        return instances.getOrDefault(serverId, new CoordinatorListener());
    }

    public synchronized static CoordinatorListener create(String graphName, final ZkUtils zkUtils, final int serverId) {
        if (zkUtils == null)
            return new CoordinatorListener();

        CoordinatorListener controllerListener = new CoordinatorListener(graphName, zkUtils, serverId);
        instances.put(serverId, controllerListener);

        return controllerListener;

    }

    //Just for Test
    private CoordinatorListener() {
        this.nodeCache = null;
        this.executor = null;
        this.serverId = -1;
    }

    private CoordinatorListener(String graphName, ZkUtils zkUtils, int serverId) {
        this.serverId = serverId;

        ThreadFactory factory = CommonUtil.createFactoryWithDefaultExceptionHandler("Controller-listener-S" + serverId, LOG);
        executor = new ThreadPoolExecutor(2, 4, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), factory);
        nodeCache = zkUtils.watchNode(ZKPaths.getCoordinatorPath(graphName), this);
    }

    public void startUp() {
        try {
            nodeCache.start();
        } catch (Exception e) {
            throw new RuntimeException("Coordinator listener start failed;", e);
        }
    }

    public void addControllerMissedListener(final Runnable listener) {
        missedLintenerTasks.add(listener);
    }

    public void addControllerUpdatedListener(final Consumer<Endpoint> listener) {
        updatedListenerTasks.add(listener);
    }


    @Override
    public void nodeChanged() throws Exception {
        LOG.debug("Detect Controller change by server {}", serverId);

        ChildData data = nodeCache.getCurrentData();

        if (data == null) {
            LOG.info("Controller missed on server {}", serverId);
            coordinatorEndpoint = null;
            missedLintenerTasks.forEach(executor::execute);

        } else {
            coordinatorEndpoint = JSON.fromJson(new String(data.getData()), Endpoint.class);
            LOG.info("Controller address updated {} on server {}", coordinatorEndpoint.toString(), serverId);
            updatedListenerTasks.forEach(t -> executor.execute(() -> t.accept(coordinatorEndpoint)));
        }
    }

    public void shutDown() {
        try {
            cleanListener();
            if (nodeCache != null) {
                nodeCache.close();
            }

            if (executor != null) {
                executor.shutdownNow();
            }

            instances.remove(serverId);
        } catch (IOException e) {
            LOG.error("Listener on server {} may not be complete shutdown", serverId);
        }
    }

    public void cleanListener() {
        updatedListenerTasks.clear();
        missedLintenerTasks.clear();
    }
}

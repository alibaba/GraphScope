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
package com.alibaba.maxgraph.common.cluster.management;

import com.alibaba.maxgraph.common.cluster.InstanceConfig;
import com.alibaba.maxgraph.common.component.AbstractLifecycleComponent;
import com.alibaba.maxgraph.common.util.CommonUtil;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

public class ClusterApplierService extends AbstractLifecycleComponent {

    private final Collection<ClusterStateListener> clusterStateListeners = new CopyOnWriteArrayList<>();
    private final Collection<ClusterStateApplier> clusterStateAppliers = new CopyOnWriteArrayList<>();

    private final AtomicReference<ClusterState> lastAppliedState;

    private ExecutorService threadPoolExecutor;

    public ClusterApplierService(InstanceConfig settings) {
        super(settings);
        this.lastAppliedState = new AtomicReference<>();
    }

    @Override
    protected void doStart() {
        logger.info("Start ClusterApplierService...");
        setInitialState(ClusterState.emptyClusterState());
        threadPoolExecutor = Executors.newSingleThreadExecutor(
                CommonUtil.createFactoryWithDefaultExceptionHandler("cluster-applier-service", logger));
    }

    @Override
    protected void doStop() {
        logger.info("Stop ClusterApplierService...");
        if (threadPoolExecutor != null) {
            threadPoolExecutor.shutdown();
        }
    }

    @Override
    protected void doClose() throws IOException {

    }

    public ClusterState lastAppliedState() {
        return lastAppliedState.get();
    }


    class UpdateTask implements Runnable {

        final ClusterState nextClusterState;
        final String source;

        UpdateTask(String source, ClusterState nextClusterState) {
            this.source = source;
            this.nextClusterState = nextClusterState;
        }

        @Override
        public void run() {
            runTask(this);
        }
    }

    protected void runTask(UpdateTask task) {
        if (!lifecycle.started()) {
            return;
        }

        final ClusterState previousClusterState = lastAppliedState.get();
        final ClusterState newClusterState = task.nextClusterState;

        if (previousClusterState == newClusterState || previousClusterState.version() == newClusterState.version()) {
            logger.debug("no change in cluster lastAppliedState");
        } else {
            logger.info("Update task applying changes. Source: " + task.source +
                    ". from " + previousClusterState.version() + ", to " + newClusterState.version());
            logger.info("previous: " + previousClusterState.toProto().toString());
            logger.info("new: " + newClusterState.toProto().toString());
            applyChanges(task, previousClusterState, newClusterState);
        }
    }

    private void applyChanges(UpdateTask task, ClusterState previousClusterState, ClusterState newClusterState) {
        ClusterChangedEvent clusterChangedEvent =
                new ClusterChangedEvent(task.source, newClusterState, previousClusterState);
        // nodes delta, update connections
        // call some cluster lastAppliedState appliers
        callClusterStateAppliers(clusterChangedEvent);
        lastAppliedState.set(newClusterState);
        callClusterStateListeners(clusterChangedEvent);
    }

    private void callClusterStateAppliers(ClusterChangedEvent event) {
        for (ClusterStateApplier applier : clusterStateAppliers) {
            applier.applyClusterState(event);
        }
    }

    private void callClusterStateListeners(ClusterChangedEvent clusterChangedEvent) {
        for(ClusterStateListener listener : clusterStateListeners) {
            try {
                listener.clusterChanged(clusterChangedEvent);
            } catch (Exception ex) {
                logger.error("fail to notify listener", ex);
            }
        }
    }

    public void onNewClusterState(String source, ClusterState newClusterState) {
        if (!lifecycle.started()) {
            return;
        }
        UpdateTask updateTask = new UpdateTask(source, newClusterState);
        threadPoolExecutor.execute(updateTask);
    }

    public void setInitialState(ClusterState initialState) {
        if (lifecycle.started()) {
            throw new IllegalStateException("Error: setting initial lastAppliedState after starting");
        }
        lastAppliedState.set(initialState);
    }

    public long currentVersion() {
        return lastAppliedState.get().version();
    }

    public void addListener(ClusterStateListener listener) {
        clusterStateListeners.add(listener);
    }

    public void removeListener(ClusterStateListener listener) {
        clusterStateListeners.remove(listener);
    }

    public void addApplier(ClusterStateApplier applier) {
        clusterStateAppliers.add(applier);
    }

    public void removeApplier(ClusterStateApplier applier) {
        clusterStateAppliers.remove(applier);
    }
}

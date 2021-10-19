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
package com.alibaba.maxgraph.coordinator.service;

import com.alibaba.maxgraph.common.ServerAssignment;
import com.alibaba.maxgraph.common.cluster.InstanceConfig;
import com.alibaba.maxgraph.common.cluster.management.ClusterApplierService;
import com.alibaba.maxgraph.common.cluster.management.ClusterState;
import com.alibaba.maxgraph.common.cluster.management.NodeInfos;
import com.alibaba.maxgraph.common.component.AbstractLifecycleComponent;
import com.alibaba.maxgraph.common.util.CommonUtil;
import com.alibaba.maxgraph.coordinator.LoggerStore;
import com.alibaba.maxgraph.coordinator.manager.PartitionManager;
import com.alibaba.maxgraph.proto.*;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.TextFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

public class MasterService extends AbstractLifecycleComponent {

    public final static String CLUSTER_CKP_PATH = "cluster";

    private ClusterApplierService clusterApplierService;
    private LoggerStore loggerStore;
    private PartitionManager partitionManager;

    private ScheduledExecutorService executor;

    private AtomicReference<ClusterState> committedState;

    private Map<NodeID, TimedNode> timedNodes = new ConcurrentHashMap<>();

    public MasterService(InstanceConfig settings, ClusterApplierService clusterApplierService,
                         LoggerStore loggerStore, PartitionManager partitionManager) {
        super(settings);
        this.clusterApplierService = clusterApplierService;
        this.loggerStore = loggerStore;
        this.partitionManager = partitionManager;

        this.committedState = new AtomicReference<>();
        this.committedState.set(ClusterState.emptyClusterState());
    }

    public void updateNodeState(NodeInfo nodeInfo, long timestamp, NodeStateProto nodeStateProto) {
        NodeID nodeId = nodeInfo.getNodeId();
        TimedNode oldNode = timedNodes.put(nodeId, new TimedNode(nodeInfo, timestamp, nodeStateProto));
        if (oldNode == null) {
            logger.info("New node connected: " + TextFormat.shortDebugString(nodeInfo));
        }
    }


    class TimedNode {
        NodeInfo nodeInfo;
        long timestamp;
        NodeStateProto nodeStateProto;

        public TimedNode(NodeInfo nodeInfo, long timestamp, NodeStateProto nodeStateProto) {
            this.nodeInfo = nodeInfo;
            this.timestamp = timestamp;
            this.nodeStateProto = nodeStateProto;
        }
    }

    @Override
    protected void doStart() {
        byte[] clusterStateCkpBytes = null;
        try {
            clusterStateCkpBytes = loggerStore.read(CLUSTER_CKP_PATH);
            if (clusterStateCkpBytes.length > 0) {
                ClusterState clusterState = ClusterState.fromProto(ClusterStateProto.parseFrom(clusterStateCkpBytes));
                // TODO refresh heartbeat timestamp
                this.committedState.set(clusterState);
                clusterApplierService.onNewClusterState("Coordinator recover", clusterState);
            }
        } catch (IOException e) {
            String readData = clusterStateCkpBytes == null ? "NULL" : new String(clusterStateCkpBytes);
            logger.warn("Recover from checkpoint failed. Checkpoint data: " + readData + ". Exception: " + e.getMessage());
        }

        executor = Executors.newSingleThreadScheduledExecutor(
                CommonUtil.createFactoryWithDefaultExceptionHandler("maxgraph-master-service", logger));
        // wait nodes connect after coordinator recover
        executor.scheduleWithFixedDelay(new UpdateClusterStateTask(), 5000, 5000, TimeUnit.MILLISECONDS);
    }

    class UpdateClusterStateTask implements Runnable {

        @Override
        public void run() {
            logger.info("Master service scheduling...");
            long aliveThreashold = System.currentTimeMillis() - 30000; // Timeout 30s

            ImmutableMap.Builder<NodeID, NodeInfo> nodesBuilder = ImmutableMap.builder();

            Map<Integer, PartitionInfo.Builder> partitionInfoBuilderMap = new HashMap<>();

            // Make cluster state
            for (Iterator<Map.Entry<NodeID, TimedNode>> it = timedNodes.entrySet().iterator(); it.hasNext();) {

                Map.Entry<NodeID, TimedNode> nodeEntry = it.next();
                TimedNode timedNode = nodeEntry.getValue();
                NodeID nodeID = nodeEntry.getKey();

                if (timedNode.timestamp < aliveThreashold) {
                    // nodes timeout monitor
                    logger.info("Remove timeout node " + TextFormat.shortDebugString(nodeID) + ". Last timestamp is: " +
                            timedNode.timestamp + ". Threshold is: " + aliveThreashold);
                    it.remove();
                    // timedNode.nodeInfo.getServerId()
                    continue;
                }
                // Maybe ip port change for same NodeID
                nodesBuilder.put(nodeEntry.getKey(), timedNode.nodeInfo);

                //
                if (nodeID.getRole() == RoleType.STORE_NODE) {
                    for (Map.Entry<Integer, StateList> entityStateEntry :
                            timedNode.nodeStateProto.getNodeStateMapMap().entrySet()) {
                        int partitionID = entityStateEntry.getKey();
                        PartitionInfo.Builder partitionInfoBuilder = partitionInfoBuilderMap.get(partitionID);
                        if (partitionInfoBuilder == null) {
                            partitionInfoBuilder = PartitionInfo.newBuilder();
                            partitionInfoBuilderMap.put(partitionID, partitionInfoBuilder);
                        }
                        partitionInfoBuilder.putShardInfos(nodeID.getId(),
                                ShardState.valueOf((int) entityStateEntry.getValue().getStates(0)));
                    }
                }
            }

            ImmutableMap.Builder<Integer, PartitionInfo> partitionInfoMapBuild = ImmutableMap.builder();
            for (Map.Entry<Integer, PartitionInfo.Builder> entry : partitionInfoBuilderMap.entrySet()) {
                partitionInfoMapBuild.put(entry.getKey(), entry.getValue().build());
            }

            ImmutableMap<Integer, PartitionInfo> newPartitionInfoMap = partitionInfoMapBuild.build();
            ImmutableMap<NodeID, NodeInfo> newNodes = nodesBuilder.build();

            Map<Integer, ServerAssignment> assignments = partitionManager.getAssignments();
            ImmutableMap.Builder<Integer, NodeStateProto> newIdealStateBuilder = ImmutableMap.builder();
            for (ServerAssignment assignment : assignments.values()) {
                NodeStateProto.Builder builder = NodeStateProto.newBuilder();
                for (Integer partition : assignment.getPartitions()) {
                    builder.putNodeStateMap(partition, StateList.newBuilder().addStates(ShardState.ONLINING_VALUE).build());
                }
                newIdealStateBuilder.put(assignment.getServerId(), builder.build());
            }
            ImmutableMap<Integer, NodeStateProto> newIdealState = newIdealStateBuilder.build();

            ClusterState currentState = committedState.get();
            NodeInfos nodes = currentState.nodes();
            ImmutableMap<Integer, PartitionInfo> currentPartitionInfoMap = currentState.getPartitionInfoMap();
            ImmutableMap<Integer, NodeStateProto> nodeExpectedStateMap = currentState.getNodeExpectedStateMap();

            if (nodes.getNodes().equals(newNodes) && newPartitionInfoMap.equals(currentPartitionInfoMap)
                    && nodeExpectedStateMap.equals(newIdealState)) {
                // No need update committedState
                return;
            }

            ClusterState newClusterState = new ClusterState(currentState.version() + 1,
                    new NodeInfos(newNodes), newPartitionInfoMap, newIdealState);
            logger.info("New cluster state: " + TextFormat.shortDebugString(newClusterState.toProto()));
            try {
                loggerStore.write(CLUSTER_CKP_PATH, newClusterState.toProto().toByteArray());
            } catch (IOException e) {
                logger.error("Checkpoint newClusterState fail.", e);
                return;
            }
            committedState.set(newClusterState);
            clusterApplierService.onNewClusterState("from Master", newClusterState);
        }
    }

    @Override
    protected void doStop() {
        if (executor != null) {
            executor.shutdown();
        }
    }

    @Override
    protected void doClose() throws IOException {

    }

}

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
package com.alibaba.maxgraph.coordinator.manager.runtime;

import com.alibaba.maxgraph.common.DataStatus;
import com.alibaba.maxgraph.common.cluster.management.ClusterApplierService;
import com.alibaba.maxgraph.common.cluster.management.ClusterChangedEvent;
import com.alibaba.maxgraph.common.cluster.management.ClusterStateListener;
import com.alibaba.maxgraph.coordinator.DefaultPartitionGroupStrategy;
import com.alibaba.maxgraph.coordinator.PartitionGroupStrategy;
import com.alibaba.maxgraph.coordinator.manager.ServerDataManager;
import com.alibaba.maxgraph.logging.LogEvents;
import com.alibaba.maxgraph.logging.Logging;
import com.alibaba.maxgraph.proto.*;
import com.google.protobuf.TextFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @Author: peaker.lgf
 * @Date: 2019-12-24 16:23
 **/
public class PegasusRuntimeManager implements ClusterStateListener, RuntimeManager {
    private static final Logger LOG = LoggerFactory.getLogger(PegasusRuntimeManager.class);

    // Instance graph name
    private final String graphName;
    // server role id
    private final int serverId;

    private Map<Integer, PegasusGroupInfo> groupsInfo;

    private ServerDataManager serverDataManager;
    private ClusterApplierService clusterApplierService;

    public PegasusRuntimeManager(ServerDataManager serverDataManager, ClusterApplierService clusterApplierService) {
        this.serverDataManager = serverDataManager;
        this.clusterApplierService = clusterApplierService;
        this.graphName = serverDataManager.instanceConfig.getGraphName();
        this.serverId = serverDataManager.instanceConfig.getServerId();
        this.groupsInfo = new HashMap<>();
    }

    @Override
    public void init() throws Exception {
        PartitionGroupStrategy partitionGroupStrategy = new DefaultPartitionGroupStrategy();
        List<List<Integer>> groupPartitions = partitionGroupStrategy.getGroupInfo(
                serverDataManager.partitionManager.getAssignments());
        LOG.info("Assignments info: " + serverDataManager.partitionManager.getAssignments().toString());
        LOG.info("GroupPartition info: " + groupPartitions.toString());

        for (int groupIndex = 0; groupIndex < groupPartitions.size(); groupIndex++) {
            PegasusGroupInfo pegasusGroupInfo = new PegasusGroupInfo(groupPartitions.get(groupIndex), 0,
                    GroupStatus.STARTING, this.graphName, this.serverId);
            this.groupsInfo.put(groupIndex, pegasusGroupInfo);
            Logging.runtime(this.graphName, RoleType.AM, this.serverId, LogEvents.RuntimeEvent.GROUP_RESTORE, groupIndex, 0, 0L, "Group " + groupIndex + " restarting...");
        }
    }

    @Override
    public synchronized void initRuntimeResp(ServerHBResp.Builder builder, DataStatus runtimeStatus) {
        int serverId = runtimeStatus.serverId;
        String serverIP = runtimeStatus.endpoint.getIp();
        int serverPort = runtimeStatus.runtimeHBReq.getRuntimePort();
        int storePort = runtimeStatus.endpoint.getPort();
        RuntimeHBReq.RuntimeStatus serverStatus = runtimeStatus.runtimeHBReq.getServerStatus();
        int workerNumPerProcess = runtimeStatus.getRuntimeHBReq().getWorkerNumPerProcess();
        List<Integer> processPartitionList = runtimeStatus.getRuntimeHBReq().getProcessPartitionListList();

        int groupId = getGroupID(serverId);
        if (groupId == -1) {
            LOG.error("Can't find group info of serverid: {}, serverip: {}", serverId, serverIP);
            throw new RuntimeException("Get group info error: serverId: " + serverId + " serverIp " + serverIP);
        }
        int workerId = getWorkerID(groupId, serverId);

        if (workerId == -1) {
            LOG.error("Can't find worker {}, {} from group: {}", serverId, serverIP, groupId);
            throw new RuntimeException(
                    "Get worker id error: groupId: " + groupId + " serverId: " + serverId + " serverIp: " + serverIP);
        }

        if(!serverStatus.equals(RuntimeHBReq.RuntimeStatus.RUNNING)) {
            LOG.info("worker {} in group {} is starting, ip: {}, port: {}.", workerId, groupId, serverIP, serverPort);
            Logging.runtime(graphName, RoleType.AM, serverId, LogEvents.RuntimeEvent.SERVER_STARTING, groupId, workerId, 0L, "Runtime server " + workerId + " is starting.");
        } else {
            LOG.info("worker {} in group {} is running, ip: {}, port: {}.", workerId, groupId, serverIP, serverPort);
            Logging.runtime(graphName, RoleType.AM, serverId, LogEvents.RuntimeEvent.SERVER_RUNNING, groupId, workerId, 0L, "Runtime server " + workerId + " is running.");
        }

        PegasusGroupInfo singlePegasusGroupInfo = groupsInfo.get(groupId);
        singlePegasusGroupInfo.updateNodeInfo(workerId, new NodeInfo(serverIP, serverPort, storePort, serverStatus, serverId));
        singlePegasusGroupInfo.updateTaskPartitionList(workerId, workerNumPerProcess, processPartitionList);

        RuntimeHBResp runtimeResp = buildRuntimeResp(singlePegasusGroupInfo, workerId, groupId);
        builder.setRuntimeResp(runtimeResp);
        groupsInfo.put(groupId, singlePegasusGroupInfo);
    }

    public void close() {
        this.clusterApplierService.removeListener(this);
    }

    @Override
    public Map<Integer, GroupStatus> getGroupsStatus() {
        Map<Integer, GroupStatus> result = new HashMap<>();
        for (Map.Entry<Integer, PegasusGroupInfo> entry : this.groupsInfo.entrySet()) {
            PegasusGroupInfo singlePegasusGroupInfo = entry.getValue();
            GroupStatus singleGroupStatus = singlePegasusGroupInfo.getGroupStatus();
            result.put(entry.getKey(), singleGroupStatus);
        }
        return result;
    }

    /**
     * Get group id
     *
     * @param serverId server node id
     * @return group id
     */
    private synchronized int getGroupID(int serverId) {
        //get groupId of the server

        int groupId = -1;
        for (Map.Entry<Integer, PegasusGroupInfo> groupInfo : groupsInfo.entrySet()) {
            Map<Integer, NodeInfo> nodes = groupInfo.getValue().nodes;
            for (Map.Entry<Integer, NodeInfo> node : nodes.entrySet()) {
                if (node.getValue().containsServerId(serverId)) {
                    groupId = groupInfo.getKey();
                    return groupId;
                }
            }
        }
        return groupId;
    }

    /**
     * Get worker id, which is the worker id in timely server
     *
     * @param groupId  group id
     * @param serverId server node id
     * @return
     */
    private synchronized int getWorkerID(int groupId, int serverId) {
        int workerId = -1;
        Map<Integer, NodeInfo> nodes = groupsInfo.get(groupId).nodes;
        for (Map.Entry<Integer, NodeInfo> node : nodes.entrySet()) {
            if (node.getValue().containsServerId(serverId)) {
                workerId = node.getKey();
                return workerId;
            }
        }
        return workerId;
    }

    private synchronized RuntimeHBResp buildRuntimeResp(PegasusGroupInfo singlePegasusGroupInfo, int workerId, int groupId) {
        // build the response to runtime
        RuntimeHBResp.Builder runtimeRespBuilder = RuntimeHBResp.newBuilder();
        runtimeRespBuilder.setWorkerId(workerId);
        runtimeRespBuilder.setGroupId(groupId);
        List<RuntimeAddressProto> addressProtoList = singlePegasusGroupInfo.getAvaliableAddressList();
        runtimeRespBuilder.addAllAddresses(addressProtoList);
        runtimeRespBuilder.addAllTaskPartitionList(singlePegasusGroupInfo.getProcessTaskPartitionList());

        LOG.info("Response to timely worker {} in group {}, version: {}, ip list: {} response: {}",
                workerId,
                groupId,
                singlePegasusGroupInfo.version,
                addressProtoList.toString(),
                TextFormat.printToString(runtimeRespBuilder));
        if(!singlePegasusGroupInfo.getGroupStatus().equals(GroupStatus.RUNNING)) {
            Logging.runtime(this.graphName, RoleType.AM, this.serverId, LogEvents.RuntimeEvent.GROUP_READY, groupId, workerId, singlePegasusGroupInfo.version,
                    "Response info to pegasus worker, info: version: " + singlePegasusGroupInfo.version + ", workerId: " + workerId + ", groupId: " + groupId + ", ip list: " + addressProtoList.toString());
        }
        return runtimeRespBuilder.build();
    }


    /**
     * Listen cluster change, which will lead to group change
     *
     * @param event
     */
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        Set<Integer> addNodeSet = new HashSet<>();
        Set<Integer> removeNodeSet = new HashSet<>();
        for (com.alibaba.maxgraph.proto.NodeInfo removedNode : event.nodesDelta().removedNodes()) {
            if (removedNode.getNodeId().getRole() == RoleType.STORE_NODE) {
                int groupId = this.getGroupID(removedNode.getNodeId().getId() + 1);
                LOG.info("removedNode.getNodeId().getId(): {}, group id: {}", removedNode.getNodeId().getId(), groupId);
                removeNodeSet.add(groupId);
            }
        }
        for (com.alibaba.maxgraph.proto.NodeInfo addedNode : event.nodesDelta().addedNodes()) {
            if (addedNode.getNodeId().getRole() == RoleType.STORE_NODE) {
                int groupId = this.getGroupID(addedNode.getNodeId().getId() + 1);
                LOG.info("addedNode.getNodeId().getId(): {}", addedNode.getNodeId().getId());
                addNodeSet.add(groupId);
            }
        }

        // now the servers in every group will not change, so we only need to process the removeNodeSet
        for (Integer groupId : removeNodeSet) {
            PegasusGroupInfo pegasusGroupInfo = groupsInfo.get(groupId);
            if (pegasusGroupInfo.groupStatus.equals(GroupStatus.RUNNING)) {
                pegasusGroupInfo.invalidGroup();
                LOG.info("Group {} is changed, set group status to down.", groupId);

                groupsInfo.put(groupId, pegasusGroupInfo);
                Logging.runtime(this.graphName, RoleType.AM, this.serverId, LogEvents.RuntimeEvent.SERVER_STARTING, groupId, -1, 0L,
                        "Set group status to DOWN, caused by at least one node is removed.");

            }
        }
    }

}

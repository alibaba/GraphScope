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
package com.alibaba.maxgraph.common;

import com.alibaba.maxgraph.common.client.WorkerInfo;
import com.alibaba.maxgraph.proto.*;
import com.alibaba.maxgraph.proto.RoleType;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class InstanceStatus {

    // serverId -> ServerAssignment
    public final Map<Integer, ServerAssignment> assignments = Maps.newConcurrentMap();

    // partitionId -> serverId
    public final Map<Integer, Integer> partition2Server = Maps.newConcurrentMap();

    // roleType -> WorkInfo
    public final HashMultimap<RoleType, WorkerInfo> workerInfoMap = HashMultimap.create();

    // serverId -> WorkInfo
    public final Map<Integer, WorkerInfo> server2WorkerInfo = Maps.newConcurrentMap();

    public final InstanceInfoProto.Status status;

    public InstanceStatus(InstanceInfoProto instanceInfoProto) {
        Map<Integer, PartitionProtos> assignmentMapMap = instanceInfoProto.getAssignmentMap();

        for (Map.Entry<Integer, PartitionProtos> targetPartition : assignmentMapMap.entrySet()) {
            List<Integer> partitionIdList = targetPartition.getValue().getPartitionIdList();
            assignments.put(targetPartition.getKey(), new ServerAssignment(targetPartition.getKey(), partitionIdList));
        }

        for (ServerAssignment serverAssignment : assignments.values()) {
            for (Integer integer : serverAssignment.getPartitions()) {
                partition2Server.put(integer, serverAssignment.getServerId());
            }
        }

        Map<Integer, WorkerInfoProtos> workerInfoProtos = instanceInfoProto.getWorkerInfosMap();

        for (Map.Entry<Integer, WorkerInfoProtos> entry : workerInfoProtos.entrySet()) {
            WorkerInfoProtos value = entry.getValue();
            for (WorkerInfoProto workerInfoProto : value.getInfosList()) {
                WorkerInfo workerInfo = WorkerInfo.fromProto(workerInfoProto);
                workerInfoMap.put(RoleType.forNumber(entry.getKey()), workerInfo);
                server2WorkerInfo.put(workerInfoProto.getId(), workerInfo);
            }
        }

        this.status = instanceInfoProto.getStatus();
    }

    public WorkerInfo getWorkerInfoById(Integer workerId) {
        return server2WorkerInfo.get(workerId);
    }

    public ServerAssignment readServerAssignment(Integer serverId) {
        return assignments.get(serverId);
    }

    public Set<WorkerInfo> getWorkInfo(RoleType roleType) {
        return Sets.newHashSet(workerInfoMap.get(roleType));
    }

    @Override
    public String toString() {
        return "InstanceStatus{" +
                "assignments=" + assignments +
                ", partition2Server=" + partition2Server +
                ", workerInfoMap=" + workerInfoMap +
                ", server2WorkerInfo=" + server2WorkerInfo +
                ", status=" + status +
                '}';
    }
}

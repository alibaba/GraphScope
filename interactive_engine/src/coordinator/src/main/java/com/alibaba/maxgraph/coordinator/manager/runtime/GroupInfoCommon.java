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

import com.alibaba.maxgraph.proto.RuntimeHBReq;
import com.alibaba.maxgraph.proto.RuntimeTaskPartitionProto;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @Author: peaker.lgf
 * @Date: 2020-01-19 11:29
 **/
public class GroupInfoCommon {
    private static final Logger LOG = LoggerFactory.getLogger(GroupInfoCommon.class);
    protected final String graph;
    protected final int serverId;
    protected long version;
    protected GroupStatus groupStatus;       // group status
    protected Map<Integer, NodeInfo> nodes;   // worker_id, NodeInfo

    protected int hbCount;      // count number of the group receiving
    protected Map<Integer, Map<Integer, List<Integer>>> processTaskPartitionList = Maps.newHashMap();    // processor index -> task global index -> partition list to the task

    /**
     * Create GraphInfo in starting status.
     *
     * @param serverList serverid list, server id is the server in all cluster
     * @param version    group version
     */
    public GroupInfoCommon(List<Integer> serverList, long version, String graph, int serverId) {
        this(serverList, version, GroupStatus.STARTING, graph, serverId);
    }

    /**
     * @param serverList  server_id list, server id is the server in all cluster
     * @param version     group version
     * @param groupStatus initialize group status
     */
    public GroupInfoCommon(List<Integer> serverList, long version, GroupStatus groupStatus, String graph, int serverId) {
        this.nodes = new HashMap<>();
        for (int i = 0; i < serverList.size(); i++) {
            this.nodes.put(i, new NodeInfo("", 0, 0, RuntimeHBReq.RuntimeStatus.DOWN, serverList.get(i)));
        }
        this.version = version;
        this.groupStatus = groupStatus;
        this.hbCount = 0;
        this.graph = graph;
        this.serverId = serverId;
    }

    public void updateTaskPartitionList(int workerId, int workerNumPerProcess, List<Integer> processPartitionList) {
        int index = workerNumPerProcess * workerId;
        Map<Integer, List<Integer>> taskPartitionList = this.processTaskPartitionList.computeIfAbsent(workerId, k -> Maps.newHashMap());
        LinkedList<Integer> partitionList = Lists.newLinkedList(processPartitionList);
        int taskPartitionNum = partitionList.size() / workerNumPerProcess;
        for (int i = 0; i < workerNumPerProcess - 1; i++) {
            int taskIndex = index + i;
            List<Integer> currTaskPartitionList = Lists.newArrayList();
            for (int k = 0; k < taskPartitionNum; k++) {
                currTaskPartitionList.add(partitionList.pollFirst());
            }
            taskPartitionList.put(taskIndex, currTaskPartitionList);
        }
        taskPartitionList.put(index + workerNumPerProcess - 1, partitionList);
    }

    public List<RuntimeTaskPartitionProto> getProcessTaskPartitionList() {
        List<RuntimeTaskPartitionProto> taskPartitionProtoList = Lists.newArrayList();
        for (Map.Entry<Integer, Map<Integer, List<Integer>>> entry : this.processTaskPartitionList.entrySet()) {
            int processIndex = entry.getKey();
            for (Map.Entry<Integer, List<Integer>> taskEntry : entry.getValue().entrySet()) {
                taskPartitionProtoList.add(RuntimeTaskPartitionProto.newBuilder()
                        .setTaskIndex(taskEntry.getKey())
                        .setProcessIndex(processIndex)
                        .addAllPartitionList(taskEntry.getValue()).build());
            }
        }

        return taskPartitionProtoList;
    }
}

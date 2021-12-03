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
package com.alibaba.maxgraph.coordinator.manager;

import com.alibaba.maxgraph.common.PartitionAssigner;
import com.alibaba.maxgraph.common.ServerAssignment;
import com.alibaba.maxgraph.common.TimelyStylePartitionAssigner;
import com.alibaba.maxgraph.common.cluster.InstanceConfig;
import com.alibaba.maxgraph.common.lock.ExtendedRWLock;
import com.alibaba.maxgraph.common.lock.LockWrapper;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class PartitionManager {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionManager.class);

    public static final int DIMENSION_PARTITION_NUMBER = Integer.MAX_VALUE;

    // serverId -> ServerAssignment
    public final Map<Integer, ServerAssignment> assignments = Maps.newConcurrentMap();

    private final PartitionAssigner assigner = new TimelyStylePartitionAssigner();
    private final ExtendedRWLock partitionLock = new ExtendedRWLock();

    private ServerDataManager serverDataManager;
    private InstanceConfig instanceConfig;
    private int replicaCount;


    public PartitionManager(ServerDataManager serverDataManager) {
        this.serverDataManager = serverDataManager;
        this.instanceConfig = serverDataManager.instanceConfig;
        this.replicaCount = instanceConfig.getReplicaCount();
    }

    void start() throws Exception {
        LOG.info("ServerDataManager created, assigner: {}", this.getAssignerName());
        for (int i = 1; i < instanceConfig.getResourceExecutorCount() + 1; i++) {
            getServerAssignment(i);
        }
    }

    public String getAssignerName() {
        return this.assigner.getClass().getCanonicalName();
    }

    public ServerAssignment readServerAssignment(Integer serverId) {
        return assignments.get(serverId);
    }

    public ServerAssignment getServerAssignment(Integer serverId) throws Exception {
        ServerAssignment serverAssignment = assignments.get(serverId);
        if (serverAssignment == null) {
            try (LockWrapper ignore = partitionLock.openWriteLock()) {
                serverAssignment = assignments.get(serverId);
                if (serverAssignment == null) {
                    LOG.info("assigner:{}, instance config:{}", assigner, instanceConfig);
                    List<Integer> assignment = assigner.getAssignment(instanceConfig.getPartitionNum(),
                            instanceConfig.getResourceExecutorCount(), serverId, replicaCount);
                    serverAssignment = new ServerAssignment(serverId, assignment);

                    LOG.info("assign partition to server {}: {}", serverId, assignment);

                    Map<Integer, ServerAssignment> temp = Maps.newHashMap(assignments);
                    temp.put(serverId, serverAssignment);
                    serverDataManager.namingProxy.persistentServerAssignment(temp);
                    assignments.put(serverId, serverAssignment);
                }
                return serverAssignment;
            }
        }

        return assignments.get(serverId);
    }

    public Map<Integer, ServerAssignment> getAssignments() {
        return assignments;
    }
}

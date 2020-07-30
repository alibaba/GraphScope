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
package com.alibaba.maxgraph.common.cluster;

import com.alibaba.maxgraph.proto.RoleType;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;

import java.util.Map;

public class InstanceResourceCalculate {

    private final static int MIN_CPU_NUM = 2;
    private final static int MIN_EXECUTOR_CPU_NUM = 4;
    private final static int MIN_EXECUTOR_MEM_G_SIZE = 4;

    private final static int MAX_EXECUTOR_CPU_NUM = 56; // H41
    private final static int MAX_EXECUTOR_MEM_G_SIZE = 450; // H41

    // Different Resource Model will defined in future like Normal.
    private double executorMemoryCpuRatio = 8; // H41 is 480G VS 60Core
    private double executorCpuOversoldRatio = 0.2;
    private double executorMemoryODPSDataRatio = 4;

    public InstanceResourceCalculate() {

    }

    public InstanceResourceCalculate(double executorMemoryCpuRatio, double executorCpuOversoldRatio, double executorMemoryODPSDataRatio) {
        this.executorMemoryCpuRatio = executorMemoryCpuRatio;
        this.executorCpuOversoldRatio = executorCpuOversoldRatio;
        this.executorMemoryODPSDataRatio = executorMemoryODPSDataRatio;
    }

    /**
     * current only use odps size params to calculate all resources
     *
     * @param params
     * @return
     */
    public Map<RoleType, MaxGraphRoleConfiguration> calMaxGraphResource(InstanceBusinessParams params) {
        Map<RoleType, MaxGraphRoleConfiguration> resourceMap = Maps.newHashMap();
        MaxGraphRoleConfiguration executorNodeConfig = getExecutorNodeConfig(params);
        resourceMap.put(RoleType.ID_SERVICE, getIdNodeConfig());
        resourceMap.put(RoleType.EXECUTOR, executorNodeConfig);
        resourceMap.put(RoleType.FRONTEND, getFrontNodeConfig(executorNodeConfig));
        resourceMap.put(RoleType.INGEST_NODE, getIdNodeConfig());
        resourceMap.put(RoleType.COORDINATOR, new MaxGraphRoleConfiguration(1, 4, 1));
        resourceMap.put(RoleType.AM, new MaxGraphRoleConfiguration(1, 4, 1));
        return resourceMap;
    }

    public MaxGraphRoleConfiguration getIdNodeConfig() {
        return new MaxGraphRoleConfiguration(1, 1, 1);
    }

    public MaxGraphRoleConfiguration getFrontNodeConfig(MaxGraphRoleConfiguration executorResource) {
        int memorySize = Math.min(16, Math.max(4, executorResource.memorySize / 4));
        return new MaxGraphRoleConfiguration(MIN_CPU_NUM, memorySize, 2);
    }

    public MaxGraphRoleConfiguration getIngestNodeConfig() {
        return new MaxGraphRoleConfiguration(MIN_CPU_NUM, 4, 2);
    }

    public MaxGraphRoleConfiguration getExecutorNodeConfig(InstanceBusinessParams instanceBusinessParams) {
        long totalODPSSize = instanceBusinessParams.vertexODPSTableSize + instanceBusinessParams.edgeODPSTableSize;
        int nodeNum = calExecutorNodeNum(totalODPSSize);
        int memoryNum = calExecutorMemoryGSize(totalODPSSize, nodeNum);
        int cpuNum = Math.min(MAX_EXECUTOR_CPU_NUM, (int) Math.ceil(Math.max(MIN_EXECUTOR_CPU_NUM, (memoryNum / executorMemoryCpuRatio)) * (1 +
                executorCpuOversoldRatio)));

        MaxGraphRoleConfiguration maxGraphRoleConfiguration = new MaxGraphRoleConfiguration(cpuNum, memoryNum, nodeNum);

        int downloadThreadNum = Math.max(2, (int)Math.ceil(cpuNum / 3 * 1.2));
        int loadThreadNum = Math.max(1, downloadThreadNum / 2);
        int queryWorkerNum = Math.max(1, cpuNum / 2);
        int grpcThreadNum = queryWorkerNum * 2;

        maxGraphRoleConfiguration.getRoleParams().put(InstanceConfig.EXECUTOR_DOWNLOAD_DATA_THREAD_NUM,
                "" + downloadThreadNum);

        maxGraphRoleConfiguration.getRoleParams().put(InstanceConfig.EXECUTOR_LOAD_DATA_THREAD_NUM,
                "" + loadThreadNum);

        maxGraphRoleConfiguration.getRoleParams().put(InstanceConfig.TIMELY_WORKER_PER_PROCESS, "" + queryWorkerNum);

        maxGraphRoleConfiguration.getRoleParams().put(InstanceConfig.EXECUTOR_GRPC_THREAD_COUNT, "" + grpcThreadNum);

        return maxGraphRoleConfiguration;
    }

    private int calExecutorMemoryGSize(long totalODPSSize, int nodeNum) {
        return Math.min(MAX_EXECUTOR_MEM_G_SIZE, Math.max(MIN_EXECUTOR_MEM_G_SIZE, (int) (totalODPSSize / 1024 / 1024 / 1024 * executorMemoryODPSDataRatio) / nodeNum));
    }

    private static int calExecutorNodeNum(long totalODPSSize) {
        long size = totalODPSSize / 1024 / 1024 / 1024; //GB

        if (size < 10) {
            return 2;
        } else if (size < 50) {
            return 4;
        } else if (size < 100) {
            return 8;
        } else if (size < 500) {
            return 16;
        } else {
            return 32;
        }
    }

    public static int calPartitionNum(int executorNum) {
        return (int) Math.min(1024, Math.max(2, executorNum * Math.pow(2, Math.log(executorNum) / Math.log(2) + 1)));
    }

    public static class InstanceBusinessParams {

        public final long vertexNum;
        public final long edgeNum;

        public final long vertexODPSTableSize;
        public final long edgeODPSTableSize;

        public final long queryQPS;
        public final long writeQPS;

        @JsonCreator
        public InstanceBusinessParams(@JsonProperty("vertex_num") long vertexNum, @JsonProperty("edge_num") long edgeNum,
                                      @JsonProperty("vertex_size") long vertexODPSTableSize, @JsonProperty("edge_size") long edgeODPSTableSize,
                                      @JsonProperty("query_qps") long queryQPS, @JsonProperty("write_qps") long writeQPS) {
            this.vertexNum = vertexNum;
            this.edgeNum = edgeNum;
            this.vertexODPSTableSize = vertexODPSTableSize;
            this.edgeODPSTableSize = edgeODPSTableSize;
            this.queryQPS = queryQPS;
            this.writeQPS = writeQPS;
        }
    }

    public static class InstanceBusinessInfo extends InstanceBusinessParams {

        public final String clusterId;
        public final String graphName;
        public final String graphDesc;
        public final String creator;
        public final String ownerId;
        public final String ownerName;
        public final String graphVersion;

        @JsonCreator
        public InstanceBusinessInfo(@JsonProperty("vertex_num") long vertexNum, @JsonProperty("edge_num") long edgeNum,
                                    @JsonProperty("vertex_size") long vertexODPSTableSize, @JsonProperty("edge_size") long edgeODPSTableSize,
                                    @JsonProperty("query_qps") long queryQPS, @JsonProperty("write_qps") long writeQPS,
                                    @JsonProperty("cluster_id") String clusterId,
                                    @JsonProperty("graph_name") String graphName,
                                    @JsonProperty("graph_desc") String graphDesc,
                                    @JsonProperty("graph_version") String graphVersion, @JsonProperty("graph_creator") String creator,
                                    @JsonProperty("graph_owner_id") String ownerId, @JsonProperty("graph_owner_name") String ownerName) {
            super(vertexNum, edgeNum, vertexODPSTableSize, edgeODPSTableSize, queryQPS, writeQPS);
            this.clusterId = clusterId;
            this.graphName = graphName;
            this.graphDesc = graphDesc;
            this.creator = creator;
            this.ownerId = ownerId;
            this.ownerName = ownerName;
            this.graphVersion = graphVersion;
        }
    }

    public static class MaxGraphRoleConfiguration {
        private int cpuNum;
        private int memorySize; // M
        private int roleNum;

        private Map<String, String> roleParams = Maps.newHashMap();

        public MaxGraphRoleConfiguration(int cpuNum, int memorySize, int roleNum) {
            this.cpuNum = cpuNum;
            this.memorySize = memorySize;
            this.roleNum = roleNum;
        }

        public Map<String, String> getRoleParams() {
            return roleParams;
        }

        public int getCpuNum() {
            return cpuNum;
        }

        public int getMemorySize() {
            return memorySize;
        }

        public int getRoleNum() {
            return roleNum;
        }
    }
}

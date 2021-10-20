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

import com.alibaba.maxgraph.proto.ClusterStateProto;
import com.alibaba.maxgraph.proto.NodeStateProto;
import com.alibaba.maxgraph.proto.PartitionInfo;
import com.alibaba.maxgraph.proto.ShardState;
import com.google.common.collect.ImmutableMap;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


public class ClusterState {

    private long version;
    private NodeInfos nodes;
    private ImmutableMap<Integer, PartitionInfo> partitionInfoMap;  // partitionID: {nodeID: STATE}
    private ImmutableMap<Integer, NodeStateProto> nodeExpectedStateMap; // nodeID : { partitionID : STATE }

    public ClusterState(long version, NodeInfos nodes, Map<Integer, PartitionInfo> partitionInfoMap,
                        Map<Integer, NodeStateProto> expectedStateMap) {
        this.version = version;
        this.nodes = nodes;
        this.partitionInfoMap = ImmutableMap.copyOf(partitionInfoMap);
        this.nodeExpectedStateMap = ImmutableMap.copyOf(expectedStateMap);
    }

    public NodeInfos nodes() {
        return this.nodes;
    }

    public NodeStateProto expectedState(int serverID) {
        return nodeExpectedStateMap.get(serverID);
    }

    public ImmutableMap<Integer, NodeStateProto> getNodeExpectedStateMap() {
        return this.nodeExpectedStateMap;
    }

    public Map<Integer, ShardState> getNodeShards(int nodeID) {
        Map<Integer, ShardState> shards = new HashMap<>();
        for (Map.Entry<Integer, PartitionInfo> partitionInfoEntry : partitionInfoMap.entrySet()) {
            ShardState shardState = partitionInfoEntry.getValue().getShardInfosMap().get(nodeID);
            if (shardState != null) {
                shards.put(partitionInfoEntry.getKey(), shardState);
            }
        }
        return shards;
    }

    public long version() {
        return version;
    }

    public ImmutableMap<Integer, PartitionInfo> getPartitionInfoMap() {
        return this.partitionInfoMap;
    }

    public static ClusterState fromProto(ClusterStateProto proto) {
        return new ClusterState(proto.getVersion(),
                new NodeInfos(proto.getNodesList()),
                proto.getPartitionsMap(),
                proto.getExpectedStateMapMap());
    }

    public ClusterStateProto toProto() {
        return ClusterStateProto.newBuilder()
                .setVersion(version)
                .putAllPartitions(partitionInfoMap)
                .addAllNodes(nodes)
                .putAllExpectedStateMap(nodeExpectedStateMap)
                .build();
    }

    public static ClusterState emptyClusterState() {
        return new ClusterState(0,
                        new NodeInfos(Collections.emptyList()),
                        Collections.emptyMap(),
                        Collections.emptyMap()
                );
    }

}

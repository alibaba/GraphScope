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

import com.alibaba.maxgraph.proto.NodeID;
import com.alibaba.maxgraph.proto.NodeInfo;
import com.google.common.collect.ImmutableMap;

import java.util.*;

public class NodeInfos implements Iterable<NodeInfo> {

    private ImmutableMap<NodeID, NodeInfo> nodes;

    public NodeInfos(List<NodeInfo> nodeInfos) {
        ImmutableMap.Builder<NodeID, NodeInfo> builder = ImmutableMap.builder();
        for (NodeInfo nodeInfo : nodeInfos) {
            builder.put(nodeInfo.getNodeId(), nodeInfo);
        }
        this.nodes = builder.build();
    }

    public NodeInfos(ImmutableMap<NodeID, NodeInfo> nodeInfoMap) {
        this.nodes = nodeInfoMap;
    }

    public ImmutableMap<NodeID, NodeInfo> getNodes() {
        return nodes;
    }

    public Delta delta(NodeInfos other) {
        List<NodeInfo> removed = new ArrayList<>();
        List<NodeInfo> added = new ArrayList<>();
        for (NodeInfo node : other) {
            if (!this.nodeExists(node)) {
                removed.add(node);
            }
        }
        for (NodeInfo node : this) {
            if (!other.nodeExists(node)) {
                added.add(node);
            }
        }
        return new Delta(added, removed);
    }

    public boolean nodeExists(NodeInfo node) {
        NodeInfo existing = nodes.get(node.getNodeId());
        return existing != null && existing.equals(node);
    }

    @Override
    public Iterator<NodeInfo> iterator() {
        return nodes.values().iterator();
    }

    public static class Delta {
        private List<NodeInfo> added;
        private List<NodeInfo> removed;

        public Delta(List<NodeInfo> added, List<NodeInfo> removed) {
            this.added = added;
            this.removed = removed;
        }

        public boolean added() {
            return !added.isEmpty();
        }

        public List<NodeInfo> addedNodes() {
            return added;
        }

        public boolean removed() {
            return !removed.isEmpty();
        }

        public List<NodeInfo> removedNodes() {
            return removed;
        }

    }

}

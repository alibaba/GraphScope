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

public class ClusterChangedEvent {

    private final ClusterState state;
    private final ClusterState previousState;
    private final String source;
    private final NodeInfos.Delta nodesDelta;

    public ClusterChangedEvent(String source, ClusterState state, ClusterState previousState) {
        this.source = source;
        this.state = state;
        this.previousState = previousState;
        this.nodesDelta = state.nodes().delta(previousState.nodes());
    }

    public String source() {
        return source;
    }

    public ClusterState state() {
        return state;
    }

    public ClusterState previousState() {
        return previousState;
    }

    public NodeInfos.Delta nodesDelta() {
        return nodesDelta;
    }

    public boolean nodesAdded() {
        return nodesDelta.added();
    }

    public boolean nodesRemoved() {
        return nodesDelta.removed();
    }


}

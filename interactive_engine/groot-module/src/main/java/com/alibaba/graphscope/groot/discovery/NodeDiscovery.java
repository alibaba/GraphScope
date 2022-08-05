/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.groot.discovery;

import com.alibaba.maxgraph.common.RoleType;

import java.util.Map;

public interface NodeDiscovery {

    void start();

    void stop();

    void addListener(Listener listener);

    void removeListener(Listener listener);

    MaxGraphNode getLocalNode();

    interface Listener {
        void nodesJoin(RoleType role, Map<Integer, MaxGraphNode> nodes);

        void nodesLeft(RoleType role, Map<Integer, MaxGraphNode> nodes);
    }
}

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

import com.google.common.base.Preconditions;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author lvshuang.xjs@alibaba-inc.com
 * @create 2018-06-12 下午8:03
 **/

public class ServerAssignment {

    private int serverId;
    private Set<Integer> partitions = new HashSet<>();

    public ServerAssignment(int serverId) {
        this.serverId = serverId;
    }

    public ServerAssignment(int serverId, Collection<Integer> partitions) {
        this.serverId = serverId;
        this.partitions.addAll(partitions);
    }

    public void addPartition(Integer partitionId) {
        Preconditions.checkState(!partitions.contains(partitionId));
        partitions.add(partitionId);
    }

    public int getServerId() {
        return serverId;
    }

    public Set<Integer> getPartitions() {
        return partitions;
    }

    public void addPartitions(List<Integer> partitions) {
        this.partitions.addAll(partitions);
    }
}

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

import com.alibaba.maxgraph.common.util.InstanceParamCheckUtil;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

/**
 * Partition in timely-dataflow's way.
 * Partitions must be assigned equally on workers. Maybe handle non-equals case in the future.
 *
 * @author xiafei.qiuxf
 * @date 2018/6/19
 */
public class TimelyStylePartitionAssigner implements PartitionAssigner {

    @Nonnull
    @Override
    public List<Integer> getAssignment(int totalPartition, int totalWorker, int serverId, int replicaCount) {
        InstanceParamCheckUtil.checkPartition(totalPartition, totalWorker, replicaCount);

        int nodesPerGroup = totalWorker / replicaCount;
        int partPerWorker = totalPartition / nodesPerGroup;
        int workIdxOfGroup = (serverId - 1) % nodesPerGroup;

        List<Integer> partitions = new ArrayList<>(partPerWorker);

        int start = partPerWorker * workIdxOfGroup;
        for (int i = 0; i < partPerWorker && start < totalPartition; i++) {
            partitions.add(start);
            start += 1;
        }
        return partitions;
    }
}

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
package com.alibaba.maxgraph.common.util;

import com.google.common.base.Preconditions;

public class InstanceParamCheckUtil {

    public static void  checkPartition(int totalPartition, int totalWorker, int replicaCount) {
        Preconditions.checkArgument(totalWorker % replicaCount == 0,
                "Invalid total worker number and replica count for TimelyStylePartitionAssigner: %s % %s != 0",
                totalPartition, totalWorker);

        Preconditions.checkArgument(totalPartition % (totalWorker / replicaCount) == 0,
                "Invalid partition number and total worker number for TimelyStylePartitionAssigner: %s % %s != 0",
                totalPartition, totalWorker);
    }
}

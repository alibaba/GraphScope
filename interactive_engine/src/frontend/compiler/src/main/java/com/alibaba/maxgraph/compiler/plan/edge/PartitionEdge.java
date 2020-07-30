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
package com.alibaba.maxgraph.compiler.plan.edge;

public class PartitionEdge implements Comparable<PartitionEdge> {
    private final int index;
    private PartitionType partitionType;
    private ShuffleType shuffleType;

    public PartitionEdge(PartitionType partitionType) {
        this(partitionType, 0);
    }

    public PartitionEdge(PartitionType partitionType, int index) {
        this.index = index;
        this.partitionType = partitionType;
    }

    public int getIndex() {
        return index;
    }

    public PartitionType getPartitionType() {
        return partitionType;
    }

    public void setPartitionType(PartitionType partitionType) {
        this.partitionType = partitionType;
    }

    public ShuffleType getShuffleType() {
        return shuffleType;
    }

    public void setShuffleType(ShuffleType shuffleType) {
        this.shuffleType = shuffleType;
    }

    @Override
    public int compareTo(PartitionEdge that) {
        if (index < that.index) {
            return -1;
        } else if (index > that.index) {
            return 1;
        } else {
            return 0;
        }
    }
}

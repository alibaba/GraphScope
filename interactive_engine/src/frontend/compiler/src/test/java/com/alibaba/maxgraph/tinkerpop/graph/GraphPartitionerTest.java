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
package com.alibaba.maxgraph.tinkerpop.graph;

import com.alibaba.maxgraph.structure.DefaultGraphPartitioner;
import com.alibaba.maxgraph.structure.GraphPartitioner;
import org.junit.Assert;
import org.junit.Test;

public class GraphPartitionerTest {
    @Test
    public void testDefaultGraphPartitioner() {
        for (int partitionNum = 1; partitionNum <= 1024; partitionNum *= 2) {
            GraphPartitioner partitioner = new DefaultGraphPartitioner(partitionNum);
            for (long i = -partitionNum; i <= partitionNum; ++i) {
                Assert.assertEquals(partitioner.getPartition(i), (i % partitionNum + partitionNum) % partitionNum);
            }
        }
    }
}

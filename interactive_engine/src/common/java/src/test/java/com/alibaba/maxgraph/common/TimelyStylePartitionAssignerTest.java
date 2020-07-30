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

import org.junit.Test;

import java.util.Arrays;

import static org.testng.AssertJUnit.assertEquals;

/**
 * @author xiafei.qiuxf
 * @date 2018/6/19
 */
public class TimelyStylePartitionAssignerTest {

    @Test
    public void getAssignment() {
        TimelyStylePartitionAssigner assigner = new TimelyStylePartitionAssigner();

        assertEquals(assigner.getAssignment(16, 4, 1, 1), Arrays.asList(0, 1, 2, 3));
        assertEquals(assigner.getAssignment(16, 4, 2, 1), Arrays.asList(4, 5, 6, 7));
        assertEquals(assigner.getAssignment(16, 4, 3, 1), Arrays.asList(8, 9, 10, 11));
        assertEquals(assigner.getAssignment(16, 4, 4, 1), Arrays.asList(12, 13, 14, 15));

        assertEquals(assigner.getAssignment(16, 4, 1, 2),
                Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7));
        assertEquals(assigner.getAssignment(16, 4, 2, 2),
                Arrays.asList(8, 9, 10, 11, 12, 13, 14, 15));
        assertEquals(assigner.getAssignment(16, 4, 3, 2),
                Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7));
        assertEquals(assigner.getAssignment(16, 4, 4, 2),
                Arrays.asList(8, 9, 10, 11, 12, 13, 14, 15));

        assertEquals(assigner.getAssignment(2, 4, 1, 2),
                Arrays.asList(0));

        assertEquals(assigner.getAssignment(2, 4, 2, 2),
                Arrays.asList(1));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidPartNum() {
        TimelyStylePartitionAssigner assigner = new TimelyStylePartitionAssigner();

        assigner.getAssignment(18, 4, 1, 1);
    }
}

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
package com.alibaba.maxgraph.compiler.operator;

import org.junit.Test;

public class EdgeOperatorTest extends AbstractOperatorTest {
    public EdgeOperatorTest() throws Exception {
    }

    @Test
    public void testEdgeCase() {
        executeTreeQuery(g.E().inV());
    }

    @Test
    public void testEdgeLabelIdCase() {
        executeTreeQuery(g.E(1L).hasLabel("person_knows_person").outV());
    }

    @Test
    public void testEdgeWeightFilterCase() {
        executeTreeQuery(g.E().hasLabel("person_knows_person").has("weight", 1.0).outV());
    }

    @Test
    public void testEdgeValueMapCase() {
        executeTreeQuery(g.E().valueMap());
    }
}

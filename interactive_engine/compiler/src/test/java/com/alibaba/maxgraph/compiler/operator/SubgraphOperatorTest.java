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

import com.alibaba.maxgraph.tinkerpop.traversal.DefaultMaxGraphTraversal;
import org.junit.Test;

import java.io.IOException;

public class SubgraphOperatorTest extends AbstractOperatorTest {

    public SubgraphOperatorTest() throws IOException {
    }

    @Test
    public void testSubgraphSourceVineyard() {
        DefaultMaxGraphTraversal subgraph = (DefaultMaxGraphTraversal) g.E().hasLabel("person_knows_person").subgraph("person_graph");
        executeTreeQuery(subgraph.outputVineyard("vineyard"));
    }

}

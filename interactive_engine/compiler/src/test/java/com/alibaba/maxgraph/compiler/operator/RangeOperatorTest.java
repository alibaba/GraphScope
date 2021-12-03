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

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Test;

public class RangeOperatorTest extends AbstractOperatorTest {

    public RangeOperatorTest() throws Exception {
    }

    @Test
    public void testRangeInVCase() {
        GraphTraversal traversal = g.V().outE().inV().limit(10).path();
        executeTreeQuery(traversal);
    }

    @Test
    public void testRangeInVOrderEdgeCase() {
        GraphTraversal traversal = g.V().outE().order().by(T.id).inV().limit(10).path();
        executeTreeQuery(traversal);
    }

    @Test
    public void testRangeInVOrderEdgePropCase() {
        GraphTraversal traversal = g.V().outE().order().by("weight").inV().limit(10).path();
        executeTreeQuery(traversal);
    }

    @Test
    public void testRangeInVFilterCase() {
        GraphTraversal traversal = g.V().outE().inV().has("age", P.gt(30)).limit(10).path();
        executeTreeQuery(traversal);
    }

    @Test
    public void testRangeOutVCase() {
        GraphTraversal traversal = g.V().inE().outV().range(10, 20).path();
        executeTreeQuery(traversal);
    }

    @Test
    public void testRangeOutVFilterCase() {
        GraphTraversal traversal = g.V().inE().outV().has("age", P.gt(30)).limit(10).path();
        executeTreeQuery(traversal);
    }

    @Test
    public void testRangeOtherVCase() {
        GraphTraversal traversal = g.V().bothE().otherV().limit(10).path();
        executeTreeQuery(traversal);
    }

    @Test
    public void testRangeOtherVFilterCase() {
        GraphTraversal traversal = g.V().bothE().otherV().has("age", P.gt(30)).limit(10).path();
        executeTreeQuery(traversal);
    }

    @Test
    public void testRangeBothVCase() {
        GraphTraversal traversal = g.V().outE().bothV().limit(10).path();
        executeTreeQuery(traversal);
    }

    @Test
    public void testRangeBothVFilterCase() {
        GraphTraversal traversal = g.V().outE().bothV().has("age", P.gt(30)).limit(10).path();
        executeTreeQuery(traversal);
    }

    @Test
    public void testRangeCountCase() {
        executeTreeQuery(g.V().out().limit(20).count());
    }
}

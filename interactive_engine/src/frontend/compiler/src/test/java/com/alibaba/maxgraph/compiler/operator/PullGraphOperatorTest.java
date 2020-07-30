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

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.junit.Test;

import java.io.IOException;

public class PullGraphOperatorTest extends AbstractOperatorTest {
    public PullGraphOperatorTest() throws IOException {
    }

    @Test
    public void testPullGraphOutPath() {
        executeTreeQuery(g.enablePullGraph().V().out().path());
    }

    @Test
    public void testPullGraphThreeOutPath() {
        executeTreeQuery(g.enablePullGraph().V().out().out().out().path());
    }

    @Test
    public void testPullGraphFourOutCase() {
        executeTreeQuery(g.enablePullGraph().V().out().out().out().out());
    }

    @Test
    public void testPullGraphBothOutPath() {
        executeTreeQuery(g.enablePullGraph().V().both().out().out().path());
    }

    @Test
    public void testPullGraphPropValueCase() {
        GraphTraversal traversal = g.enablePullGraph().V().as("a").out().values("firstname", "lastname").select("a");
        executeTreeQuery(traversal);
    }
}

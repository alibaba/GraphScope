/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.common.intermediate.operator;

import com.alibaba.graphscope.common.IrPlan;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.After;
import org.junit.Test;

public class SubGraphAsUnionOpTest {
    private IrPlan irPlan;
    private Graph graph = TinkerFactory.createModern();
    private GraphTraversalSource g = graph.traversal();

    // g.E().subgraph()
    @Test
    public void subgraphOpTest() throws Exception {
        //        Traversal traversal = g.E().subgraph("graph_1");
        //        IrStandardOpProcessor.applyStrategies(traversal);
        //        InterOpCollection ops = (new InterOpCollectionBuilder(traversal)).build();
        //        InterOpCollection.process(ops);
        //        irPlan = new IrPlan(new IrMeta(""), ops);
        //        String actual = irPlan.getPlanAsJson();
        //        Assert.assertEquals(FileUtils.readJsonFromResource("subgraph.json"), actual);
    }

    @After
    public void after() {
        if (irPlan != null) {
            irPlan.close();
        }
    }
}

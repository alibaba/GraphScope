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

package com.alibaba.graphscope.common.intermediate.strategy;

import com.alibaba.graphscope.common.IrPlan;
import com.alibaba.graphscope.common.intermediate.InterOpCollection;
import com.alibaba.graphscope.common.ir.Utils;
import com.alibaba.graphscope.common.utils.FileUtils;
import com.alibaba.graphscope.gremlin.InterOpCollectionBuilder;
import com.alibaba.graphscope.gremlin.integration.suite.utils.__;
import com.alibaba.graphscope.gremlin.plugin.processor.IrStandardOpProcessor;
import com.alibaba.graphscope.gremlin.plugin.traversal.IrCustomizedTraversalSource;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class ElementFusionStrategyTest {
    private IrPlan irPlan;
    private Graph graph = TinkerFactory.createModern();
    private GraphTraversalSource g = graph.traversal(IrCustomizedTraversalSource.class);

    // group().by(outE().count())
    @Test
    public void g_V_group_by_outE_count_test() {
        Traversal traversal = g.V().group().by(__.outE().count());
        IrStandardOpProcessor.applyStrategies(traversal);
        InterOpCollection opCollection = (new InterOpCollectionBuilder(traversal)).build();
        irPlan = new IrPlan(Utils.schemaMeta, opCollection);
        String actual = irPlan.getPlanAsJson();
        Assert.assertEquals(FileUtils.readJsonFromResource("group_key_out_count.json"), actual);
    }

    // group().by(outE().has('name', 'marko').count())
    @Test
    public void g_V_group_by_outE_has_count_test() {
        Traversal traversal = g.V().group().by(__.outE().has("name", "marko").count());
        IrStandardOpProcessor.applyStrategies(traversal);
        InterOpCollection opCollection = (new InterOpCollectionBuilder(traversal)).build();
        irPlan = new IrPlan(Utils.schemaMeta, opCollection);
        String actual = irPlan.getPlanAsJson();
        Assert.assertEquals(FileUtils.readJsonFromResource("group_key_out_has_count.json"), actual);
    }

    // group().by(out().count())
    @Test
    public void g_V_group_by_out_count_test() {
        Traversal traversal = g.V().group().by(__.out().count());
        IrStandardOpProcessor.applyStrategies(traversal);
        InterOpCollection opCollection = (new InterOpCollectionBuilder(traversal)).build();
        irPlan = new IrPlan(Utils.schemaMeta, opCollection);
        String actual = irPlan.getPlanAsJson();
        Assert.assertEquals(FileUtils.readJsonFromResource("group_key_out_count.json"), actual);
    }

    @After
    public void after() {
        if (irPlan != null) {
            irPlan.close();
        }
    }
}

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

package com.alibaba.graphscope.gremlin;

import com.alibaba.graphscope.common.intermediate.ArgUtils;
import com.alibaba.graphscope.common.intermediate.operator.ProjectOp;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import com.alibaba.graphscope.gremlin.InterOpCollectionBuilder.StepTransformFactory;
import org.javatuples.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class SelectStepTest {
    private Graph graph = TinkerFactory.createModern();
    private GraphTraversalSource g = graph.traversal();

    @Test
    public void g_V_select_test() {
        Traversal traversal = g.V().as("a").out().as("b").select("a", "b");
        Step selectStep = traversal.asAdmin().getEndStep();
        ProjectOp op = (ProjectOp) StepTransformFactory.SELECT_BY_STEP.apply(selectStep);
        List<Pair> exprWithAlias = (List<Pair>) op.getExprWithAlias().get().applyArg();

        Pair firstEntry = exprWithAlias.get(0);
        Assert.assertEquals("@a", firstEntry.getValue0());
        Assert.assertEquals(ArgUtils.asFfiAlias("project_a", false), firstEntry.getValue1());

        Pair sndEntry = exprWithAlias.get(1);
        Assert.assertEquals("@b", sndEntry.getValue0());
        Assert.assertEquals(ArgUtils.asFfiAlias("project_b", false), sndEntry.getValue1());
    }

    @Test
    public void g_V_select_key_test() {
        Traversal traversal = g.V().as("a").out().as("b").select("a", "b").by("name");
        Step selectStep = traversal.asAdmin().getEndStep();
        ProjectOp op = (ProjectOp) StepTransformFactory.SELECT_BY_STEP.apply(selectStep);
        List<Pair> exprWithAlias = (List<Pair>) op.getExprWithAlias().get().applyArg();

        Pair firstEntry = exprWithAlias.get(0);
        Assert.assertEquals("@a.name", firstEntry.getValue0());
        Assert.assertEquals(ArgUtils.asFfiAlias("a_name", false), firstEntry.getValue1());

        Pair sndEntry = exprWithAlias.get(1);
        Assert.assertEquals("@b.name", sndEntry.getValue0());
        Assert.assertEquals(ArgUtils.asFfiAlias("b_name", false), sndEntry.getValue1());
    }

    @Test
    public void g_V_select_one_test() {
        Traversal traversal = g.V().as("a").select("a");
        Step selectStep = traversal.asAdmin().getEndStep();
        ProjectOp op = (ProjectOp) StepTransformFactory.SELECT_ONE_BY_STEP.apply(selectStep);

        List<Pair> exprWithAlias = (List<Pair>) op.getExprWithAlias().get().applyArg();
        Assert.assertEquals("@a", exprWithAlias.get(0).getValue0());
        Assert.assertEquals(ArgUtils.asNoneAlias(), exprWithAlias.get(0).getValue1());
    }

    @Test
    public void g_V_select_one_by_key_test() {
        Traversal traversal = g.V().as("a").select("a").by("name");
        Step selectStep = traversal.asAdmin().getEndStep();
        ProjectOp op = (ProjectOp) StepTransformFactory.SELECT_ONE_BY_STEP.apply(selectStep);

        List<Pair> exprWithAlias = (List<Pair>) op.getExprWithAlias().get().applyArg();
        Assert.assertEquals("@a.name", exprWithAlias.get(0).getValue0());
        Assert.assertEquals(ArgUtils.asNoneAlias(), exprWithAlias.get(0).getValue1());
    }

    @Test
    public void g_V_select_one_by_valueMap_key_test() {
        Traversal traversal = g.V().as("a").select("a").by(__.valueMap("name"));
        Step selectStep = traversal.asAdmin().getEndStep();
        ProjectOp op = (ProjectOp) StepTransformFactory.SELECT_ONE_BY_STEP.apply(selectStep);

        List<Pair> exprWithAlias = (List<Pair>) op.getExprWithAlias().get().applyArg();
        Assert.assertEquals("{@a.name}", exprWithAlias.get(0).getValue0());
        Assert.assertEquals(ArgUtils.asNoneAlias(), exprWithAlias.get(0).getValue1());
    }

    @Test
    public void g_V_select_one_by_valueMap_keys_test() {
        Traversal traversal = g.V().as("a").select("a").by(__.valueMap("name", "id"));
        Step selectStep = traversal.asAdmin().getEndStep();
        ProjectOp op = (ProjectOp) StepTransformFactory.SELECT_ONE_BY_STEP.apply(selectStep);

        List<Pair> exprWithAlias = (List<Pair>) op.getExprWithAlias().get().applyArg();
        Assert.assertEquals("{@a.name, @a.id}", exprWithAlias.get(0).getValue0());
        Assert.assertEquals(ArgUtils.asNoneAlias(), exprWithAlias.get(0).getValue1());
    }
}

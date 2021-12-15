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
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.javatuples.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class SelectStepTest {
    private Graph graph = TinkerFactory.createModern();
    private GraphTraversalSource g = graph.traversal();

    @Test
    public void g_V_select_test() {
        Traversal traversal = g.V().select("a", "b");
        Step selectStep = traversal.asAdmin().getEndStep();
        ProjectOp op = (ProjectOp) IrPlanBuidler.StepTransformFactory.SELECT_BY_STEP.apply(selectStep);
        List<Pair> expected = Arrays.asList(Pair.with("@a", ArgUtils.strAsNameId("~@a")),
                Pair.with("@b", ArgUtils.strAsNameId("~@b")));
        Assert.assertEquals(expected, op.getProjectExprWithAlias().get().getArg());
    }

    @Test
    public void g_V_select_key_test() {
        Traversal traversal = g.V().select("a", "b").by("name");
        Step selectStep = traversal.asAdmin().getEndStep();
        ProjectOp op = (ProjectOp) IrPlanBuidler.StepTransformFactory.SELECT_BY_STEP.apply(selectStep);
        List<Pair> expected = Arrays.asList(Pair.with("@a.name", ArgUtils.strAsNameId("~@a.name")),
                Pair.with("@b.name", ArgUtils.strAsNameId("~@b.name")));
        Assert.assertEquals(expected, op.getProjectExprWithAlias().get().getArg());
    }
}

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

import com.alibaba.graphscope.common.intermediate.operator.ExpandOp;
import com.alibaba.graphscope.common.jna.type.FfiDirection;
import com.alibaba.graphscope.common.jna.type.FfiNameOrId;
import com.alibaba.graphscope.gremlin.transform.StepTransformFactory;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class VertexStepTest {
    private Graph graph = TinkerFactory.createModern();
    private GraphTraversalSource g = graph.traversal();

    @Test
    public void g_V_out() {
        Traversal traversal = g.V().out();
        Step vertexStep = traversal.asAdmin().getEndStep();
        ExpandOp op = (ExpandOp) StepTransformFactory.VERTEX_STEP.apply(vertexStep);
        Assert.assertEquals(FfiDirection.Out, op.getDirection().get().applyArg());
        Assert.assertEquals(false, op.getIsEdge().get().applyArg());
    }

    @Test
    public void g_V_outE() {
        Traversal traversal = g.V().outE();
        Step vertexStep = traversal.asAdmin().getEndStep();
        ExpandOp op = (ExpandOp) StepTransformFactory.VERTEX_STEP.apply(vertexStep);
        Assert.assertEquals(FfiDirection.Out, op.getDirection().get().applyArg());
        Assert.assertEquals(true, op.getIsEdge().get().applyArg());
    }

    @Test
    public void g_V_out_label() {
        Traversal traversal = g.V().out("knows");
        Step vertexStep = traversal.asAdmin().getEndStep();
        ExpandOp op = (ExpandOp) StepTransformFactory.VERTEX_STEP.apply(vertexStep);
        FfiNameOrId.ByValue label =
                ((List<FfiNameOrId.ByValue>) op.getLabels().get().applyArg()).get(0);
        Assert.assertEquals("knows", label.name);
    }
}

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
import com.alibaba.graphscope.common.intermediate.operator.UnfoldOp;
import com.alibaba.graphscope.common.jna.type.*;
import com.alibaba.graphscope.gremlin.transform.StepTransformFactory;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Assert;
import org.junit.Test;

public class UnfoldStepTest {
    private Graph graph = TinkerFactory.createModern();
    private GraphTraversalSource g = graph.traversal();

    @Test
    public void g_V() {
        Traversal traversal = g.V().fold().unfold();
        Step unfoldStep = traversal.asAdmin().getEndStep();
        UnfoldOp op = (UnfoldOp) StepTransformFactory.UNFOLD_STEP.apply(unfoldStep);
        FfiAlias.ByValue arg = (FfiAlias.ByValue) op.getUnfoldTag().get().getArg();
        Assert.assertEquals("", ArgUtils.tagName(arg.alias));
        Assert.assertEquals(false, op.getUnfoldAlias().isPresent());
    }

    @Test
    public void g_E() {
        Traversal traversal = g.E().fold().unfold();
        Step unfoldStep = traversal.asAdmin().getEndStep();
        UnfoldOp op = (UnfoldOp) StepTransformFactory.UNFOLD_STEP.apply(unfoldStep);
        FfiAlias.ByValue arg = (FfiAlias.ByValue) op.getUnfoldTag().get().getArg();
        Assert.assertEquals("", ArgUtils.tagName(arg.alias));
        Assert.assertEquals(false, op.getUnfoldAlias().isPresent());
    }
}

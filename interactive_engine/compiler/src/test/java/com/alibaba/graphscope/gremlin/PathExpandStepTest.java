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

import com.alibaba.graphscope.common.intermediate.operator.PathExpandOp;
import com.alibaba.graphscope.common.jna.type.*;
import com.alibaba.graphscope.gremlin.antlr4.__;
import com.alibaba.graphscope.gremlin.plugin.traversal.IrCustomizedTraversalSource;
import com.alibaba.graphscope.gremlin.transform.StepTransformFactory;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Assert;
import org.junit.Test;

public class PathExpandStepTest {
    private Graph graph = TinkerFactory.createModern();
    private IrCustomizedTraversalSource g = graph.traversal(IrCustomizedTraversalSource.class);

    @Test
    public void g_V_path_expand_test() {
        Traversal traversal = g.V().out(__.range(1, 2));
        Step step = traversal.asAdmin().getEndStep();
        PathExpandOp op = (PathExpandOp) StepTransformFactory.PATH_EXPAND_STEP.apply(step);

        Assert.assertEquals(FfiDirection.Out, op.getDirection().get().applyArg());
        Assert.assertEquals(FfiExpandOpt.Vertex, op.getExpandOpt().get().applyArg());
        Assert.assertEquals(1, op.getLower().get().applyArg());
        Assert.assertEquals(2, op.getUpper().get().applyArg());
    }

    @Test
    public void g_V_path_expand_label_test() {
        Traversal traversal = g.V().out(__.range(1, 2), "knows");
        Step step = traversal.asAdmin().getEndStep();
        PathExpandOp op = (PathExpandOp) StepTransformFactory.PATH_EXPAND_STEP.apply(step);

        Assert.assertEquals(FfiDirection.Out, op.getDirection().get().applyArg());
        Assert.assertEquals(FfiExpandOpt.Vertex, op.getExpandOpt().get().applyArg());
        Assert.assertEquals(1, op.getLower().get().applyArg());
        Assert.assertEquals(2, op.getUpper().get().applyArg());
        FfiNameOrId.ByValue label = op.getParams().get().getTables().get(0);
        Assert.assertEquals("knows", label.name);
    }

    @Test
    public void g_V_path_expand_with_test() {
        Traversal traversal =
                g.V().out(__.range(1, 2)).with("PathOpt", "Simple").with("ResultOpt", "AllV");
        Step step = traversal.asAdmin().getEndStep();
        PathExpandOp op = (PathExpandOp) StepTransformFactory.PATH_EXPAND_STEP.apply(step);

        Assert.assertEquals(FfiDirection.Out, op.getDirection().get().applyArg());
        Assert.assertEquals(FfiExpandOpt.Vertex, op.getExpandOpt().get().applyArg());
        Assert.assertEquals(1, op.getLower().get().applyArg());
        Assert.assertEquals(2, op.getUpper().get().applyArg());
        Assert.assertEquals(PathOpt.Simple, op.getPathOpt());
        Assert.assertEquals(ResultOpt.AllV, op.getResultOpt());
    }
}

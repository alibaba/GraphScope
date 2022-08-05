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

package com.alibaba.graphscope.gremlin.subtask;

import com.alibaba.graphscope.common.intermediate.InterOpCollection;
import com.alibaba.graphscope.common.intermediate.operator.ApplyOp;
import com.alibaba.graphscope.common.intermediate.operator.InterOpBase;
import com.alibaba.graphscope.common.intermediate.operator.SelectOp;
import com.alibaba.graphscope.common.jna.type.FfiJoinKind;
import com.alibaba.graphscope.gremlin.antlr4.__;
import com.alibaba.graphscope.gremlin.transform.TraversalParentTransformFactory;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class NotTraversalTest {
    private Graph graph = TinkerFactory.createModern();
    private GraphTraversalSource g = graph.traversal();

    private List<InterOpBase> getApplyOrSelect(Traversal traversal) {
        TraversalParent parent = (TraversalParent) traversal.asAdmin().getEndStep();
        return TraversalParentTransformFactory.NOT_TRAVERSAL_STEP.apply(parent);
    }

    @Test
    public void g_V_not_values() {
        Traversal traversal = g.V().not(__.values("name"));
        SelectOp selectOp = (SelectOp) getApplyOrSelect(traversal).get(0);

        Assert.assertEquals("!@.name", selectOp.getPredicate().get().applyArg());
    }

    @Test
    public void g_V_not_a() {
        Traversal traversal = g.V().as("a").not(__.select("a"));
        SelectOp selectOp = (SelectOp) getApplyOrSelect(traversal).get(0);

        Assert.assertEquals("!@a", selectOp.getPredicate().get().applyArg());
    }

    @Test
    public void g_V_not_a_values() {
        Traversal traversal = g.V().as("a").not(__.select("a").by(__.values("name")));
        SelectOp selectOp = (SelectOp) getApplyOrSelect(traversal).get(0);

        Assert.assertEquals("!@a.name", selectOp.getPredicate().get().applyArg());
    }

    @Test
    public void g_V_not_out_out() {
        Traversal traversal = g.V().not(__.out().out());
        ApplyOp applyOp = (ApplyOp) getApplyOrSelect(traversal).get(0);

        Assert.assertEquals(FfiJoinKind.Anti, applyOp.getJoinKind().get().applyArg());
        InterOpCollection subOps =
                (InterOpCollection) applyOp.getSubOpCollection().get().applyArg();
        Assert.assertEquals(2, subOps.unmodifiableCollection().size());
    }
}

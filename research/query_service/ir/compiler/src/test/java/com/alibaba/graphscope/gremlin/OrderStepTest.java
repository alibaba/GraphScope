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
import com.alibaba.graphscope.common.intermediate.operator.OrderOp;
import com.alibaba.graphscope.common.jna.type.FfiOrderOpt;
import com.alibaba.graphscope.common.jna.type.FfiProperty;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import com.alibaba.graphscope.gremlin.InterOpCollectionBuilder.StepTransformFactory;
import org.javatuples.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class OrderStepTest {
    private Graph graph = TinkerFactory.createModern();
    private GraphTraversalSource g = graph.traversal();


    @Test
    public void g_V_order_test() {
        Traversal traversal = g.V().order();
        Step orderStep = traversal.asAdmin().getEndStep();
        OrderOp op = (OrderOp) StepTransformFactory.ORDER_BY_STEP.apply(orderStep);
        List<Pair> expected = Arrays.asList(Pair.with(ArgUtils.asNoneVar(), FfiOrderOpt.Asc));
        Assert.assertEquals(expected, op.getOrderVarWithOrder().get().applyArg());
    }

    @Test
    public void g_V_order_by_label_test() {
        Traversal traversal = g.V().order().by("~label");
        Step orderStep = traversal.asAdmin().getEndStep();
        OrderOp op = (OrderOp) StepTransformFactory.ORDER_BY_STEP.apply(orderStep);
        FfiProperty.ByValue property = ArgUtils.asFfiProperty("~label");
        List<Pair> expected = Arrays.asList(Pair.with(ArgUtils.asVarPropertyOnly(property), FfiOrderOpt.Asc));
        Assert.assertEquals(expected, op.getOrderVarWithOrder().get().applyArg());
    }

    @Test
    public void g_V_order_by_key_test() {
        Traversal traversal = g.V().order().by("name");
        Step orderStep = traversal.asAdmin().getEndStep();
        OrderOp op = (OrderOp) StepTransformFactory.ORDER_BY_STEP.apply(orderStep);
        FfiProperty.ByValue property = ArgUtils.asFfiProperty("name");
        List<Pair> expected = Arrays.asList(Pair.with(ArgUtils.asVarPropertyOnly(property), FfiOrderOpt.Asc));
        Assert.assertEquals(expected, op.getOrderVarWithOrder().get().applyArg());
    }
}

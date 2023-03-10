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

import com.alibaba.graphscope.common.intermediate.ArgUtils;
import com.alibaba.graphscope.common.intermediate.InterOpCollection;
import com.alibaba.graphscope.common.intermediate.operator.ApplyOp;
import com.alibaba.graphscope.common.intermediate.operator.InterOpBase;
import com.alibaba.graphscope.common.intermediate.operator.OrderOp;
import com.alibaba.graphscope.common.jna.type.FfiJoinKind;
import com.alibaba.graphscope.common.jna.type.FfiOrderOpt;
import com.alibaba.graphscope.gremlin.integration.suite.utils.__;
import com.alibaba.graphscope.gremlin.plugin.processor.IrStandardOpProcessor;
import com.alibaba.graphscope.gremlin.transform.TraversalParentTransformFactory;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.javatuples.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class OrderStepTest {
    private Graph graph = TinkerFactory.createModern();
    private GraphTraversalSource g = graph.traversal();

    private List<InterOpBase> getApplyWithOrder(Traversal traversal) {
        IrStandardOpProcessor.applyStrategies(traversal);
        TraversalParent parent = (TraversalParent) traversal.asAdmin().getEndStep();
        return TraversalParentTransformFactory.ORDER_BY_STEP.apply(parent);
    }

    @Test
    public void g_V_order_test() {
        Traversal traversal = g.V().order();
        OrderOp op = (OrderOp) getApplyWithOrder(traversal).get(0);

        List<Pair> expected = Arrays.asList(Pair.with(ArgUtils.asNoneVar(), FfiOrderOpt.Asc));
        Assert.assertEquals(expected, op.getOrderVarWithOrder().get().applyArg());
    }

    @Test
    public void g_V_order_by_label_test() {
        Traversal traversal = g.V().order().by("~label");
        OrderOp op = (OrderOp) getApplyWithOrder(traversal).get(0);

        List<Pair> expected =
                Arrays.asList(Pair.with(ArgUtils.asVar("", "~label"), FfiOrderOpt.Asc));
        Assert.assertEquals(expected, op.getOrderVarWithOrder().get().applyArg());
    }

    @Test
    public void g_V_order_by_key_test() {
        Traversal traversal = g.V().order().by("name");
        OrderOp op = (OrderOp) getApplyWithOrder(traversal).get(0);

        List<Pair> expected = Arrays.asList(Pair.with(ArgUtils.asVar("", "name"), FfiOrderOpt.Asc));
        Assert.assertEquals(expected, op.getOrderVarWithOrder().get().applyArg());
    }

    @Test
    public void g_V_order_by_values_test() {
        Traversal traversal = g.V().order().by("name");
        OrderOp op = (OrderOp) getApplyWithOrder(traversal).get(0);

        List<Pair> expected = Arrays.asList(Pair.with(ArgUtils.asVar("", "name"), FfiOrderOpt.Asc));
        Assert.assertEquals(expected, op.getOrderVarWithOrder().get().applyArg());
    }

    @Test
    public void g_V_order_by_a_test() {
        Traversal traversal = g.V().as("a").order().by(__.select("a"));
        OrderOp op = (OrderOp) getApplyWithOrder(traversal).get(0);

        List<Pair> expected = Arrays.asList(Pair.with(ArgUtils.asVar("a", ""), FfiOrderOpt.Asc));
        Assert.assertEquals(expected, op.getOrderVarWithOrder().get().applyArg());
    }

    @Test
    public void g_V_order_by_a_name_test() {
        Traversal traversal = g.V().as("a").order().by(__.select("a").by("name"));
        OrderOp op = (OrderOp) getApplyWithOrder(traversal).get(0);

        List<Pair> expected =
                Arrays.asList(Pair.with(ArgUtils.asVar("a", "name"), FfiOrderOpt.Asc));
        Assert.assertEquals(expected, op.getOrderVarWithOrder().get().applyArg());
    }

    @Test
    public void g_V_order_by_out_count_test() {
        Traversal traversal = g.V().order().by(__.out().count());
        List<InterOpBase> ops = getApplyWithOrder(traversal);
        Assert.assertEquals(2, ops.size());

        ApplyOp applyOp = (ApplyOp) ops.get(0);
        Assert.assertEquals(FfiJoinKind.Inner, applyOp.getJoinKind().get().applyArg());
        InterOpCollection subOps =
                (InterOpCollection) applyOp.getSubOpCollection().get().applyArg();
        Assert.assertEquals(1, subOps.unmodifiableCollection().size());
        Assert.assertEquals(
                ArgUtils.asAlias("~alias_1_0", false), applyOp.getAlias().get().applyArg());

        OrderOp orderOp = (OrderOp) ops.get(1);
        List<Pair> expected =
                Arrays.asList(Pair.with(ArgUtils.asVar("~alias_1_0", ""), FfiOrderOpt.Asc));
        Assert.assertEquals(expected, orderOp.getOrderVarWithOrder().get().applyArg());
    }

    @Test
    public void g_V_order_by_name_by_out_count_test() {
        Traversal traversal = g.V().order().by("name").by(__.out().count());
        List<InterOpBase> ops = getApplyWithOrder(traversal);
        Assert.assertEquals(2, ops.size());

        ApplyOp applyOp = (ApplyOp) ops.get(0);
        Assert.assertEquals(FfiJoinKind.Inner, applyOp.getJoinKind().get().applyArg());
        InterOpCollection subOps =
                (InterOpCollection) applyOp.getSubOpCollection().get().applyArg();
        Assert.assertEquals(1, subOps.unmodifiableCollection().size());
        Assert.assertEquals(
                ArgUtils.asAlias("~alias_1_1", false), applyOp.getAlias().get().applyArg());

        OrderOp orderOp = (OrderOp) ops.get(1);
        List<Pair> expected =
                Arrays.asList(
                        Pair.with(ArgUtils.asVar("", "name"), FfiOrderOpt.Asc),
                        Pair.with(ArgUtils.asVar("~alias_1_1", ""), FfiOrderOpt.Asc));
        Assert.assertEquals(expected, orderOp.getOrderVarWithOrder().get().applyArg());
    }

    @Test
    public void g_V_order_by_out_out_count_by_out_count_test() {
        Traversal traversal =
                g.V().order().by(__.out().out().count(), Order.desc).by(__.out().count());
        List<InterOpBase> ops = getApplyWithOrder(traversal);
        Assert.assertEquals(3, ops.size());

        ApplyOp apply1 = (ApplyOp) ops.get(0);
        Assert.assertEquals(FfiJoinKind.Inner, apply1.getJoinKind().get().applyArg());
        InterOpCollection subOps = (InterOpCollection) apply1.getSubOpCollection().get().applyArg();
        Assert.assertEquals(3, subOps.unmodifiableCollection().size());
        Assert.assertEquals(
                ArgUtils.asAlias("~alias_1_0", false), apply1.getAlias().get().applyArg());

        ApplyOp apply2 = (ApplyOp) ops.get(1);
        Assert.assertEquals(FfiJoinKind.Inner, apply2.getJoinKind().get().applyArg());
        subOps = (InterOpCollection) apply2.getSubOpCollection().get().applyArg();
        Assert.assertEquals(1, subOps.unmodifiableCollection().size());
        Assert.assertEquals(
                ArgUtils.asAlias("~alias_1_1", false), apply2.getAlias().get().applyArg());

        OrderOp orderOp = (OrderOp) ops.get(2);
        List<Pair> expected =
                Arrays.asList(
                        Pair.with(ArgUtils.asVar("~alias_1_0", ""), FfiOrderOpt.Desc),
                        Pair.with(ArgUtils.asVar("~alias_1_1", ""), FfiOrderOpt.Asc));
        Assert.assertEquals(expected, orderOp.getOrderVarWithOrder().get().applyArg());
    }
}

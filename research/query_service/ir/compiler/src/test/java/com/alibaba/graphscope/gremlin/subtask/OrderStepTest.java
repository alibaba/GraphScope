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
import com.alibaba.graphscope.common.jna.type.FfiAlias;
import com.alibaba.graphscope.common.jna.type.FfiJoinKind;
import com.alibaba.graphscope.common.jna.type.FfiOrderOpt;
import com.alibaba.graphscope.gremlin.transform.TraversalParentTransformFactory;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
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
        TraversalParent parent = (TraversalParent) traversal.asAdmin().getEndStep();
        return TraversalParentTransformFactory.ORDER_BY_STEP.apply(parent);
    }

    @Test
    public void g_V_order_test() {
        Traversal traversal = g.V().order();
        OrderOp op = (OrderOp) getApplyWithOrder(traversal).get(0);

        List<Pair> expected = Arrays.asList(Pair.with(ArgUtils.asFfiNoneVar(), FfiOrderOpt.Asc));
        Assert.assertEquals(expected, op.getOrderVarWithOrder().get().applyArg());
    }

    @Test
    public void g_V_order_by_label_test() {
        Traversal traversal = g.V().order().by("~label");
        OrderOp op = (OrderOp) getApplyWithOrder(traversal).get(0);

        List<Pair> expected = Arrays.asList(Pair.with(ArgUtils.asFfiVar("", "~label"), FfiOrderOpt.Asc));
        Assert.assertEquals(expected, op.getOrderVarWithOrder().get().applyArg());
    }

    @Test
    public void g_V_order_by_key_test() {
        Traversal traversal = g.V().order().by("name");
        OrderOp op = (OrderOp) getApplyWithOrder(traversal).get(0);

        List<Pair> expected = Arrays.asList(Pair.with(ArgUtils.asFfiVar("", "name"), FfiOrderOpt.Asc));
        Assert.assertEquals(expected, op.getOrderVarWithOrder().get().applyArg());
    }

    @Test
    public void g_V_order_by_values_test() {
        Traversal traversal = g.V().order().by("name");
        OrderOp op = (OrderOp) getApplyWithOrder(traversal).get(0);

        List<Pair> expected = Arrays.asList(Pair.with(ArgUtils.asFfiVar("", "name"), FfiOrderOpt.Asc));
        Assert.assertEquals(expected, op.getOrderVarWithOrder().get().applyArg());
    }

    @Test
    public void g_V_order_by_a_test() {
        Traversal traversal = g.V().as("a").order().by(__.select("a"));
        OrderOp op = (OrderOp) getApplyWithOrder(traversal).get(0);

        List<Pair> expected = Arrays.asList(Pair.with(ArgUtils.asFfiVar("a", ""), FfiOrderOpt.Asc));
        Assert.assertEquals(expected, op.getOrderVarWithOrder().get().applyArg());
    }

    @Test
    public void g_V_order_by_a_name_test() {
        Traversal traversal = g.V().as("a").order().by(__.select("a").by("name"));
        OrderOp op = (OrderOp) getApplyWithOrder(traversal).get(0);

        List<Pair> expected = Arrays.asList(Pair.with(ArgUtils.asFfiVar("a", "name"), FfiOrderOpt.Asc));
        Assert.assertEquals(expected, op.getOrderVarWithOrder().get().applyArg());
    }

    @Test
    public void g_V_order_by_out_count_test() {
        Traversal traversal = g.V().order().by(__.out().count());
        List<InterOpBase> ops = getApplyWithOrder(traversal);
        Assert.assertEquals(2, ops.size());

        ApplyOp applyOp = (ApplyOp) ops.get(0);
        Assert.assertEquals(FfiJoinKind.Inner, applyOp.getJoinKind().get().applyArg());
        InterOpCollection subOps = (InterOpCollection) applyOp.getSubOpCollection().get().applyArg();
        Assert.assertEquals(2, subOps.unmodifiableCollection().size());
        Assert.assertTrue(applyOp.getAlias().isPresent());

        FfiAlias.ByValue applyAlias = (FfiAlias.ByValue) applyOp.getAlias().get().applyArg();
        String aliasName = applyAlias.alias.name;

        OrderOp orderOp = (OrderOp) ops.get(1);
        List<Pair> expected = Arrays.asList(Pair.with(ArgUtils.asFfiVar(aliasName, ""), FfiOrderOpt.Asc));
        Assert.assertEquals(expected, orderOp.getOrderVarWithOrder().get().applyArg());
    }

    @Test
    public void g_V_order_by_name_by_out_count_test() {
        Traversal traversal = g.V().order().by("name").by(__.out().count());
        List<InterOpBase> ops = getApplyWithOrder(traversal);
        Assert.assertEquals(2, ops.size());

        ApplyOp applyOp = (ApplyOp) ops.get(0);
        Assert.assertEquals(FfiJoinKind.Inner, applyOp.getJoinKind().get().applyArg());
        InterOpCollection subOps = (InterOpCollection) applyOp.getSubOpCollection().get().applyArg();
        Assert.assertEquals(2, subOps.unmodifiableCollection().size());
        Assert.assertTrue(applyOp.getAlias().isPresent());

        FfiAlias.ByValue applyAlias = (FfiAlias.ByValue) applyOp.getAlias().get().applyArg();
        String aliasName = applyAlias.alias.name;

        OrderOp orderOp = (OrderOp) ops.get(1);
        List<Pair> expected = Arrays.asList(
                Pair.with(ArgUtils.asFfiVar("", "name"), FfiOrderOpt.Asc),
                Pair.with(ArgUtils.asFfiVar(aliasName, ""), FfiOrderOpt.Asc));
        Assert.assertEquals(expected, orderOp.getOrderVarWithOrder().get().applyArg());
    }

    @Test
    public void g_V_order_by_out_out_count_by_out_count_test() {
        Traversal traversal = g.V().order().by(__.out().out().count(), Order.desc).by(__.out().count());
        List<InterOpBase> ops = getApplyWithOrder(traversal);
        Assert.assertEquals(3, ops.size());

        ApplyOp apply1 = (ApplyOp) ops.get(0);
        Assert.assertEquals(FfiJoinKind.Inner, apply1.getJoinKind().get().applyArg());
        InterOpCollection subOps = (InterOpCollection) apply1.getSubOpCollection().get().applyArg();
        Assert.assertEquals(3, subOps.unmodifiableCollection().size());
        Assert.assertTrue(apply1.getAlias().isPresent());

        FfiAlias.ByValue alias1 = (FfiAlias.ByValue) apply1.getAlias().get().applyArg();
        String aliasName1 = alias1.alias.name;

        ApplyOp apply2 = (ApplyOp) ops.get(1);
        Assert.assertEquals(FfiJoinKind.Inner, apply2.getJoinKind().get().applyArg());
        subOps = (InterOpCollection) apply2.getSubOpCollection().get().applyArg();
        Assert.assertEquals(2, subOps.unmodifiableCollection().size());
        Assert.assertTrue(apply2.getAlias().isPresent());

        FfiAlias.ByValue alias2 = (FfiAlias.ByValue) apply2.getAlias().get().applyArg();
        String aliasName2 = alias2.alias.name;

        OrderOp orderOp = (OrderOp) ops.get(2);
        List<Pair> expected = Arrays.asList(
                Pair.with(ArgUtils.asFfiVar(aliasName1, ""), FfiOrderOpt.Desc),
                Pair.with(ArgUtils.asFfiVar(aliasName2, ""), FfiOrderOpt.Asc));
        Assert.assertEquals(expected, orderOp.getOrderVarWithOrder().get().applyArg());
    }
}

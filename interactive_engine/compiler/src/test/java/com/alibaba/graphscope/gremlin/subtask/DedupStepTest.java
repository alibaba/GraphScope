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
import com.alibaba.graphscope.common.intermediate.operator.DedupOp;
import com.alibaba.graphscope.common.intermediate.operator.InterOpBase;
import com.alibaba.graphscope.common.jna.type.FfiJoinKind;
import com.alibaba.graphscope.common.jna.type.FfiVariable;
import com.alibaba.graphscope.gremlin.transform.TraversalParentTransformFactory;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class DedupStepTest {
    private Graph graph = TinkerFactory.createModern();
    private GraphTraversalSource g = graph.traversal();

    private List<InterOpBase> getApplyWithDedup(Traversal traversal) {
        TraversalParent parent = (TraversalParent) traversal.asAdmin().getEndStep();
        return TraversalParentTransformFactory.DEDUP_STEP.apply(parent);
    }

    @Test
    public void g_V_dedup_test() {
        Traversal traversal = g.V().dedup();
        DedupOp op = (DedupOp) getApplyWithDedup(traversal).get(0);

        FfiVariable.ByValue expectedVar = ArgUtils.asNoneVar();
        Assert.assertEquals(
                Collections.singletonList(expectedVar), op.getDedupKeys().get().applyArg());
    }

    @Test
    public void g_V_dedup_by_name_test() {
        Traversal traversal = g.V().dedup().by("name");
        DedupOp op = (DedupOp) getApplyWithDedup(traversal).get(0);

        FfiVariable.ByValue expectedVar = ArgUtils.asVar("", "name");
        Assert.assertEquals(
                Collections.singletonList(expectedVar), op.getDedupKeys().get().applyArg());
    }

    @Test
    public void g_V_dedup_by_values_test() {
        Traversal traversal = g.V().dedup().by(__.values("name"));
        DedupOp op = (DedupOp) getApplyWithDedup(traversal).get(0);

        FfiVariable.ByValue expectedVar = ArgUtils.asVar("", "name");
        Assert.assertEquals(
                Collections.singletonList(expectedVar), op.getDedupKeys().get().applyArg());
    }

    @Test
    public void g_V_dedup_by_out_count_test() {
        Traversal traversal = g.V().dedup().by(__.out().count());
        List<InterOpBase> ops = getApplyWithDedup(traversal);

        ApplyOp applyOp = (ApplyOp) ops.get(0);
        Assert.assertEquals(FfiJoinKind.Inner, applyOp.getJoinKind().get().applyArg());
        InterOpCollection subOps =
                (InterOpCollection) applyOp.getSubOpCollection().get().applyArg();
        Assert.assertEquals(2, subOps.unmodifiableCollection().size());
        Assert.assertEquals(
                ArgUtils.asAlias("~alias_1_0", false), applyOp.getAlias().get().applyArg());

        DedupOp dedupOp = (DedupOp) ops.get(1);
        FfiVariable.ByValue expectedVar = ArgUtils.asVar("~alias_1_0", "");
        Assert.assertEquals(
                Collections.singletonList(expectedVar), dedupOp.getDedupKeys().get().applyArg());
    }

    @Test
    public void g_V_as_dedup_a_by_name_test() {
        Traversal traversal = g.V().as("a").dedup("a").by("name");
        DedupOp op = (DedupOp) getApplyWithDedup(traversal).get(0);

        FfiVariable.ByValue expectedVar = ArgUtils.asVar("a", "name");
        Assert.assertEquals(
                Collections.singletonList(expectedVar), op.getDedupKeys().get().applyArg());
    }

    @Test
    public void g_V_as_dedup_a_b_by_name_test() {
        Traversal traversal = g.V().as("a").out().as("b").dedup("a", "b").by("name");
        DedupOp op = (DedupOp) getApplyWithDedup(traversal).get(0);

        Assert.assertEquals(
                Arrays.asList(ArgUtils.asVar("a", "name"), ArgUtils.asVar("b", "name")),
                op.getDedupKeys().get().applyArg());
    }

    @Test
    public void g_V_dedup_by_label_test() {
        Traversal traversal = g.V().dedup().by(T.label);
        DedupOp op = (DedupOp) getApplyWithDedup(traversal).get(0);

        FfiVariable.ByValue expectedVar = ArgUtils.asVar("", "~label");
        Assert.assertEquals(
                Collections.singletonList(expectedVar), op.getDedupKeys().get().applyArg());
    }

    @Test
    public void g_V_dedup_by_id_test() {
        Traversal traversal = g.V().dedup().by(T.id);
        DedupOp op = (DedupOp) getApplyWithDedup(traversal).get(0);

        FfiVariable.ByValue expectedVar = ArgUtils.asVar("", "~id");
        Assert.assertEquals(
                Collections.singletonList(expectedVar), op.getDedupKeys().get().applyArg());
    }
}

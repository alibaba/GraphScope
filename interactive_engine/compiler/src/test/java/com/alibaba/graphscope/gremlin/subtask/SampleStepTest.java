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
import com.alibaba.graphscope.common.intermediate.operator.SampleOp;
import com.alibaba.graphscope.common.jna.type.FfiJoinKind;
import com.alibaba.graphscope.gremlin.integration.suite.utils.__;
import com.alibaba.graphscope.gremlin.plugin.processor.IrStandardOpProcessor;
import com.alibaba.graphscope.gremlin.plugin.traversal.IrCustomizedTraversalSource;
import com.alibaba.graphscope.gremlin.transform.StepTransformFactory;
import com.alibaba.graphscope.gremlin.transform.TraversalParentTransformFactory;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class SampleStepTest {
    private Graph graph = TinkerFactory.createModern();
    private IrCustomizedTraversalSource g = graph.traversal(IrCustomizedTraversalSource.class);

    private List<InterOpBase> getApplyWithDedup(Traversal traversal) {
        IrStandardOpProcessor.applyStrategies(traversal);
        TraversalParent parent = (TraversalParent) traversal.asAdmin().getEndStep();
        return TraversalParentTransformFactory.SAMPLE_BY_STEP.apply(parent);
    }

    // g.V().coin()
    @Test
    public void g_V_coin_test() {
        Traversal traversal = g.V().coin(0.5);
        Step graphStep = traversal.asAdmin().getEndStep();
        SampleOp op = (SampleOp) StepTransformFactory.COIN_STEP.apply(graphStep);
        Assert.assertEquals(
                0.5, ((SampleOp.RatioType) op.getSampleType()).getRatio(), Math.ulp(0.5));
    }

    @Test
    public void g_V_sample_test() {
        Traversal traversal = g.V().out().sample(10);
        SampleOp sampleOp = (SampleOp) getApplyWithDedup(traversal).get(0);
        Assert.assertEquals(10, ((SampleOp.AmountType) sampleOp.getSampleType()).getAmount());
    }

    @Test
    public void g_V_sample_by_name_test() {
        Traversal traversal = g.V().out().sample(10).by("name");
        SampleOp sampleOp = (SampleOp) getApplyWithDedup(traversal).get(0);
        Assert.assertEquals(10, ((SampleOp.AmountType) sampleOp.getSampleType()).getAmount());
        Assert.assertEquals(ArgUtils.asVar("", "name"), sampleOp.getVariable());
    }

    @Test
    public void g_V_sample_by_a_age_test() {
        Traversal traversal = g.V().as("a").out().sample(10).by(__.select("a").by("age"));
        SampleOp sampleOp = (SampleOp) getApplyWithDedup(traversal).get(0);
        Assert.assertEquals(10, ((SampleOp.AmountType) sampleOp.getSampleType()).getAmount());
        Assert.assertEquals(ArgUtils.asVar("a", "age"), sampleOp.getVariable());
    }

    @Test
    public void g_V_sample_by_out_count_test() {
        Traversal traversal = g.V().out().sample(10).by(__.out().count());
        List<InterOpBase> ops = getApplyWithDedup(traversal);

        ApplyOp applyOp = (ApplyOp) ops.get(0);
        Assert.assertEquals(FfiJoinKind.Inner, applyOp.getJoinKind().get().applyArg());
        InterOpCollection subOps =
                (InterOpCollection) applyOp.getSubOpCollection().get().applyArg();
        Assert.assertEquals(2, subOps.unmodifiableCollection().size());
        Assert.assertEquals(
                ArgUtils.asAlias("~alias_2_0", false), applyOp.getAlias().get().applyArg());

        SampleOp sampleOp = (SampleOp) ops.get(1);
        Assert.assertEquals(10, ((SampleOp.AmountType) sampleOp.getSampleType()).getAmount());
        Assert.assertEquals(ArgUtils.asVar("~alias_2_0", ""), sampleOp.getVariable());
    }
}

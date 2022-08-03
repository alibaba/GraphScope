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

import com.alibaba.graphscope.common.intermediate.ArgAggFn;
import com.alibaba.graphscope.common.intermediate.ArgUtils;
import com.alibaba.graphscope.common.intermediate.operator.GroupOp;
import com.alibaba.graphscope.common.jna.type.FfiAggOpt;
import com.alibaba.graphscope.gremlin.transform.StepTransformFactory;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class AggregateStepTest {
    private Graph graph = TinkerFactory.createModern();
    private GraphTraversalSource g = graph.traversal();

    @Test
    public void g_V_count_test() {
        Traversal traversal = g.V().count();
        Step step = traversal.asAdmin().getEndStep();
        GroupOp op = (GroupOp) StepTransformFactory.AGGREGATE_STEP.apply(step);

        Assert.assertEquals(Collections.emptyList(), op.getGroupByKeys().get().applyArg());
        ArgAggFn expectedValue =
                new ArgAggFn(FfiAggOpt.Count, ArgUtils.asFfiAlias("~values_1_0", false));
        Assert.assertEquals(
                Collections.singletonList(expectedValue), op.getGroupByValues().get().applyArg());
    }

    @Test
    public void g_V_count_as_test() {
        Traversal traversal = g.V().count().as("a");
        Step step = traversal.asAdmin().getEndStep();
        GroupOp op = (GroupOp) StepTransformFactory.AGGREGATE_STEP.apply(step);

        Assert.assertEquals(Collections.emptyList(), op.getGroupByKeys().get().applyArg());
        ArgAggFn expectedValue = new ArgAggFn(FfiAggOpt.Count, ArgUtils.asFfiAlias("a", true));
        Assert.assertEquals(
                Collections.singletonList(expectedValue), op.getGroupByValues().get().applyArg());
    }

    // the same with min/max/mean/fold
    @Test
    public void g_V_values_sum_test() {
        Traversal traversal = g.V().values("age").sum();
        Step step = traversal.asAdmin().getEndStep();
        GroupOp op = (GroupOp) StepTransformFactory.AGGREGATE_STEP.apply(step);

        Assert.assertEquals(Collections.emptyList(), op.getGroupByKeys().get().applyArg());
        ArgAggFn expectedValue =
                new ArgAggFn(FfiAggOpt.Sum, ArgUtils.asFfiAlias("~values_2_0", false));
        Assert.assertEquals(
                Collections.singletonList(expectedValue), op.getGroupByValues().get().applyArg());
    }
}

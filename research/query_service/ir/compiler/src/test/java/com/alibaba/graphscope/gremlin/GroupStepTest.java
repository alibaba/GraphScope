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
import com.alibaba.graphscope.common.jna.type.*;
import org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.__;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import com.alibaba.graphscope.gremlin.InterOpCollectionBuilder.StepTransformFactory;
import org.javatuples.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class GroupStepTest {
    private Graph graph = TinkerFactory.createModern();
    private GraphTraversalSource g = graph.traversal();

    @Test
    public void g_V_group_test() {
        Traversal traversal = g.V().group();
        Step step = traversal.asAdmin().getEndStep();
        GroupOp op = (GroupOp) StepTransformFactory.GROUP_STEP.apply(step);

        Pair<FfiVariable.ByValue, FfiAlias.ByValue> expectedKey = Pair.with(ArgUtils.asNoneVar(),
                ArgUtils.asFfiAlias("keys", false));
        ArgAggFn expectedValue = new ArgAggFn(FfiAggOpt.ToList, ArgUtils.asFfiAlias("values", false));

        Assert.assertEquals(Collections.singletonList(expectedKey), op.getGroupByKeys().get().applyArg());
        Assert.assertEquals(Collections.singletonList(expectedValue), op.getGroupByValues().get().applyArg());
    }

    @Test
    public void g_V_group_by_key_test() {
        Traversal traversal = g.V().group().by("name");
        Step step = traversal.asAdmin().getEndStep();
        GroupOp op = (GroupOp) StepTransformFactory.GROUP_STEP.apply(step);

        FfiProperty.ByValue keyProperty = ArgUtils.asFfiProperty("name");
        Pair<FfiVariable.ByValue, FfiAlias.ByValue> expectedKey = Pair.with(ArgUtils.asVarPropertyOnly(keyProperty),
                ArgUtils.asFfiAlias("keys_name", false));
        ArgAggFn expectedValue = new ArgAggFn(FfiAggOpt.ToList, ArgUtils.asFfiAlias("values", false));

        Assert.assertEquals(Collections.singletonList(expectedKey), op.getGroupByKeys().get().applyArg());
        Assert.assertEquals(Collections.singletonList(expectedValue), op.getGroupByValues().get().applyArg());
    }

    @Test
    public void g_V_group_by_values_test() {
        Traversal traversal = g.V().group().by(__.values("name"));
        Step step = traversal.asAdmin().getEndStep();
        GroupOp op = (GroupOp) StepTransformFactory.GROUP_STEP.apply(step);

        FfiProperty.ByValue keyProperty = ArgUtils.asFfiProperty("name");
        Pair<FfiVariable.ByValue, FfiAlias.ByValue> expectedKey = Pair.with(ArgUtils.asVarPropertyOnly(keyProperty),
                ArgUtils.asFfiAlias("keys_name", false));
        ArgAggFn expectedValue = new ArgAggFn(FfiAggOpt.ToList, ArgUtils.asFfiAlias("values", false));

        Assert.assertEquals(Collections.singletonList(expectedKey), op.getGroupByKeys().get().applyArg());
        Assert.assertEquals(Collections.singletonList(expectedValue), op.getGroupByValues().get().applyArg());
    }

    @Test
    public void g_V_group_by_values_as_test() {
        Traversal traversal = g.V().group().by(__.values("name").as("a"));
        Step step = traversal.asAdmin().getEndStep();
        GroupOp op = (GroupOp) StepTransformFactory.GROUP_STEP.apply(step);

        FfiProperty.ByValue keyProperty = ArgUtils.asFfiProperty("name");
        Pair<FfiVariable.ByValue, FfiAlias.ByValue> expectedKey = Pair.with(ArgUtils.asVarPropertyOnly(keyProperty),
                ArgUtils.asFfiAlias("a", true));
        ArgAggFn expectedValue = new ArgAggFn(FfiAggOpt.ToList, ArgUtils.asFfiAlias("values", false));

        Assert.assertEquals(Collections.singletonList(expectedKey), op.getGroupByKeys().get().applyArg());
        Assert.assertEquals(Collections.singletonList(expectedValue), op.getGroupByValues().get().applyArg());
    }

    @Test
    public void g_V_group_by_key_by_count_test() {
        Traversal traversal = g.V().group().by("name").by(__.count());
        Step step = traversal.asAdmin().getEndStep();
        GroupOp op = (GroupOp) StepTransformFactory.GROUP_STEP.apply(step);

        FfiProperty.ByValue keyProperty = ArgUtils.asFfiProperty("name");
        Pair<FfiVariable.ByValue, FfiAlias.ByValue> expectedKey = Pair.with(ArgUtils.asVarPropertyOnly(keyProperty),
                ArgUtils.asFfiAlias("keys_name", false));
        ArgAggFn expectedValue = new ArgAggFn(FfiAggOpt.Count, ArgUtils.asFfiAlias("values", false));

        Assert.assertEquals(Collections.singletonList(expectedKey), op.getGroupByKeys().get().applyArg());
        Assert.assertEquals(Collections.singletonList(expectedValue), op.getGroupByValues().get().applyArg());
    }

    @Test
    public void g_V_group_by_key_by_count_as_test() {
        Traversal traversal = g.V().group().by("name").by(__.count().as("b"));
        Step step = traversal.asAdmin().getEndStep();
        GroupOp op = (GroupOp) StepTransformFactory.GROUP_STEP.apply(step);

        FfiProperty.ByValue keyProperty = ArgUtils.asFfiProperty("name");
        Pair<FfiVariable.ByValue, FfiAlias.ByValue> expectedKey = Pair.with(ArgUtils.asVarPropertyOnly(keyProperty),
                ArgUtils.asFfiAlias("keys_name", false));
        ArgAggFn expectedValue = new ArgAggFn(FfiAggOpt.Count, ArgUtils.asFfiAlias("b", true));

        Assert.assertEquals(Collections.singletonList(expectedKey), op.getGroupByKeys().get().applyArg());
        Assert.assertEquals(Collections.singletonList(expectedValue), op.getGroupByValues().get().applyArg());
    }

    @Test
    public void g_V_group_by_key_by_fold_test() {
        Traversal traversal = g.V().group().by("name").by(__.fold());
        Step step = traversal.asAdmin().getEndStep();
        GroupOp op = (GroupOp) StepTransformFactory.GROUP_STEP.apply(step);

        FfiProperty.ByValue keyProperty = ArgUtils.asFfiProperty("name");
        Pair<FfiVariable.ByValue, FfiAlias.ByValue> expectedKey = Pair.with(ArgUtils.asVarPropertyOnly(keyProperty),
                ArgUtils.asFfiAlias("keys_name", false));
        ArgAggFn expectedValue = new ArgAggFn(FfiAggOpt.ToList, ArgUtils.asFfiAlias("values", false));

        Assert.assertEquals(Collections.singletonList(expectedKey), op.getGroupByKeys().get().applyArg());
        Assert.assertEquals(Collections.singletonList(expectedValue), op.getGroupByValues().get().applyArg());
    }

    @Test
    public void g_V_group_by_key_by_fold_as_test() {
        Traversal traversal = g.V().group().by("name").by(__.fold().as("b"));
        Step step = traversal.asAdmin().getEndStep();
        GroupOp op = (GroupOp) StepTransformFactory.GROUP_STEP.apply(step);

        FfiProperty.ByValue keyProperty = ArgUtils.asFfiProperty("name");
        Pair<FfiVariable.ByValue, FfiAlias.ByValue> expectedKey = Pair.with(ArgUtils.asVarPropertyOnly(keyProperty),
                ArgUtils.asFfiAlias("keys_name", false));
        ArgAggFn expectedValue = new ArgAggFn(FfiAggOpt.ToList, ArgUtils.asFfiAlias("b", true));

        Assert.assertEquals(Collections.singletonList(expectedKey), op.getGroupByKeys().get().applyArg());
        Assert.assertEquals(Collections.singletonList(expectedValue), op.getGroupByValues().get().applyArg());
    }


    @Test
    public void g_V_groupCount_test() {
        Traversal traversal = g.V().groupCount();
        Step step = traversal.asAdmin().getEndStep();
        GroupOp op = (GroupOp) StepTransformFactory.GROUP_COUNT_STEP.apply(step);

        Pair<FfiVariable.ByValue, FfiAlias.ByValue> expectedKey = Pair.with(ArgUtils.asNoneVar(),
                ArgUtils.asFfiAlias("keys", false));
        ArgAggFn expectedValue = new ArgAggFn(FfiAggOpt.Count, ArgUtils.asFfiAlias("values", false));

        Assert.assertEquals(Collections.singletonList(expectedKey), op.getGroupByKeys().get().applyArg());
        Assert.assertEquals(Collections.singletonList(expectedValue), op.getGroupByValues().get().applyArg());
    }

    @Test
    public void g_V_groupCount_by_key_test() {
        Traversal traversal = g.V().groupCount().by("name");
        Step step = traversal.asAdmin().getEndStep();
        GroupOp op = (GroupOp) StepTransformFactory.GROUP_COUNT_STEP.apply(step);

        FfiProperty.ByValue keyProperty = ArgUtils.asFfiProperty("name");
        Pair<FfiVariable.ByValue, FfiAlias.ByValue> expectedKey = Pair.with(ArgUtils.asVarPropertyOnly(keyProperty),
                ArgUtils.asFfiAlias("keys_name", false));
        ArgAggFn expectedValue = new ArgAggFn(FfiAggOpt.Count, ArgUtils.asFfiAlias("values", false));

        Assert.assertEquals(Collections.singletonList(expectedKey), op.getGroupByKeys().get().applyArg());
        Assert.assertEquals(Collections.singletonList(expectedValue), op.getGroupByValues().get().applyArg());
    }

    @Test
    public void g_V_groupCount_by_values_test() {
        Traversal traversal = g.V().groupCount().by(__.values("name"));
        Step step = traversal.asAdmin().getEndStep();
        GroupOp op = (GroupOp) StepTransformFactory.GROUP_COUNT_STEP.apply(step);

        FfiProperty.ByValue keyProperty = ArgUtils.asFfiProperty("name");
        Pair<FfiVariable.ByValue, FfiAlias.ByValue> expectedKey = Pair.with(ArgUtils.asVarPropertyOnly(keyProperty),
                ArgUtils.asFfiAlias("keys_name", false));
        ArgAggFn expectedValue = new ArgAggFn(FfiAggOpt.Count, ArgUtils.asFfiAlias("values", false));

        Assert.assertEquals(Collections.singletonList(expectedKey), op.getGroupByKeys().get().applyArg());
        Assert.assertEquals(Collections.singletonList(expectedValue), op.getGroupByValues().get().applyArg());
    }

    @Test
    public void g_V_groupCount_by_values_as_test() {
        Traversal traversal = g.V().groupCount().by(__.values("name").as("a"));
        Step step = traversal.asAdmin().getEndStep();
        GroupOp op = (GroupOp) StepTransformFactory.GROUP_COUNT_STEP.apply(step);

        FfiProperty.ByValue keyProperty = ArgUtils.asFfiProperty("name");
        Pair<FfiVariable.ByValue, FfiAlias.ByValue> expectedKey = Pair.with(ArgUtils.asVarPropertyOnly(keyProperty),
                ArgUtils.asFfiAlias("a", true));
        ArgAggFn expectedValue = new ArgAggFn(FfiAggOpt.Count,  ArgUtils.asFfiAlias("values", false));

        Assert.assertEquals(Collections.singletonList(expectedKey), op.getGroupByKeys().get().applyArg());
        Assert.assertEquals(Collections.singletonList(expectedValue), op.getGroupByValues().get().applyArg());
    }
}

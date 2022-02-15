package com.alibaba.graphscope.gremlin.subtask;

import com.alibaba.graphscope.common.intermediate.ArgAggFn;
import com.alibaba.graphscope.common.intermediate.ArgUtils;
import com.alibaba.graphscope.common.intermediate.InterOpCollection;
import com.alibaba.graphscope.common.intermediate.operator.ApplyOp;
import com.alibaba.graphscope.common.intermediate.operator.GroupOp;
import com.alibaba.graphscope.common.intermediate.operator.InterOpBase;
import com.alibaba.graphscope.common.jna.type.FfiAggOpt;
import com.alibaba.graphscope.common.jna.type.FfiAlias;
import com.alibaba.graphscope.common.jna.type.FfiJoinKind;
import com.alibaba.graphscope.common.jna.type.FfiVariable;
import com.alibaba.graphscope.gremlin.transform.TraversalParentTransformFactory;
import org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.__;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.javatuples.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class GroupStepTest {
    private Graph graph = TinkerFactory.createModern();
    private GraphTraversalSource g = graph.traversal();

    private List<InterOpBase> getApplyWithGroup(Traversal traversal) {
        TraversalParent parent = (TraversalParent) traversal.asAdmin().getEndStep();
        return TraversalParentTransformFactory.GROUP_BY_STEP.apply(parent);
    }

    @Test
    public void g_V_group_test() {
        Traversal traversal = g.V().group();
        GroupOp op = (GroupOp) getApplyWithGroup(traversal).get(0);

        Pair<FfiVariable.ByValue, FfiAlias.ByValue> expectedKey = Pair.with(
                ArgUtils.asNoneVar(),
                ArgUtils.asFfiAlias("keys", false));
        ArgAggFn expectedValue = new ArgAggFn(FfiAggOpt.ToList, ArgUtils.asFfiAlias("values", false));

        Assert.assertEquals(Collections.singletonList(expectedKey), op.getGroupByKeys().get().applyArg());
        Assert.assertEquals(Collections.singletonList(expectedValue), op.getGroupByValues().get().applyArg());
    }

    @Test
    public void g_V_group_by_key_test() {
        Traversal traversal = g.V().group().by("name");
        GroupOp op = (GroupOp) getApplyWithGroup(traversal).get(0);

        Pair<FfiVariable.ByValue, FfiAlias.ByValue> expectedKey = Pair.with(
                ArgUtils.asVar("", "name"),
                ArgUtils.asFfiAlias("keys_name", false));
        ArgAggFn expectedValue = new ArgAggFn(FfiAggOpt.ToList, ArgUtils.asFfiAlias("values", false));

        Assert.assertEquals(Collections.singletonList(expectedKey), op.getGroupByKeys().get().applyArg());
        Assert.assertEquals(Collections.singletonList(expectedValue), op.getGroupByValues().get().applyArg());
    }

    @Test
    public void g_V_group_by_values_test() {
        Traversal traversal = g.V().group().by(__.values("name"));
        GroupOp op = (GroupOp) getApplyWithGroup(traversal).get(0);

        Pair<FfiVariable.ByValue, FfiAlias.ByValue> expectedKey = Pair.with(
                ArgUtils.asVar("", "name"),
                ArgUtils.asFfiAlias("keys_name", false));
        ArgAggFn expectedValue = new ArgAggFn(FfiAggOpt.ToList, ArgUtils.asFfiAlias("values", false));

        Assert.assertEquals(Collections.singletonList(expectedKey), op.getGroupByKeys().get().applyArg());
        Assert.assertEquals(Collections.singletonList(expectedValue), op.getGroupByValues().get().applyArg());
    }

    @Test
    public void g_V_group_by_values_as_test() {
        Traversal traversal = g.V().group().by(__.values("name").as("a"));
        GroupOp op = (GroupOp) getApplyWithGroup(traversal).get(0);

        Pair<FfiVariable.ByValue, FfiAlias.ByValue> expectedKey = Pair.with(
                ArgUtils.asVar("", "name"),
                ArgUtils.asFfiAlias("a", true));
        ArgAggFn expectedValue = new ArgAggFn(FfiAggOpt.ToList, ArgUtils.asFfiAlias("values", false));

        Assert.assertEquals(Collections.singletonList(expectedKey), op.getGroupByKeys().get().applyArg());
        Assert.assertEquals(Collections.singletonList(expectedValue), op.getGroupByValues().get().applyArg());
    }

    @Test
    public void g_V_group_by_key_by_count_test() {
        Traversal traversal = g.V().group().by("name").by(__.count());
        GroupOp op = (GroupOp) getApplyWithGroup(traversal).get(0);

        Pair<FfiVariable.ByValue, FfiAlias.ByValue> expectedKey = Pair.with(
                ArgUtils.asVar("", "name"),
                ArgUtils.asFfiAlias("keys_name", false));
        ArgAggFn expectedValue = new ArgAggFn(FfiAggOpt.Count, ArgUtils.asFfiAlias("values", false));

        Assert.assertEquals(Collections.singletonList(expectedKey), op.getGroupByKeys().get().applyArg());
        Assert.assertEquals(Collections.singletonList(expectedValue), op.getGroupByValues().get().applyArg());
    }

    @Test
    public void g_V_group_by_key_by_count_as_test() {
        Traversal traversal = g.V().group().by("name").by(__.count().as("b"));
        GroupOp op = (GroupOp) getApplyWithGroup(traversal).get(0);

        Pair<FfiVariable.ByValue, FfiAlias.ByValue> expectedKey = Pair.with(
                ArgUtils.asVar("", "name"),
                ArgUtils.asFfiAlias("keys_name", false));
        ArgAggFn expectedValue = new ArgAggFn(FfiAggOpt.Count, ArgUtils.asFfiAlias("b", true));

        Assert.assertEquals(Collections.singletonList(expectedKey), op.getGroupByKeys().get().applyArg());
        Assert.assertEquals(Collections.singletonList(expectedValue), op.getGroupByValues().get().applyArg());
    }

    @Test
    public void g_V_group_by_key_by_fold_test() {
        Traversal traversal = g.V().group().by("name").by(__.fold());
        GroupOp op = (GroupOp) getApplyWithGroup(traversal).get(0);

        Pair<FfiVariable.ByValue, FfiAlias.ByValue> expectedKey = Pair.with(
                ArgUtils.asVar("", "name"),
                ArgUtils.asFfiAlias("keys_name", false));
        ArgAggFn expectedValue = new ArgAggFn(FfiAggOpt.ToList, ArgUtils.asFfiAlias("values", false));

        Assert.assertEquals(Collections.singletonList(expectedKey), op.getGroupByKeys().get().applyArg());
        Assert.assertEquals(Collections.singletonList(expectedValue), op.getGroupByValues().get().applyArg());
    }

    @Test
    public void g_V_group_by_key_by_fold_as_test() {
        Traversal traversal = g.V().group().by("name").by(__.fold().as("b"));
        GroupOp op = (GroupOp) getApplyWithGroup(traversal).get(0);

        Pair<FfiVariable.ByValue, FfiAlias.ByValue> expectedKey = Pair.with(
                ArgUtils.asVar("", "name"),
                ArgUtils.asFfiAlias("keys_name", false));
        ArgAggFn expectedValue = new ArgAggFn(FfiAggOpt.ToList, ArgUtils.asFfiAlias("b", true));

        Assert.assertEquals(Collections.singletonList(expectedKey), op.getGroupByKeys().get().applyArg());
        Assert.assertEquals(Collections.singletonList(expectedValue), op.getGroupByValues().get().applyArg());
    }

    @Test
    public void g_V_group_by_a_key_by_fold_as_test() {
        Traversal traversal = g.V().as("a").group().by(__.select("a").by("name")).by(__.fold().as("b"));
        GroupOp op = (GroupOp) getApplyWithGroup(traversal).get(0);

        Pair<FfiVariable.ByValue, FfiAlias.ByValue> expectedKey = Pair.with(
                ArgUtils.asVar("a", "name"),
                ArgUtils.asFfiAlias("keys_a_name", false));
        ArgAggFn expectedValue = new ArgAggFn(FfiAggOpt.ToList, ArgUtils.asFfiAlias("b", true));

        Assert.assertEquals(Collections.singletonList(expectedKey), op.getGroupByKeys().get().applyArg());
        Assert.assertEquals(Collections.singletonList(expectedValue), op.getGroupByValues().get().applyArg());
    }

    @Test
    public void g_V_groupCount_test() {
        Traversal traversal = g.V().groupCount();
        GroupOp op = (GroupOp) getApplyWithGroup(traversal).get(0);

        Pair<FfiVariable.ByValue, FfiAlias.ByValue> expectedKey = Pair.with(
                ArgUtils.asNoneVar(),
                ArgUtils.asFfiAlias("keys", false));
        ArgAggFn expectedValue = new ArgAggFn(FfiAggOpt.Count, ArgUtils.asFfiAlias("values", false));

        Assert.assertEquals(Collections.singletonList(expectedKey), op.getGroupByKeys().get().applyArg());
        Assert.assertEquals(Collections.singletonList(expectedValue), op.getGroupByValues().get().applyArg());
    }

    @Test
    public void g_V_groupCount_by_key_test() {
        Traversal traversal = g.V().groupCount().by("name");
        GroupOp op = (GroupOp) getApplyWithGroup(traversal).get(0);

        Pair<FfiVariable.ByValue, FfiAlias.ByValue> expectedKey = Pair.with(
                ArgUtils.asVar("", "name"),
                ArgUtils.asFfiAlias("keys_name", false));
        ArgAggFn expectedValue = new ArgAggFn(FfiAggOpt.Count, ArgUtils.asFfiAlias("values", false));

        Assert.assertEquals(Collections.singletonList(expectedKey), op.getGroupByKeys().get().applyArg());
        Assert.assertEquals(Collections.singletonList(expectedValue), op.getGroupByValues().get().applyArg());
    }

    @Test
    public void g_V_groupCount_by_values_test() {
        Traversal traversal = g.V().groupCount().by(__.values("name"));
        GroupOp op = (GroupOp) getApplyWithGroup(traversal).get(0);

        Pair<FfiVariable.ByValue, FfiAlias.ByValue> expectedKey = Pair.with(
                ArgUtils.asVar("", "name"),
                ArgUtils.asFfiAlias("keys_name", false));
        ArgAggFn expectedValue = new ArgAggFn(FfiAggOpt.Count, ArgUtils.asFfiAlias("values", false));

        Assert.assertEquals(Collections.singletonList(expectedKey), op.getGroupByKeys().get().applyArg());
        Assert.assertEquals(Collections.singletonList(expectedValue), op.getGroupByValues().get().applyArg());
    }

    @Test
    public void g_V_groupCount_by_values_as_test() {
        Traversal traversal = g.V().groupCount().by(__.values("name").as("a"));
        GroupOp op = (GroupOp) getApplyWithGroup(traversal).get(0);

        Pair<FfiVariable.ByValue, FfiAlias.ByValue> expectedKey = Pair.with(
                ArgUtils.asVar("", "name"),
                ArgUtils.asFfiAlias("a", true));
        ArgAggFn expectedValue = new ArgAggFn(FfiAggOpt.Count, ArgUtils.asFfiAlias("values", false));

        Assert.assertEquals(Collections.singletonList(expectedKey), op.getGroupByKeys().get().applyArg());
        Assert.assertEquals(Collections.singletonList(expectedValue), op.getGroupByValues().get().applyArg());
    }

    @Test
    public void g_V_groupCount_by_a_key_test() {
        Traversal traversal = g.V().as("a").groupCount().by(__.select("a").by("name"));
        GroupOp op = (GroupOp) getApplyWithGroup(traversal).get(0);

        Pair<FfiVariable.ByValue, FfiAlias.ByValue> expectedKey = Pair.with(
                ArgUtils.asVar("a", "name"),
                ArgUtils.asFfiAlias("keys_a_name", false));
        ArgAggFn expectedValue = new ArgAggFn(FfiAggOpt.Count, ArgUtils.asFfiAlias("values", false));

        Assert.assertEquals(Collections.singletonList(expectedKey), op.getGroupByKeys().get().applyArg());
        Assert.assertEquals(Collections.singletonList(expectedValue), op.getGroupByValues().get().applyArg());
    }

    @Test
    public void g_V_group_by_out_count_test() {
        Traversal traversal = g.V().group().by(__.out().count());
        List<InterOpBase> ops = getApplyWithGroup(traversal);
        Assert.assertEquals(2, ops.size());

        ApplyOp applyOp = (ApplyOp) ops.get(0);
        Assert.assertEquals(FfiJoinKind.Inner, applyOp.getJoinKind().get().applyArg());
        InterOpCollection subOps = (InterOpCollection) applyOp.getSubOpCollection().get().applyArg();
        Assert.assertEquals(2, subOps.unmodifiableCollection().size());
        Assert.assertEquals(ArgUtils.asFfiAlias("apply", false), applyOp.getAlias().get().applyArg());

        GroupOp groupOp = (GroupOp) ops.get(1);
        Pair<FfiVariable.ByValue, FfiAlias.ByValue> expectedKey = Pair.with(
                ArgUtils.asVar("apply", ""),
                ArgUtils.asFfiAlias("keys_apply", false));
        ArgAggFn expectedValue = new ArgAggFn(FfiAggOpt.ToList, ArgUtils.asFfiAlias("values", false));
        Assert.assertEquals(Collections.singletonList(expectedKey), groupOp.getGroupByKeys().get().applyArg());
        Assert.assertEquals(Collections.singletonList(expectedValue), groupOp.getGroupByValues().get().applyArg());
    }
}

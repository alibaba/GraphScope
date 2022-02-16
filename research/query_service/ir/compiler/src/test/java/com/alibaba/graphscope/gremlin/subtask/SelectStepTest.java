package com.alibaba.graphscope.gremlin.subtask;

import com.alibaba.graphscope.common.intermediate.ArgUtils;
import com.alibaba.graphscope.common.intermediate.InterOpCollection;
import com.alibaba.graphscope.common.intermediate.operator.ApplyOp;
import com.alibaba.graphscope.common.intermediate.operator.InterOpBase;
import com.alibaba.graphscope.common.intermediate.operator.ProjectOp;
import com.alibaba.graphscope.common.jna.type.FfiJoinKind;
import com.alibaba.graphscope.gremlin.InterOpCollectionBuilder;
import com.alibaba.graphscope.gremlin.transform.TraversalParentTransformFactory;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.javatuples.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class SelectStepTest {
    private Graph graph = TinkerFactory.createModern();
    private GraphTraversalSource g = graph.traversal();

    private List<InterOpBase> getApplyWithProject(Traversal traversal) {
        TraversalParent parent = (TraversalParent) traversal.asAdmin().getEndStep();
        return TraversalParentTransformFactory.PROJECT_BY_STEP.apply(parent);
    }

    private InterOpBase generateInterOpFromBuilder(Traversal traversal, int idx) {
        return (new InterOpCollectionBuilder(traversal)).build().unmodifiableCollection().get(idx);
    }

    @Test
    public void g_V_select_one_test() {
        Traversal traversal = g.V().as("a").select("a");
        ProjectOp op = (ProjectOp) getApplyWithProject(traversal).get(0);

        List<Pair> exprWithAlias = (List<Pair>) op.getExprWithAlias().get().applyArg();
        Assert.assertEquals("@a", exprWithAlias.get(0).getValue0());
        Assert.assertEquals(ArgUtils.asNoneAlias(), exprWithAlias.get(0).getValue1());
    }

    @Test
    public void g_V_select_one_as_test() {
        Traversal traversal = g.V().as("a").select("a").as("b");
        ProjectOp op = (ProjectOp) generateInterOpFromBuilder(traversal, 1);

        List<Pair> exprWithAlias = (List<Pair>) op.getExprWithAlias().get().applyArg();
        Assert.assertEquals("@a", exprWithAlias.get(0).getValue0());
        Assert.assertEquals(ArgUtils.asFfiAlias("b", true), exprWithAlias.get(0).getValue1());
    }

    @Test
    public void g_V_select_one_by_key_test() {
        Traversal traversal = g.V().as("a").select("a").by("name");
        ProjectOp op = (ProjectOp) getApplyWithProject(traversal).get(0);

        List<Pair> exprWithAlias = (List<Pair>) op.getExprWithAlias().get().applyArg();
        Assert.assertEquals("@a.name", exprWithAlias.get(0).getValue0());
        Assert.assertEquals(ArgUtils.asNoneAlias(), exprWithAlias.get(0).getValue1());
    }

    @Test
    public void g_V_select_one_by_valueMap_key_test() {
        Traversal traversal = g.V().as("a").select("a").by(__.valueMap("name"));
        ProjectOp op = (ProjectOp) getApplyWithProject(traversal).get(0);

        List<Pair> exprWithAlias = (List<Pair>) op.getExprWithAlias().get().applyArg();
        Assert.assertEquals("{@a.name}", exprWithAlias.get(0).getValue0());
        Assert.assertEquals(ArgUtils.asNoneAlias(), exprWithAlias.get(0).getValue1());
    }

    @Test
    public void g_V_select_one_by_valueMap_keys_test() {
        Traversal traversal = g.V().as("a").select("a").by(__.valueMap("name", "id"));
        ProjectOp op = (ProjectOp) getApplyWithProject(traversal).get(0);

        List<Pair> exprWithAlias = (List<Pair>) op.getExprWithAlias().get().applyArg();
        Assert.assertEquals("{@a.name, @a.id}", exprWithAlias.get(0).getValue0());
        Assert.assertEquals(ArgUtils.asNoneAlias(), exprWithAlias.get(0).getValue1());
    }

    @Test
    public void g_V_select_test() {
        Traversal traversal = g.V().as("a").out().as("b").select("a", "b");
        ProjectOp op = (ProjectOp) getApplyWithProject(traversal).get(0);

        List<Pair> exprWithAlias = (List<Pair>) op.getExprWithAlias().get().applyArg();

        Pair firstEntry = exprWithAlias.get(0);
        Assert.assertEquals("@a", firstEntry.getValue0());
        Assert.assertEquals(ArgUtils.asFfiAlias("a_2", false), firstEntry.getValue1());

        Pair sndEntry = exprWithAlias.get(1);
        Assert.assertEquals("@b", sndEntry.getValue0());
        Assert.assertEquals(ArgUtils.asFfiAlias("b_2", false), sndEntry.getValue1());
    }

    @Test
    public void g_V_select_key_test() {
        Traversal traversal = g.V().as("a").out().as("b").select("a", "b").by("name");
        ProjectOp op = (ProjectOp) getApplyWithProject(traversal).get(0);

        List<Pair> exprWithAlias = (List<Pair>) op.getExprWithAlias().get().applyArg();

        Pair firstEntry = exprWithAlias.get(0);
        Assert.assertEquals("@a.name", firstEntry.getValue0());
        Assert.assertEquals(ArgUtils.asFfiAlias("a_name_2", false), firstEntry.getValue1());

        Pair sndEntry = exprWithAlias.get(1);
        Assert.assertEquals("@b.name", sndEntry.getValue0());
        Assert.assertEquals(ArgUtils.asFfiAlias("b_name_2", false), sndEntry.getValue1());
    }


    @Test
    public void g_V_select_a_by_out_count_test() {
        Traversal traversal = g.V().as("a").select("a").by(__.out().count());
        List<InterOpBase> ops = getApplyWithProject(traversal);

        Assert.assertEquals(2, ops.size());

        ApplyOp applyOp = (ApplyOp) ops.get(0);
        Assert.assertEquals(FfiJoinKind.Inner, applyOp.getJoinKind().get().applyArg());
        InterOpCollection subOps = (InterOpCollection) applyOp.getSubOpCollection().get().applyArg();
        Assert.assertEquals(3, subOps.unmodifiableCollection().size());
        Assert.assertEquals(ArgUtils.asFfiAlias("a_apply_0", false), applyOp.getAlias().get().applyArg());

        ProjectOp projectOp = (ProjectOp) ops.get(1);
        List<Pair> exprWithAlias = (List<Pair>) projectOp.getExprWithAlias().get().applyArg();
        Assert.assertEquals(1, exprWithAlias.size());

        Pair firstEntry = exprWithAlias.get(0);
        // expression
        Assert.assertEquals("@a_apply_0", firstEntry.getValue0());
        // alias
        Assert.assertEquals(ArgUtils.asNoneAlias(), firstEntry.getValue1());
    }

    @Test
    public void g_V_select_a_b_by_name_by_out_count_test() {
        Traversal traversal = g.V().as("a").out().as("b").select("a", "b").by("name").by(__.out().count());
        List<InterOpBase> ops = getApplyWithProject(traversal);

        Assert.assertEquals(2, ops.size());

        ApplyOp applyOp = (ApplyOp) ops.get(0);
        Assert.assertEquals(FfiJoinKind.Inner, applyOp.getJoinKind().get().applyArg());
        InterOpCollection subOps = (InterOpCollection) applyOp.getSubOpCollection().get().applyArg();
        Assert.assertEquals(3, subOps.unmodifiableCollection().size());
        Assert.assertEquals(ArgUtils.asFfiAlias("b_apply_1", false), applyOp.getAlias().get().applyArg());

        ProjectOp projectOp = (ProjectOp) ops.get(1);
        List<Pair> exprWithAlias = (List<Pair>) projectOp.getExprWithAlias().get().applyArg();
        Assert.assertEquals(2, exprWithAlias.size());

        Pair firstEntry = exprWithAlias.get(0);
        Assert.assertEquals("@a.name", firstEntry.getValue0());
        Assert.assertEquals(ArgUtils.asFfiAlias("a_name_2", false), firstEntry.getValue1());

        Pair sndEntry = exprWithAlias.get(1);
        Assert.assertEquals("@b_apply_1", sndEntry.getValue0());
        Assert.assertEquals(ArgUtils.asFfiAlias("b_apply_1_2", false), sndEntry.getValue1());
    }

    @Test
    public void g_V_select_a_b_by_out_out_count_test() {
        Traversal traversal = g.V().as("a").out().as("b").select("a", "b").by(__.out().out().count());
        List<InterOpBase> ops = getApplyWithProject(traversal);

        Assert.assertEquals(3, ops.size());

        ApplyOp apply1 = (ApplyOp) ops.get(0);
        Assert.assertEquals(FfiJoinKind.Inner, apply1.getJoinKind().get().applyArg());
        InterOpCollection subOps = (InterOpCollection) apply1.getSubOpCollection().get().applyArg();
        Assert.assertEquals(4, subOps.unmodifiableCollection().size());
        Assert.assertEquals(ArgUtils.asFfiAlias("a_apply_0", false), apply1.getAlias().get().applyArg());

        ApplyOp apply2 = (ApplyOp) ops.get(1);
        Assert.assertEquals(FfiJoinKind.Inner, apply2.getJoinKind().get().applyArg());
        subOps = (InterOpCollection) apply2.getSubOpCollection().get().applyArg();
        Assert.assertEquals(4, subOps.unmodifiableCollection().size());
        Assert.assertEquals(ArgUtils.asFfiAlias("b_apply_1", false), apply2.getAlias().get().applyArg());

        ProjectOp projectOp = (ProjectOp) ops.get(2);
        List<Pair> exprWithAlias = (List<Pair>) projectOp.getExprWithAlias().get().applyArg();
        Assert.assertEquals(2, exprWithAlias.size());

        Pair firstEntry = exprWithAlias.get(0);
        Assert.assertEquals("@a_apply_0", firstEntry.getValue0());
        Assert.assertEquals(ArgUtils.asFfiAlias("a_apply_0_2", false), firstEntry.getValue1());

        Pair sndEntry = exprWithAlias.get(1);
        Assert.assertEquals("@b_apply_1", sndEntry.getValue0());
        Assert.assertEquals(ArgUtils.asFfiAlias("b_apply_1_2", false), sndEntry.getValue1());
    }
}

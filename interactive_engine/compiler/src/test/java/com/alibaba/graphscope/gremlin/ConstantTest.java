package com.alibaba.graphscope.gremlin;

import com.alibaba.graphscope.common.intermediate.ArgUtils;
import com.alibaba.graphscope.common.intermediate.operator.ProjectOp;
import com.alibaba.graphscope.gremlin.plugin.traversal.IrCustomizedTraversalSource;
import com.alibaba.graphscope.gremlin.transform.StepTransformFactory;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.javatuples.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class ConstantTest {
    private Graph graph = TinkerFactory.createModern();
    private GraphTraversalSource g = graph.traversal(IrCustomizedTraversalSource.class);

    @Test
    public void g_V_constant_str_test() {
        Traversal traversal = g.V().constant("marko");
        Step step = traversal.asAdmin().getEndStep();
        ProjectOp op = (ProjectOp) StepTransformFactory.CONSTANT_STEP.apply(step);

        List<Pair> exprWithAlias = (List<Pair>) op.getExprWithAlias().get().applyArg();
        Assert.assertEquals("\"marko\"", exprWithAlias.get(0).getValue0());
        Assert.assertEquals(ArgUtils.asNoneAlias(), exprWithAlias.get(0).getValue1());
    }

    @Test
    public void g_V_constant_id_test() {
        Traversal traversal = g.V().constant(123);
        Step step = traversal.asAdmin().getEndStep();
        ProjectOp op = (ProjectOp) StepTransformFactory.CONSTANT_STEP.apply(step);

        List<Pair> exprWithAlias = (List<Pair>) op.getExprWithAlias().get().applyArg();
        Assert.assertEquals("123", exprWithAlias.get(0).getValue0());
        Assert.assertEquals(ArgUtils.asNoneAlias(), exprWithAlias.get(0).getValue1());
    }

    @Test
    public void g_V_constant_float_test() {
        Traversal traversal = g.V().constant(0.4);
        Step step = traversal.asAdmin().getEndStep();
        ProjectOp op = (ProjectOp) StepTransformFactory.CONSTANT_STEP.apply(step);

        List<Pair> exprWithAlias = (List<Pair>) op.getExprWithAlias().get().applyArg();
        Assert.assertEquals("0.4", exprWithAlias.get(0).getValue0());
        Assert.assertEquals(ArgUtils.asNoneAlias(), exprWithAlias.get(0).getValue1());
    }

    @Test
    public void g_V_constant_bool_test() {
        Traversal traversal = g.V().constant(true);
        Step step = traversal.asAdmin().getEndStep();
        ProjectOp op = (ProjectOp) StepTransformFactory.CONSTANT_STEP.apply(step);

        List<Pair> exprWithAlias = (List<Pair>) op.getExprWithAlias().get().applyArg();
        Assert.assertEquals("true", exprWithAlias.get(0).getValue0());
        Assert.assertEquals(ArgUtils.asNoneAlias(), exprWithAlias.get(0).getValue1());
    }
}

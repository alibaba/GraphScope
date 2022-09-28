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

public class IdLabelTest {
    private Graph graph = TinkerFactory.createModern();
    private GraphTraversalSource g = graph.traversal(IrCustomizedTraversalSource.class);

    @Test
    public void g_V_id_test() {
        Traversal traversal = g.V().id();
        Step step = traversal.asAdmin().getEndStep();
        ProjectOp op = (ProjectOp) StepTransformFactory.ID_STEP.apply(step);

        List<Pair> exprWithAlias = (List<Pair>) op.getExprWithAlias().get().applyArg();
        Assert.assertEquals("@.~id", exprWithAlias.get(0).getValue0());
        Assert.assertEquals(ArgUtils.asNoneAlias(), exprWithAlias.get(0).getValue1());
    }

    @Test
    public void g_V_label_test() {
        Traversal traversal = g.V().label();
        Step step = traversal.asAdmin().getEndStep();
        ProjectOp op = (ProjectOp) StepTransformFactory.LABEL_STEP.apply(step);

        List<Pair> exprWithAlias = (List<Pair>) op.getExprWithAlias().get().applyArg();
        Assert.assertEquals("@.~label", exprWithAlias.get(0).getValue0());
        Assert.assertEquals(ArgUtils.asNoneAlias(), exprWithAlias.get(0).getValue1());
    }
}

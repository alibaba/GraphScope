package com.alibaba.graphscope.gremlin;

import com.alibaba.graphscope.common.intermediate.ArgUtils;
import com.alibaba.graphscope.common.intermediate.operator.ProjectOp;
import com.alibaba.graphscope.gremlin.plugin.traversal.IrCustomizedTraversalSource;
import com.alibaba.graphscope.gremlin.transform.StepTransformFactory;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.javatuples.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class ExprStepTest {
    private Graph graph = TinkerFactory.createModern();
    private IrCustomizedTraversalSource g = graph.traversal(IrCustomizedTraversalSource.class);

    @Test
    public void g_V_expr_test() {
        Traversal traversal = g.V().expr("@.age");
        Step exprStep = traversal.asAdmin().getEndStep();
        ProjectOp projectOp = (ProjectOp) StepTransformFactory.EXPR_STEP.apply(exprStep);

        List<Pair> exprWithAlias = (List<Pair>) projectOp.getExprWithAlias().get().applyArg();
        Assert.assertEquals("@.age", exprWithAlias.get(0).getValue0());
        Assert.assertEquals(ArgUtils.asFfiNoneAlias(), exprWithAlias.get(0).getValue1());
    }
}

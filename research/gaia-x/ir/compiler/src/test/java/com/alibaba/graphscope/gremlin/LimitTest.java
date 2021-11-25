package com.alibaba.graphscope.gremlin;

import com.alibaba.graphscope.common.intermediate.operator.LimitOp;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Assert;
import org.junit.Test;

public class LimitTest {
    private Graph graph = TinkerFactory.createModern();
    private GraphTraversalSource g = graph.traversal();

    @Test
    public void g_V_limit_test() {
        Traversal traversal = g.V().limit(1);
        Step limitStep = traversal.asAdmin().getEndStep();
        LimitOp op = (LimitOp) IrPlanBuidler.StepTransformFactory.LIMIT_STEP.apply(limitStep);
        Assert.assertEquals(2, op.getUpper().get().getArg());
    }
}

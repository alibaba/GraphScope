package com.alibaba.graphscope.gremlin;

import com.alibaba.graphscope.common.intermediate.operator.SelectOp;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Assert;
import org.junit.Test;

public class HasStepTest {
    private Graph graph = TinkerFactory.createModern();
    private GraphTraversalSource g = graph.traversal();

    @Test
    public void g_V_property_test() {
        Traversal traversal = g.V().has("name", "marko");
        Step hasStep = traversal.asAdmin().getEndStep();
        SelectOp op = (SelectOp) IrPlanBuidler.StepTransformFactory.HAS_STEP.apply(hasStep);
        Assert.assertEquals("@.name == \"marko\"", op.getPredicate().get().getArg());
    }
}

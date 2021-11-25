package com.alibaba.graphscope.gremlin;

import com.alibaba.graphscope.common.intermediate.operator.ExpandOp;
import com.alibaba.graphscope.common.jna.type.FfiDirection;
import com.alibaba.graphscope.common.jna.type.FfiNameOrId;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class VertexStepTest {
    private Graph graph = TinkerFactory.createModern();
    private GraphTraversalSource g = graph.traversal();

    @Test
    public void g_V_out() {
        Traversal traversal = g.V().out();
        Step vertexStep = traversal.asAdmin().getEndStep();
        ExpandOp op = (ExpandOp) IrPlanBuidler.StepTransformFactory.VERTEX_STEP.apply(vertexStep);
        Assert.assertEquals(FfiDirection.Out, op.getDirection().get().getArg());
        Assert.assertEquals(false, op.getIsEdge().get().getArg());
    }

    @Test
    public void g_V_outE() {
        Traversal traversal = g.V().outE();
        Step vertexStep = traversal.asAdmin().getEndStep();
        ExpandOp op = (ExpandOp) IrPlanBuidler.StepTransformFactory.VERTEX_STEP.apply(vertexStep);
        Assert.assertEquals(FfiDirection.Out, op.getDirection().get().getArg());
        Assert.assertEquals(true, op.getIsEdge().get().getArg());
    }

    @Test
    public void g_V_out_label() {
        Traversal traversal = g.V().out("knows");
        Step vertexStep = traversal.asAdmin().getEndStep();
        ExpandOp op = (ExpandOp) IrPlanBuidler.StepTransformFactory.VERTEX_STEP.apply(vertexStep);
        FfiNameOrId.ByValue label = ((List<FfiNameOrId.ByValue>) op.getLabels().get().getArg()).get(0);
        Assert.assertEquals("knows", label.name);
    }
}

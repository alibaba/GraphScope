package com.alibaba.graphscope.gremlin;

import com.alibaba.graphscope.common.intermediate.operator.ExpandOp;
import com.alibaba.graphscope.common.intermediate.operator.ScanFusionOp;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Assert;
import org.junit.Test;

// test whether Optional<OpArg> in InterOp is empty when parameters in traversal is not set
public class EmptyTest {
    private Graph graph = TinkerFactory.createModern();
    private GraphTraversalSource g = graph.traversal();

    @Test
    public void g_V_has_no_id_test() {
        Traversal traversal = g.V();
        traversal.asAdmin().applyStrategies();
        Step step = traversal.asAdmin().getStartStep();
        ScanFusionOp op = (ScanFusionOp) IrPlanBuidler.StepTransformFactory.TINKER_GRAPH_STEP.apply(step);
        Assert.assertEquals(false, op.getIds().isPresent());
    }

    @Test
    public void g_V_has_no_label_has_no_property_test() {
        Traversal traversal = g.V();
        traversal.asAdmin().applyStrategies();
        Step step = traversal.asAdmin().getStartStep();
        ScanFusionOp op = (ScanFusionOp) IrPlanBuidler.StepTransformFactory.TINKER_GRAPH_STEP.apply(step);
        Assert.assertEquals(false, op.getPredicate().isPresent());
        Assert.assertEquals(false, op.getLabels().isPresent());
    }

    @Test
    public void g_V_has_label_has_no_property_test() {
        Traversal traversal = g.V().hasLabel("person");
        traversal.asAdmin().applyStrategies();
        Step step = traversal.asAdmin().getStartStep();
        ScanFusionOp op = (ScanFusionOp) IrPlanBuidler.StepTransformFactory.TINKER_GRAPH_STEP.apply(step);
        Assert.assertEquals(false, op.getPredicate().isPresent());
        Assert.assertEquals(true, op.getLabels().isPresent());
    }

    @Test
    public void g_V_has_no_label_has_property_test() {
        Traversal traversal = g.V().has("id", 1);
        traversal.asAdmin().applyStrategies();
        Step step = traversal.asAdmin().getStartStep();
        ScanFusionOp op = (ScanFusionOp) IrPlanBuidler.StepTransformFactory.TINKER_GRAPH_STEP.apply(step);
        Assert.assertEquals(true, op.getPredicate().isPresent());
        Assert.assertEquals(false, op.getLabels().isPresent());
    }

    @Test
    public void g_V_outE_has_no_label_test() {
        Traversal traversal = g.V().out();
        traversal.asAdmin().applyStrategies();
        Step step = traversal.asAdmin().getEndStep();
        ExpandOp op = (ExpandOp) IrPlanBuidler.StepTransformFactory.VERTEX_STEP.apply(step);
        Assert.assertEquals(false, op.getLabels().isPresent());
    }
}

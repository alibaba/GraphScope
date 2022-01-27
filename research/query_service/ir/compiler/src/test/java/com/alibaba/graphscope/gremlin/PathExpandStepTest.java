package com.alibaba.graphscope.gremlin;

import com.alibaba.graphscope.common.intermediate.operator.PathExpandOp;
import com.alibaba.graphscope.common.jna.type.FfiDirection;
import com.alibaba.graphscope.common.jna.type.FfiNameOrId;
import com.alibaba.graphscope.gremlin.plugin.traversal.IrCustomizedTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import com.alibaba.graphscope.gremlin.InterOpCollectionBuilder.StepTransformFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class PathExpandStepTest {
    private Graph graph = TinkerFactory.createModern();
    private GraphTraversalSource g = graph.traversal(IrCustomizedTraversalSource.class);

    @Test
    public void g_V_path_expand_test() {
        IrCustomizedTraversalSource g1 = (IrCustomizedTraversalSource) g;
        Traversal traversal = g1.V().out(__.range(1, 2));
        Step step = traversal.asAdmin().getEndStep();
        PathExpandOp op = (PathExpandOp) StepTransformFactory.PATH_EXPAND_STEP.apply(step);

        Assert.assertEquals(FfiDirection.Out, op.getDirection().get().applyArg());
        Assert.assertEquals(false, op.getIsEdge().get().applyArg());
        Assert.assertEquals(2, op.getLower().get().applyArg());
        Assert.assertEquals(3, op.getUpper().get().applyArg());
    }

    @Test
    public void g_V_path_expand_label_test() {
        IrCustomizedTraversalSource g1 = (IrCustomizedTraversalSource) g;
        Traversal traversal = g1.V().out(__.range(1, 2), "knows");
        Step step = traversal.asAdmin().getEndStep();
        PathExpandOp op = (PathExpandOp) StepTransformFactory.PATH_EXPAND_STEP.apply(step);

        Assert.assertEquals(FfiDirection.Out, op.getDirection().get().applyArg());
        Assert.assertEquals(false, op.getIsEdge().get().applyArg());
        Assert.assertEquals(2, op.getLower().get().applyArg());
        Assert.assertEquals(3, op.getUpper().get().applyArg());
        FfiNameOrId.ByValue label = ((List<FfiNameOrId.ByValue>) op.getLabels().get().applyArg()).get(0);
        Assert.assertEquals("knows", label.name);
    }
}

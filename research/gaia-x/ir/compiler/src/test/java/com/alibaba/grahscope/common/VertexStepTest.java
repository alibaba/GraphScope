package com.alibaba.grahscope.common;

import com.alibaba.graphscope.common.intermediate.operator.ExpandOp;
import com.alibaba.graphscope.common.jna.type.FfiDirection;
import com.alibaba.graphscope.common.jna.type.FfiNameOrId;
import com.alibaba.graphscope.gremlin.Gremlin2IrBuilder;
import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class VertexStepTest {
    private Graph graph = EmptyGraph.EmptyGraphFactory.open(new BaseConfiguration());
    private GraphTraversalSource g = graph.traversal();
    private Gremlin2IrBuilder builder = Gremlin2IrBuilder.INSTANCE;

    @Test
    public void g_V_out() {
        Traversal traversal = g.V().out();
        Step vertexStep = traversal.asAdmin().getEndStep();
        ExpandOp op = (ExpandOp) builder.getIrOpSupplier(vertexStep);
        Assert.assertEquals(FfiDirection.Out, op.getDirection().get().getArg());
        Assert.assertEquals(false, op.getEdgeOpt().get().getArg());
    }

    @Test
    public void g_V_outE() {
        Traversal traversal = g.V().outE();
        Step vertexStep = traversal.asAdmin().getEndStep();
        ExpandOp op = (ExpandOp) builder.getIrOpSupplier(vertexStep);
        Assert.assertEquals(FfiDirection.Out, op.getDirection().get().getArg());
        Assert.assertEquals(true, op.getEdgeOpt().get().getArg());
    }

    @Test
    public void g_V_out_label() {
        Traversal traversal = g.V().out("knows");
        Step vertexStep = traversal.asAdmin().getEndStep();
        ExpandOp op = (ExpandOp) builder.getIrOpSupplier(vertexStep);
        FfiNameOrId.ByValue label = ((List<FfiNameOrId.ByValue>) op.getLabels().get().getArg()).get(0);
        Assert.assertEquals("knows", label.name);
    }
}

package com.alibaba.graphscope.gremlin;

import com.alibaba.graphscope.common.intermediate.operator.GetVOp;
import com.alibaba.graphscope.common.jna.type.FfiVOpt;
import com.alibaba.graphscope.gremlin.transform.StepTransformFactory;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Assert;
import org.junit.Test;

public class EdgeVertexStep {
    private Graph graph = TinkerFactory.createModern();
    private GraphTraversalSource g = graph.traversal();

    @Test
    public void g_E_bothV() {
        Traversal traversal = g.E().bothV();
        GetVOp getVOp = (GetVOp) StepTransformFactory.EDGE_VERTEX_STEP.apply(traversal.asAdmin().getEndStep());
        Assert.assertEquals(FfiVOpt.BothV, getVOp.getGetVOpt().get().applyArg());
    }
}

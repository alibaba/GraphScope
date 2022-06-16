package com.alibaba.graphscope.gremlin;

import com.alibaba.graphscope.common.intermediate.ArgUtils;
import com.alibaba.graphscope.common.intermediate.InterOpCollection;
import com.alibaba.graphscope.common.intermediate.operator.DedupOp;
import com.alibaba.graphscope.common.intermediate.operator.GetVOp;
import com.alibaba.graphscope.common.intermediate.operator.SubGraphAsUnionOp;
import com.alibaba.graphscope.common.intermediate.process.SinkGraph;
import com.alibaba.graphscope.common.jna.type.FfiVOpt;
import com.alibaba.graphscope.gremlin.transform.StepTransformFactory;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class SubGraphStepTest {
    private Graph graph = TinkerFactory.createModern();
    private GraphTraversalSource g = graph.traversal();

    @Test
    public void g_E_subgraph() {
        Traversal traversal = g.E().subgraph("graph_1");
        SubGraphAsUnionOp op =
                (SubGraphAsUnionOp)
                        StepTransformFactory.SUBGRAPH_STEP.apply(traversal.asAdmin().getEndStep());

        Assert.assertEquals("graph_1", op.getGraphConfigs().get(SinkGraph.GRAPH_NAME));
        List<InterOpCollection> actualOps = (List) op.getSubOpCollectionList().get().applyArg();

        // empty opCollection means identity()
        Assert.assertTrue(actualOps.get(0).unmodifiableCollection().isEmpty());

        // bothV().dedup()
        GetVOp op1 = (GetVOp) actualOps.get(1).unmodifiableCollection().get(0);
        Assert.assertEquals(FfiVOpt.Both, op1.getGetVOpt().get().applyArg());

        DedupOp op2 = (DedupOp) actualOps.get(1).unmodifiableCollection().get(1);
        Assert.assertEquals(
                Collections.singletonList(ArgUtils.asFfiNoneVar()),
                op2.getDedupKeys().get().applyArg());
    }
}

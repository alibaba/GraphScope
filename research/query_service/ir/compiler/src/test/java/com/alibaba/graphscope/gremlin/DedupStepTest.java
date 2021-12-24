package com.alibaba.graphscope.gremlin;

import com.alibaba.graphscope.common.intermediate.ArgUtils;
import com.alibaba.graphscope.common.intermediate.operator.DedupOp;
import com.alibaba.graphscope.common.jna.type.FfiVariable;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import com.alibaba.graphscope.gremlin.InterOpCollectionBuilder.StepTransformFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class DedupStepTest {
    private Graph graph = TinkerFactory.createModern();
    private GraphTraversalSource g = graph.traversal();

    @Test
    public void g_V_dedup_test() {
        Traversal traversal = g.V().dedup();

        Step step = traversal.asAdmin().getEndStep();
        DedupOp op = (DedupOp) StepTransformFactory.DEDUP_STEP.apply(step);

        FfiVariable.ByValue expectedVar = ArgUtils.asNoneVar();
        Assert.assertEquals(Collections.singletonList(expectedVar), op.getDedupKeys().get().getArg());
    }
}
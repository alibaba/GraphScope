package com.alibaba.graphscope.gremlin;

import com.alibaba.graphscope.common.intermediate.ArgAggFn;
import com.alibaba.graphscope.common.intermediate.ArgUtils;
import com.alibaba.graphscope.common.intermediate.operator.GroupOp;
import com.alibaba.graphscope.common.jna.type.FfiAggOpt;
import com.alibaba.graphscope.gremlin.transform.StepTransformFactory;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class CountStepTest {
    private Graph graph = TinkerFactory.createModern();
    private GraphTraversalSource g = graph.traversal();

    @Test
    public void g_V_count_test() {
        Traversal traversal = g.V().count();
        Step step = traversal.asAdmin().getEndStep();
        GroupOp op = (GroupOp) StepTransformFactory.COUNT_STEP.apply(step);

        Assert.assertEquals(Collections.emptyList(), op.getGroupByKeys().get().applyArg());
        ArgAggFn expectedValue = new ArgAggFn(FfiAggOpt.Count, ArgUtils.asFfiAlias("values", false));
        Assert.assertEquals(Collections.singletonList(expectedValue), op.getGroupByValues().get().applyArg());
    }

    @Test
    public void g_V_count_as_test() {
        Traversal traversal = g.V().count().as("a");
        Step step = traversal.asAdmin().getEndStep();
        GroupOp op = (GroupOp) StepTransformFactory.COUNT_STEP.apply(step);

        Assert.assertEquals(Collections.emptyList(), op.getGroupByKeys().get().applyArg());
        ArgAggFn expectedValue = new ArgAggFn(FfiAggOpt.Count, ArgUtils.asFfiAlias("a", true));
        Assert.assertEquals(Collections.singletonList(expectedValue), op.getGroupByValues().get().applyArg());
    }
}

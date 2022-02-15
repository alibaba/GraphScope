package com.alibaba.graphscope.gremlin.subtask;

import com.alibaba.graphscope.common.intermediate.InterOpCollection;
import com.alibaba.graphscope.common.intermediate.operator.ApplyOp;
import com.alibaba.graphscope.common.intermediate.operator.InterOpBase;
import com.alibaba.graphscope.common.intermediate.operator.SelectOp;
import com.alibaba.graphscope.common.jna.type.FfiJoinKind;
import com.alibaba.graphscope.gremlin.transform.TraversalParentTransformFactory;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class WhereTraversalTest {
    private Graph graph = TinkerFactory.createModern();
    private GraphTraversalSource g = graph.traversal();

    private List<InterOpBase> getApplyOrSelect(Traversal traversal) {
        TraversalParent parent = (TraversalParent) traversal.asAdmin().getEndStep();
        return TraversalParentTransformFactory.WHERE_TRAVERSAL_STEP.apply(parent);
    }

    @Test
    public void g_V_where_values() {
        Traversal traversal = g.V().where(__.values("name"));
        SelectOp selectOp = (SelectOp) getApplyOrSelect(traversal).get(0);

        Assert.assertEquals("@.name", selectOp.getPredicate().get().applyArg());
    }

    @Test
    public void g_V_where_a() {
        Traversal traversal = g.V().as("a").where(__.select("a"));
        SelectOp selectOp = (SelectOp) getApplyOrSelect(traversal).get(0);

        Assert.assertEquals("@a", selectOp.getPredicate().get().applyArg());
    }

    @Test
    public void g_V_where_a_values() {
        Traversal traversal = g.V().as("a").where(__.select("a").by(__.values("name")));
        SelectOp selectOp = (SelectOp) getApplyOrSelect(traversal).get(0);

        Assert.assertEquals("@a.name", selectOp.getPredicate().get().applyArg());
    }

    @Test
    public void g_V_where_as_a() {
        Traversal traversal = g.V().as("a").where(__.as("a"));
        SelectOp selectOp = (SelectOp) getApplyOrSelect(traversal).get(0);

        Assert.assertEquals("@a", selectOp.getPredicate().get().applyArg());
    }

    @Test
    public void g_V_where_out_out() {
        Traversal traversal = g.V().where(__.out().out());
        ApplyOp applyOp = (ApplyOp) getApplyOrSelect(traversal).get(0);

        Assert.assertEquals(FfiJoinKind.Semi, applyOp.getJoinKind().get().applyArg());
        InterOpCollection subOps = (InterOpCollection) applyOp.getSubOpCollection().get().applyArg();
        Assert.assertEquals(2, subOps.unmodifiableCollection().size());
    }

    @Test
    public void g_V_where_as_out_as() {
        Traversal traversal = g.V().as("a").out().as("b").where(__.as("a").out().as("b"));
        ApplyOp applyOp = (ApplyOp) getApplyOrSelect(traversal).get(0);

        Assert.assertEquals(FfiJoinKind.Semi, applyOp.getJoinKind().get().applyArg());
        InterOpCollection subOps = (InterOpCollection) applyOp.getSubOpCollection().get().applyArg();
        Assert.assertEquals(3, subOps.unmodifiableCollection().size());
    }
}

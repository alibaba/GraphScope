package com.alibaba.graphscope.gremlin;

import com.alibaba.graphscope.common.intermediate.ArgUtils;
import com.alibaba.graphscope.common.intermediate.InterOpCollection;
import com.alibaba.graphscope.common.intermediate.operator.AuxiliaOp;
import com.alibaba.graphscope.common.intermediate.process.PropertyDetailsProcessor;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Assert;
import org.junit.Test;

public class TraversalPropertiesTest {
    private Graph graph = TinkerFactory.createModern();
    private GraphTraversalSource g = graph.traversal();

    @Test
    public void g_V_has_property_test() {
        Traversal traversal = g.V().hasLabel("Person").has("id", 17592186052613L);
        InterOpCollectionBuilder builder = new InterOpCollectionBuilder(traversal);
        InterOpCollection opCollection = builder.build();
        PropertyDetailsProcessor.INSTANCE.process(opCollection);

        AuxiliaOp op = (AuxiliaOp) opCollection.unmodifiableCollection().get(1);
        Assert.assertEquals(Sets.newHashSet(ArgUtils.strAsNameId("id")), op.getPropertyDetails().get().getArg());
    }

    @Test
    public void g_V_in_has_property_test() {
        Traversal traversal = g.V().in("hasCreator").has("creationDate", P.lte(20121128000000000L));
        InterOpCollectionBuilder builder = new InterOpCollectionBuilder(traversal);
        InterOpCollection opCollection = builder.build();
        PropertyDetailsProcessor.INSTANCE.process(opCollection);

        AuxiliaOp op = (AuxiliaOp) opCollection.unmodifiableCollection().get(2);
        Assert.assertEquals(Sets.newHashSet(ArgUtils.strAsNameId("creationDate")), op.getPropertyDetails().get().getArg());
    }

    @Test
    public void g_V_order_by_property_test() {
        Traversal traversal = g.V().order().by("creationDate", Order.desc).by("id", Order.asc);
        InterOpCollectionBuilder builder = new InterOpCollectionBuilder(traversal);
        InterOpCollection opCollection = builder.build();
        PropertyDetailsProcessor.INSTANCE.process(opCollection);

        AuxiliaOp op = (AuxiliaOp) opCollection.unmodifiableCollection().get(1);
        Assert.assertEquals(Sets.newHashSet(ArgUtils.strAsNameId("creationDate"), ArgUtils.strAsNameId("id")),
                op.getPropertyDetails().get().getArg());
    }

    @Test
    public void g_V_select_tag_property_test() {
        Traversal traversal = g.V().as("a").out().as("b").select("a", "b").by(__.valueMap("name", "id", "age"));
        InterOpCollectionBuilder builder = new InterOpCollectionBuilder(traversal);
        InterOpCollection opCollection = builder.build();
        PropertyDetailsProcessor.INSTANCE.process(opCollection);

        AuxiliaOp op1 = (AuxiliaOp) opCollection.unmodifiableCollection().get(1);
        Assert.assertEquals(Sets.newHashSet(ArgUtils.strAsNameId("name"), ArgUtils.strAsNameId("id"), ArgUtils.strAsNameId("age")),
                op1.getPropertyDetails().get().getArg());

        AuxiliaOp op2 = (AuxiliaOp) opCollection.unmodifiableCollection().get(3);
        Assert.assertEquals(Sets.newHashSet(ArgUtils.strAsNameId("name"), ArgUtils.strAsNameId("id"), ArgUtils.strAsNameId("age")),
                op2.getPropertyDetails().get().getArg());
    }
}
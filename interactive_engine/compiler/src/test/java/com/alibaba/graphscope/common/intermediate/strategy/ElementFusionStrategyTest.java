package com.alibaba.graphscope.common.intermediate.strategy;

import com.alibaba.graphscope.common.IrPlan;
import com.alibaba.graphscope.common.intermediate.InterOpCollection;
import com.alibaba.graphscope.common.store.IrMeta;
import com.alibaba.graphscope.common.utils.FileUtils;
import com.alibaba.graphscope.gremlin.InterOpCollectionBuilder;
import com.alibaba.graphscope.gremlin.antlr4.__;
import com.alibaba.graphscope.gremlin.plugin.traversal.IrCustomizedTraversalSource;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class ElementFusionStrategyTest {
    private IrPlan irPlan;
    private Graph graph = TinkerFactory.createModern();
    private GraphTraversalSource g = graph.traversal(IrCustomizedTraversalSource.class);

    // group().by(outE().count())
    @Test
    public void g_V_group_by_outE_count_test() {
        Traversal traversal = g.V().group().by(__.outE().count());
        InterOpCollection opCollection = (new InterOpCollectionBuilder(traversal)).build();
        InterOpCollection.applyStrategies(opCollection);
        irPlan = new IrPlan(new IrMeta(""), opCollection, "");
        String actual = irPlan.getPlanAsJson();
        Assert.assertEquals(FileUtils.readJsonFromResource("group_key_out_count.json"), actual);
    }

    // group().by(outE().has('name', 'marko').count())
    @Test
    public void g_V_group_by_outE_has_count_test() {
        Traversal traversal = g.V().group().by(__.outE().has("name", "marko").count());
        InterOpCollection opCollection = (new InterOpCollectionBuilder(traversal)).build();
        InterOpCollection.applyStrategies(opCollection);
        irPlan = new IrPlan(new IrMeta(""), opCollection, "");
        String actual = irPlan.getPlanAsJson();
        Assert.assertEquals(FileUtils.readJsonFromResource("group_key_out_has_count.json"), actual);
    }

    // group().by(out().count())
    @Test
    public void g_V_group_by_out_count_test() {
        Traversal traversal = g.V().group().by(__.out().count());
        InterOpCollection opCollection = (new InterOpCollectionBuilder(traversal)).build();
        InterOpCollection.applyStrategies(opCollection);
        irPlan = new IrPlan(new IrMeta(""), opCollection, "");
        String actual = irPlan.getPlanAsJson();
        Assert.assertEquals(FileUtils.readJsonFromResource("group_key_out_count.json"), actual);
    }

    @After
    public void after() {
        if (irPlan != null) {
            irPlan.close();
        }
    }
}

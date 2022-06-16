package com.alibaba.graphscope.common.intermediate.operator;

import com.alibaba.graphscope.common.IrPlan;
import com.alibaba.graphscope.common.intermediate.InterOpCollection;
import com.alibaba.graphscope.common.store.IrMeta;
import com.alibaba.graphscope.common.utils.FileUtils;
import com.alibaba.graphscope.gremlin.InterOpCollectionBuilder;
import com.alibaba.graphscope.gremlin.plugin.processor.IrStandardOpProcessor;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class SubGraphAsUnionOpTest {
    private IrPlan irPlan;
    private Graph graph = TinkerFactory.createModern();
    private GraphTraversalSource g = graph.traversal();

    // g.E().subgraph()
    @Test
    public void subgraphOpTest() throws Exception {
        Traversal traversal = g.E().subgraph("graph_1");
        IrStandardOpProcessor.applyStrategies(traversal);
        InterOpCollection ops = (new InterOpCollectionBuilder(traversal)).build();
        InterOpCollection.process(ops);
        irPlan = new IrPlan(new IrMeta(""), ops);
        String actual = irPlan.getPlanAsJson();
        Assert.assertEquals(FileUtils.readJsonFromResource("subgraph.json"), actual);
    }

    @After
    public void after() {
        if (irPlan != null) {
            irPlan.close();
        }
    }
}

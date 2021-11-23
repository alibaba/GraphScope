package com.alibaba.grahscope.common;

import com.alibaba.graphscope.common.jna.IrCoreLibrary;
import com.alibaba.graphscope.common.jna.type.FfiJobBuffer;
import com.alibaba.graphscope.gremlin.Gremlin2IrBuilder;
import com.sun.jna.Pointer;
import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.net.URL;

public class GraphStepTest {
    private IrCoreLibrary irCoreLib = IrCoreLibrary.INSTANCE;

    // private Graph graph = EmptyGraph.EmptyGraphFactory.open(new BaseConfiguration());
    private Graph graph = TinkerFactory.createModern();
    private GraphTraversalSource g = graph.traversal();
    private Gremlin2IrBuilder builder = Gremlin2IrBuilder.INSTANCE;

    @Test
    public void g_V_test() {
        Traversal traversal = g.V();
        Pointer ptrPlan = builder.apply(traversal);
        FfiJobBuffer buffer = irCoreLib.buildPhysicalPlan(ptrPlan);
        Assert.assertArrayEquals(readBytesFromFile("g_V.bytes"), buffer.getBytes());
        buffer.close();
        irCoreLib.destroyLogicalPlan(ptrPlan);
    }

    @Test
    public void g_E_test() {
        Traversal traversal = g.E();
        Pointer ptrPlan = builder.apply(traversal);
        FfiJobBuffer buffer = irCoreLib.buildPhysicalPlan(ptrPlan);
        Assert.assertArrayEquals(readBytesFromFile("g_E.bytes"), buffer.getBytes());
        buffer.close();
        irCoreLib.destroyLogicalPlan(ptrPlan);
    }

    @Test
    public void g_V_label_test() {
        Traversal traversal = g.V().hasLabel("person");
        Pointer ptrPlan = builder.apply(traversal);
        irCoreLib.destroyLogicalPlan(ptrPlan);
    }

    @Test
    public void g_V_id_test() {
        Traversal traversal = g.V(1L);
        Pointer ptrPlan = builder.apply(traversal);
        FfiJobBuffer buffer = irCoreLib.buildPhysicalPlan(ptrPlan);
        Assert.assertArrayEquals(readBytesFromFile("g_V_id.bytes"), buffer.getBytes());
        buffer.close();
        irCoreLib.destroyLogicalPlan(ptrPlan);
    }

    @Test
    public void g_V_property_test() {
        Traversal traversal = g.V().has("name", "marko");
        Pointer ptrPlan = builder.apply(traversal);
        irCoreLib.destroyLogicalPlan(ptrPlan);
    }

    public static byte[] readBytesFromFile(String file) {
        try {
            URL url = GraphStepTest.class.getClassLoader().getResource(file);
            return FileUtils.readFileToByteArray(new File(url.toURI()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

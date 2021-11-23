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
import org.junit.Test;

import java.io.File;

public class VertexStepTest {
    private IrCoreLibrary irCoreLib = IrCoreLibrary.INSTANCE;

    private Graph graph = EmptyGraph.EmptyGraphFactory.open(new BaseConfiguration());
    private GraphTraversalSource g = graph.traversal();
    private Gremlin2IrBuilder builder = Gremlin2IrBuilder.INSTANCE;

    @Test
    public void g_V_out() throws Exception {
        Traversal traversal = g.V().out();
        Pointer ptrPlan = builder.apply(traversal);
        FfiJobBuffer buffer = irCoreLib.buildPhysicalPlan(ptrPlan);
        FileUtils.writeByteArrayToFile(new File("src/test/resources/g_V_out.bytes"), buffer.getBytes());
        irCoreLib.destroyLogicalPlan(ptrPlan);
    }

    @Test
    public void g_V_outE() throws Exception {
        Traversal traversal = g.V().outE();
        Pointer ptrPlan = builder.apply(traversal);
        FfiJobBuffer buffer = irCoreLib.buildPhysicalPlan(ptrPlan);
        FileUtils.writeByteArrayToFile(new File("src/test/resources/g_V_outE.bytes"), buffer.getBytes());
        irCoreLib.destroyLogicalPlan(ptrPlan);
    }

    @Test
    public void g_V_out_label() throws Exception {
        Traversal traversal = g.V().out("knows");
        Pointer ptrPlan = builder.apply(traversal);
        FfiJobBuffer buffer = irCoreLib.buildPhysicalPlan(ptrPlan);
        FileUtils.writeByteArrayToFile(new File("src/test/resources/g_V_out_label.bytes"), buffer.getBytes());
        irCoreLib.destroyLogicalPlan(ptrPlan);
    }
}

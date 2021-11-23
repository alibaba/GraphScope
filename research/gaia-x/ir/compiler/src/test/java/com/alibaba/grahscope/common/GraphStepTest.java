package com.alibaba.grahscope.common;

import com.alibaba.graphscope.common.intermediate.operator.ScanFusionOp;
import com.alibaba.graphscope.common.jna.type.FfiConst;
import com.alibaba.graphscope.common.jna.type.FfiNameOrId;
import com.alibaba.graphscope.common.jna.type.FfiScanOpt;
import com.alibaba.graphscope.gremlin.Gremlin2IrBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.net.URL;
import java.util.List;

public class GraphStepTest {
    private Graph graph = TinkerFactory.createModern();
    private GraphTraversalSource g = graph.traversal();
    private Gremlin2IrBuilder builder = Gremlin2IrBuilder.INSTANCE;

    @Test
    public void g_V_test() {
        Traversal traversal = g.V();
        Step graphStep = traversal.asAdmin().getStartStep();
        ScanFusionOp op = (ScanFusionOp) builder.getIrOpSupplier(graphStep);
        Assert.assertEquals(FfiScanOpt.Vertex, op.getScanOpt().get().getArg());
    }

    @Test
    public void g_E_test() {
        Traversal traversal = g.E();
        Step graphStep = traversal.asAdmin().getStartStep();
        ScanFusionOp op = (ScanFusionOp) builder.getIrOpSupplier(graphStep);
        Assert.assertEquals(FfiScanOpt.Edge, op.getScanOpt().get().getArg());
    }

    @Test
    public void g_V_label_test() {
        Traversal traversal = g.V().hasLabel("person");
        traversal.asAdmin().applyStrategies();
        Step graphStep = traversal.asAdmin().getStartStep();
        ScanFusionOp op = (ScanFusionOp) builder.getIrOpSupplier(graphStep);
        FfiNameOrId.ByValue ffiLabel = ((List<FfiNameOrId.ByValue>) op.getLabels().get().getArg()).get(0);
        Assert.assertEquals("person", ffiLabel.name);
    }

    @Test
    public void g_V_id_test() {
        Traversal traversal = g.V(1L);
        Step graphStep = traversal.asAdmin().getStartStep();
        ScanFusionOp op = (ScanFusionOp) builder.getIrOpSupplier(graphStep);
        FfiConst.ByValue ffiId = ((List<FfiConst.ByValue>) op.getIds().get().getArg()).get(0);
        Assert.assertEquals(1L, ffiId.int64);
    }

    @Test
    public void g_V_property_test() {
        Traversal traversal = g.V().has("name", "marko");
        traversal.asAdmin().applyStrategies();
        Step graphStep = traversal.asAdmin().getStartStep();
        ScanFusionOp op = (ScanFusionOp) builder.getIrOpSupplier(graphStep);
        String expr = (String) op.getPredicate().get().getArg();
        Assert.assertEquals("@.name == \"marko\"", expr);
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

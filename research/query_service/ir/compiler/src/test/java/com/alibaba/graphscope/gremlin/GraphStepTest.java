/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.gremlin;

import com.alibaba.graphscope.common.intermediate.ArgUtils;
import com.alibaba.graphscope.common.intermediate.operator.InterOpBase;
import com.alibaba.graphscope.common.intermediate.operator.ScanFusionOp;
import com.alibaba.graphscope.common.jna.type.FfiAlias;
import com.alibaba.graphscope.common.jna.type.FfiConst;
import com.alibaba.graphscope.common.jna.type.FfiNameOrId;
import com.alibaba.graphscope.common.jna.type.FfiScanOpt;
import com.alibaba.graphscope.gremlin.plugin.processor.IrStandardOpProcessor;
import com.alibaba.graphscope.gremlin.transform.StepTransformFactory;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class GraphStepTest {
    private Graph graph = TinkerFactory.createModern();
    private GraphTraversalSource g = graph.traversal();

    @Test
    public void g_V_test() {
        Traversal traversal = g.V();
        Step graphStep = traversal.asAdmin().getStartStep();
        ScanFusionOp op = (ScanFusionOp) StepTransformFactory.GRAPH_STEP.apply(graphStep);
        Assert.assertEquals(FfiScanOpt.Entity, op.getScanOpt().get().applyArg());
    }

    @Test
    public void g_E_test() {
        Traversal traversal = g.E();
        Step graphStep = traversal.asAdmin().getStartStep();
        ScanFusionOp op = (ScanFusionOp) StepTransformFactory.GRAPH_STEP.apply(graphStep);
        Assert.assertEquals(FfiScanOpt.Relation, op.getScanOpt().get().applyArg());
    }

    @Test
    public void g_V_label_test() {
        Traversal traversal = g.V().hasLabel("person");
        IrStandardOpProcessor.applyStrategies(traversal);
        Step graphStep = traversal.asAdmin().getStartStep();
        ScanFusionOp op = (ScanFusionOp) StepTransformFactory.SCAN_FUSION_STEP.apply(graphStep);
        FfiNameOrId.ByValue ffiLabel = op.getParams().get().getTables().get(0);
        Assert.assertEquals("person", ffiLabel.name);
    }

    @Test
    public void g_V_id_test() {
        Traversal traversal = g.V(1L);
        Step graphStep = traversal.asAdmin().getStartStep();
        ScanFusionOp op = (ScanFusionOp) StepTransformFactory.GRAPH_STEP.apply(graphStep);
        FfiConst.ByValue ffiId = ((List<FfiConst.ByValue>) op.getIds().get().applyArg()).get(0);
        Assert.assertEquals(1L, ffiId.int64);
    }

    @Test
    public void g_V_property_test() {
        Traversal traversal = g.V().has("name", "marko");
        IrStandardOpProcessor.applyStrategies(traversal);
        Step graphStep = traversal.asAdmin().getStartStep();
        ScanFusionOp op = (ScanFusionOp) StepTransformFactory.SCAN_FUSION_STEP.apply(graphStep);
        String expr = op.getParams().get().getPredicate().get();
        Assert.assertEquals("@.name == \"marko\"", expr);
    }

    @Test
    public void g_V_as_test() {
        Traversal traversal = g.V().as("a");
        ScanFusionOp op = (ScanFusionOp) generateInterOpFromBuilder(traversal, 0);
        FfiAlias.ByValue expected = ArgUtils.asFfiAlias("a", true);
        Assert.assertEquals(expected, op.getAlias().get().applyArg());
    }

    @Test
    public void g_E_as_test() {
        Traversal traversal = g.E().as("a");
        ScanFusionOp op = (ScanFusionOp) generateInterOpFromBuilder(traversal, 0);
        FfiAlias.ByValue expected = ArgUtils.asFfiAlias("a", true);
        Assert.assertEquals(expected, op.getAlias().get().applyArg());
    }

    // to check the query `g.V().has("person", "name", "marko")` in ci tests
    @Test
    public void g_V_label_property_test() {
        Traversal traversal = g.V().has("person", "name", "marko");
        IrStandardOpProcessor.applyStrategies(traversal);
        Step graphStep = traversal.asAdmin().getStartStep();
        ScanFusionOp op = (ScanFusionOp) StepTransformFactory.SCAN_FUSION_STEP.apply(graphStep);
        // predicate is "@.name == \"marko\""
        String expr = op.getParams().get().getPredicate().get();
        Assert.assertEquals("@.name == \"marko\"", expr);
        // table is "person"
        FfiNameOrId.ByValue table = op.getParams().get().getTables().get(0);
        Assert.assertEquals(ArgUtils.asFfiTag("person"), table);
        // index_predicate is null
        Assert.assertEquals(false, op.getIds().isPresent());
    }

    private InterOpBase generateInterOpFromBuilder(Traversal traversal, int idx) {
        return (new InterOpCollectionBuilder(traversal)).build().unmodifiableCollection().get(idx);
    }
}

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

import com.alibaba.graphscope.common.intermediate.operator.ExpandOp;
import com.alibaba.graphscope.common.intermediate.operator.ScanFusionOp;
import com.alibaba.graphscope.gremlin.plugin.processor.IrStandardOpProcessor;
import com.alibaba.graphscope.gremlin.transform.StepTransformFactory;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Assert;
import org.junit.Test;

// test whether Optional<OpArg> in InterOp is empty when parameters in traversal is not set
public class EmptyTest {
    private Graph graph = TinkerFactory.createModern();
    private GraphTraversalSource g = graph.traversal();

    @Test
    public void g_V_has_no_id_test() {
        Traversal traversal = g.V();
        IrStandardOpProcessor.applyStrategies(traversal);
        Step step = traversal.asAdmin().getStartStep();
        ScanFusionOp op = (ScanFusionOp) StepTransformFactory.SCAN_FUSION_STEP.apply(step);
        Assert.assertEquals(false, op.getIds().isPresent());
    }

    @Test
    public void g_V_has_no_label_has_no_property_test() {
        Traversal traversal = g.V();
        IrStandardOpProcessor.applyStrategies(traversal);
        Step step = traversal.asAdmin().getStartStep();
        ScanFusionOp op = (ScanFusionOp) StepTransformFactory.SCAN_FUSION_STEP.apply(step);
        Assert.assertEquals(false, op.getPredicate().isPresent());
        Assert.assertEquals(false, op.getLabels().isPresent());
    }

    @Test
    public void g_V_has_label_has_no_property_test() {
        Traversal traversal = g.V().hasLabel("person");
        IrStandardOpProcessor.applyStrategies(traversal);
        Step step = traversal.asAdmin().getStartStep();
        ScanFusionOp op = (ScanFusionOp) StepTransformFactory.SCAN_FUSION_STEP.apply(step);
        Assert.assertEquals(false, op.getPredicate().isPresent());
        Assert.assertEquals(true, op.getLabels().isPresent());
    }

    @Test
    public void g_V_has_no_label_has_property_test() {
        Traversal traversal = g.V().has("id", 1);
        IrStandardOpProcessor.applyStrategies(traversal);
        Step step = traversal.asAdmin().getStartStep();
        ScanFusionOp op = (ScanFusionOp) StepTransformFactory.SCAN_FUSION_STEP.apply(step);
        Assert.assertEquals(true, op.getPredicate().isPresent());
        Assert.assertEquals(false, op.getLabels().isPresent());
    }

    @Test
    public void g_V_outE_has_no_label_test() {
        Traversal traversal = g.V().out();
        IrStandardOpProcessor.applyStrategies(traversal);
        Step step = traversal.asAdmin().getEndStep();
        ExpandOp op = (ExpandOp) StepTransformFactory.VERTEX_STEP.apply(step);
        Assert.assertEquals(false, op.getLabels().isPresent());
    }
}

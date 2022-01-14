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

import com.alibaba.graphscope.common.intermediate.operator.SelectOp;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import com.alibaba.graphscope.gremlin.InterOpCollectionBuilder.StepTransformFactory;
import org.junit.Assert;
import org.junit.Test;

public class HasStepTest {
    private Graph graph = TinkerFactory.createModern();
    private GraphTraversalSource g = graph.traversal();

    @Test
    public void g_V_property_test() {
        Traversal traversal = g.V().has("name", "marko");
        Step hasStep = traversal.asAdmin().getEndStep();
        SelectOp op = (SelectOp) StepTransformFactory.HAS_STEP.apply(hasStep);
        Assert.assertEquals("@.name && @.name == \"marko\"", op.getPredicate().get().getArg());
    }

    @Test
    public void g_V_has_eq_test() {
        Traversal traversal = g.V().has("name", P.eq("marko"));
        Step hasStep = traversal.asAdmin().getEndStep();
        SelectOp op = (SelectOp) StepTransformFactory.HAS_STEP.apply(hasStep);
        Assert.assertEquals("@.name && @.name == \"marko\"", op.getPredicate().get().getArg());
    }

    @Test
    public void g_V_has_neq_test() {
        Traversal traversal = g.V().has("name", P.neq("marko"));
        Step hasStep = traversal.asAdmin().getEndStep();
        SelectOp op = (SelectOp) StepTransformFactory.HAS_STEP.apply(hasStep);
        Assert.assertEquals("@.name && @.name != \"marko\"", op.getPredicate().get().getArg());
    }

    @Test
    public void g_V_has_lt_test() {
        Traversal traversal = g.V().has("age", P.lt(10));
        Step hasStep = traversal.asAdmin().getEndStep();
        SelectOp op = (SelectOp) StepTransformFactory.HAS_STEP.apply(hasStep);
        Assert.assertEquals("@.age && @.age < 10", op.getPredicate().get().getArg());
    }

    @Test
    public void g_V_has_lte_test() {
        Traversal traversal = g.V().has("age", P.lte(10));
        Step hasStep = traversal.asAdmin().getEndStep();
        SelectOp op = (SelectOp) StepTransformFactory.HAS_STEP.apply(hasStep);
        Assert.assertEquals("@.age && @.age <= 10", op.getPredicate().get().getArg());
    }

    @Test
    public void g_V_has_gt_test() {
        Traversal traversal = g.V().has("age", P.gt(10));
        Step hasStep = traversal.asAdmin().getEndStep();
        SelectOp op = (SelectOp) StepTransformFactory.HAS_STEP.apply(hasStep);
        Assert.assertEquals("@.age && @.age > 10", op.getPredicate().get().getArg());
    }

    @Test
    public void g_V_has_gte_test() {
        Traversal traversal = g.V().has("age", P.gte(10));
        Step hasStep = traversal.asAdmin().getEndStep();
        SelectOp op = (SelectOp) StepTransformFactory.HAS_STEP.apply(hasStep);
        Assert.assertEquals("@.age && @.age >= 10", op.getPredicate().get().getArg());
    }

    @Test
    public void g_V_has_within_int_test() {
        Traversal traversal = g.V().has("age", P.within(10));
        Step hasStep = traversal.asAdmin().getEndStep();
        SelectOp op = (SelectOp) StepTransformFactory.HAS_STEP.apply(hasStep);
        Assert.assertEquals("@.age && @.age within [10]", op.getPredicate().get().getArg());
    }

    @Test
    public void g_V_has_within_ints_test() {
        Traversal traversal = g.V().has("age", P.within(10, 11));
        Step hasStep = traversal.asAdmin().getEndStep();
        SelectOp op = (SelectOp) StepTransformFactory.HAS_STEP.apply(hasStep);
        Assert.assertEquals("@.age && @.age within [10, 11]", op.getPredicate().get().getArg());
    }

    @Test
    public void g_V_has_without_int_test() {
        Traversal traversal = g.V().has("age", P.without(10));
        Step hasStep = traversal.asAdmin().getEndStep();
        SelectOp op = (SelectOp) StepTransformFactory.HAS_STEP.apply(hasStep);
        Assert.assertEquals("@.age && @.age without [10]", op.getPredicate().get().getArg());
    }

    @Test
    public void g_V_has_without_ints_test() {
        Traversal traversal = g.V().has("age", P.without(10, 11));
        Step hasStep = traversal.asAdmin().getEndStep();
        SelectOp op = (SelectOp) StepTransformFactory.HAS_STEP.apply(hasStep);
        Assert.assertEquals("@.age && @.age without [10, 11]", op.getPredicate().get().getArg());
    }

    @Test
    public void g_V_has_within_strs_test() {
        Traversal traversal = g.V().has("name", P.within("marko", "josh"));
        Step hasStep = traversal.asAdmin().getEndStep();
        SelectOp op = (SelectOp) StepTransformFactory.HAS_STEP.apply(hasStep);
        Assert.assertEquals("@.name && @.name within [\"marko\", \"josh\"]", op.getPredicate().get().getArg());
    }

    @Test
    public void g_V_has_without_strs_test() {
        Traversal traversal = g.V().has("name", P.without("marko", "josh"));
        Step hasStep = traversal.asAdmin().getEndStep();
        SelectOp op = (SelectOp) StepTransformFactory.HAS_STEP.apply(hasStep);
        Assert.assertEquals("@.name && @.name without [\"marko\", \"josh\"]", op.getPredicate().get().getArg());
    }
}

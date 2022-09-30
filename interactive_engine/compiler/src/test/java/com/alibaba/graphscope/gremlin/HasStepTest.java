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
import com.alibaba.graphscope.gremlin.transform.StepTransformFactory;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.TextP;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
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
        Assert.assertEquals("@.name == \"marko\"", op.getPredicate().get().applyArg());
    }

    @Test
    public void g_V_has_eq_test() {
        Traversal traversal = g.V().has("name", P.eq("marko"));
        Step hasStep = traversal.asAdmin().getEndStep();
        SelectOp op = (SelectOp) StepTransformFactory.HAS_STEP.apply(hasStep);
        Assert.assertEquals("@.name == \"marko\"", op.getPredicate().get().applyArg());
    }

    @Test
    public void g_V_has_neq_test() {
        Traversal traversal = g.V().has("name", P.neq("marko"));
        Step hasStep = traversal.asAdmin().getEndStep();
        SelectOp op = (SelectOp) StepTransformFactory.HAS_STEP.apply(hasStep);
        Assert.assertEquals("@.name != \"marko\"", op.getPredicate().get().applyArg());
    }

    @Test
    public void g_V_has_lt_test() {
        Traversal traversal = g.V().has("age", P.lt(10));
        Step hasStep = traversal.asAdmin().getEndStep();
        SelectOp op = (SelectOp) StepTransformFactory.HAS_STEP.apply(hasStep);
        Assert.assertEquals("@.age < 10", op.getPredicate().get().applyArg());
    }

    @Test
    public void g_V_has_lte_test() {
        Traversal traversal = g.V().has("age", P.lte(10));
        Step hasStep = traversal.asAdmin().getEndStep();
        SelectOp op = (SelectOp) StepTransformFactory.HAS_STEP.apply(hasStep);
        Assert.assertEquals("@.age <= 10", op.getPredicate().get().applyArg());
    }

    @Test
    public void g_V_has_gt_test() {
        Traversal traversal = g.V().has("age", P.gt(10));
        Step hasStep = traversal.asAdmin().getEndStep();
        SelectOp op = (SelectOp) StepTransformFactory.HAS_STEP.apply(hasStep);
        Assert.assertEquals("@.age > 10", op.getPredicate().get().applyArg());
    }

    @Test
    public void g_V_has_gte_test() {
        Traversal traversal = g.V().has("age", P.gte(10));
        Step hasStep = traversal.asAdmin().getEndStep();
        SelectOp op = (SelectOp) StepTransformFactory.HAS_STEP.apply(hasStep);
        Assert.assertEquals("@.age >= 10", op.getPredicate().get().applyArg());
    }

    @Test
    public void g_V_has_within_int_test() {
        Traversal traversal = g.V().has("age", P.within(10));
        Step hasStep = traversal.asAdmin().getEndStep();
        SelectOp op = (SelectOp) StepTransformFactory.HAS_STEP.apply(hasStep);
        Assert.assertEquals("@.age within [10]", op.getPredicate().get().applyArg());
    }

    @Test
    public void g_V_has_within_ints_test() {
        Traversal traversal = g.V().has("age", P.within(10, 11));
        Step hasStep = traversal.asAdmin().getEndStep();
        SelectOp op = (SelectOp) StepTransformFactory.HAS_STEP.apply(hasStep);
        Assert.assertEquals("@.age within [10, 11]", op.getPredicate().get().applyArg());
    }

    @Test
    public void g_V_has_without_int_test() {
        Traversal traversal = g.V().has("age", P.without(10));
        Step hasStep = traversal.asAdmin().getEndStep();
        SelectOp op = (SelectOp) StepTransformFactory.HAS_STEP.apply(hasStep);
        Assert.assertEquals("@.age without [10]", op.getPredicate().get().applyArg());
    }

    @Test
    public void g_V_has_without_ints_test() {
        Traversal traversal = g.V().has("age", P.without(10, 11));
        Step hasStep = traversal.asAdmin().getEndStep();
        SelectOp op = (SelectOp) StepTransformFactory.HAS_STEP.apply(hasStep);
        Assert.assertEquals("@.age without [10, 11]", op.getPredicate().get().applyArg());
    }

    @Test
    public void g_V_has_within_strs_test() {
        Traversal traversal = g.V().has("name", P.within("marko", "josh"));
        Step hasStep = traversal.asAdmin().getEndStep();
        SelectOp op = (SelectOp) StepTransformFactory.HAS_STEP.apply(hasStep);
        Assert.assertEquals(
                "@.name within [\"marko\", \"josh\"]", op.getPredicate().get().applyArg());
    }

    @Test
    public void g_V_has_without_strs_test() {
        Traversal traversal = g.V().has("name", P.without("marko", "josh"));
        Step hasStep = traversal.asAdmin().getEndStep();
        SelectOp op = (SelectOp) StepTransformFactory.HAS_STEP.apply(hasStep);
        Assert.assertEquals(
                "@.name without [\"marko\", \"josh\"]", op.getPredicate().get().applyArg());
    }

    @Test
    public void g_V_has_and_p_test() {
        Traversal traversal = g.V().has("age", P.gt(27).and(P.lt(32)));
        Step hasStep = traversal.asAdmin().getEndStep();
        SelectOp op = (SelectOp) StepTransformFactory.HAS_STEP.apply(hasStep);
        Assert.assertEquals("@.age > 27 && (@.age < 32)", op.getPredicate().get().applyArg());
    }

    @Test
    public void g_V_has_or_p_test() {
        Traversal traversal = g.V().has("age", P.lt(27).or(P.gt(32)));
        Step hasStep = traversal.asAdmin().getEndStep();
        SelectOp op = (SelectOp) StepTransformFactory.HAS_STEP.apply(hasStep);
        Assert.assertEquals("@.age < 27 || (@.age > 32)", op.getPredicate().get().applyArg());
    }

    @Test
    public void g_V_values_is_eq_test() {
        Traversal traversal = g.V().values("age").is(P.eq(27));
        Step hasStep = traversal.asAdmin().getEndStep();
        SelectOp op = (SelectOp) StepTransformFactory.IS_STEP.apply(hasStep);
        Assert.assertEquals("@ == 27", op.getPredicate().get().applyArg());
    }

    @Test
    public void g_V_values_is_and_p_test() {
        Traversal traversal = g.V().values("age").is(P.gt(27).and(P.lt(32)));
        Step hasStep = traversal.asAdmin().getEndStep();
        SelectOp op = (SelectOp) StepTransformFactory.IS_STEP.apply(hasStep);
        Assert.assertEquals("@ > 27 && (@ < 32)", op.getPredicate().get().applyArg());
    }

    @Test
    public void g_V_has_has_test() {
        Traversal traversal = g.V().has("name", "marko").has("id", 1);
        Step hasStep = traversal.asAdmin().getEndStep();
        SelectOp op = (SelectOp) StepTransformFactory.HAS_STEP.apply(hasStep);
        Assert.assertEquals(
                "@.name == \"marko\" && (@.id == 1)", op.getPredicate().get().applyArg());
    }

    @Test
    public void g_V_has_containing_str_test() {
        Traversal traversal = g.V().has("name", TextP.containing("marko"));
        Step hasStep = traversal.asAdmin().getEndStep();
        SelectOp op = (SelectOp) StepTransformFactory.HAS_STEP.apply(hasStep);
        Assert.assertEquals("\"marko\" within @.name", op.getPredicate().get().applyArg());
    }

    @Test
    public void g_V_has_notContaining_str_test() {
        Traversal traversal = g.V().has("name", TextP.notContaining("marko"));
        Step hasStep = traversal.asAdmin().getEndStep();
        SelectOp op = (SelectOp) StepTransformFactory.HAS_STEP.apply(hasStep);
        Assert.assertEquals("\"marko\" without @.name", op.getPredicate().get().applyArg());
    }

    @Test
    public void g_V_has_P_not_test() {
        Traversal traversal = g.V().has("name", P.not(P.eq("marko")));
        Step hasStep = traversal.asAdmin().getEndStep();
        SelectOp op = (SelectOp) StepTransformFactory.HAS_STEP.apply(hasStep);
        Assert.assertEquals("@.name != \"marko\"", op.getPredicate().get().applyArg());
    }

    @Test
    public void g_V_has_P_inside_test() {
        Traversal traversal = g.V().has("age", P.inside(20, 30));
        Step hasStep = traversal.asAdmin().getEndStep();
        SelectOp op = (SelectOp) StepTransformFactory.HAS_STEP.apply(hasStep);
        Assert.assertEquals("@.age > 20 && (@.age < 30)", op.getPredicate().get().applyArg());
    }

    @Test
    public void g_V_has_P_outside_test() {
        Traversal traversal = g.V().has("age", P.outside(20, 30));
        Step hasStep = traversal.asAdmin().getEndStep();
        SelectOp op = (SelectOp) StepTransformFactory.HAS_STEP.apply(hasStep);
        Assert.assertEquals("@.age < 20 || (@.age > 30)", op.getPredicate().get().applyArg());
    }
}

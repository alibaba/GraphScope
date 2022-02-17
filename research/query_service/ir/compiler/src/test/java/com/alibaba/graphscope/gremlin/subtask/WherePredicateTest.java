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

package com.alibaba.graphscope.gremlin.subtask;

import com.alibaba.graphscope.common.intermediate.operator.InterOpBase;
import com.alibaba.graphscope.common.intermediate.operator.SelectOp;
import com.alibaba.graphscope.gremlin.transform.TraversalParentTransformFactory;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class WherePredicateTest {
    private Graph graph = TinkerFactory.createModern();
    private GraphTraversalSource g = graph.traversal();

    private List<InterOpBase> getApplyWithSelect(Traversal traversal) {
        TraversalParent parent = (TraversalParent) traversal.asAdmin().getEndStep();
        return TraversalParentTransformFactory.WHERE_BY_STEP.apply(parent);
    }

    @Test
    public void g_V_where_eq_a() {
        Traversal traversal = g.V().as("a").out().where(P.eq("a"));
        SelectOp selectOp = (SelectOp) getApplyWithSelect(traversal).get(0);

        Assert.assertEquals("@ == @a", selectOp.getPredicate().get().applyArg());
    }

    @Test
    public void g_V_where_a_eq_b() {
        Traversal traversal = g.V().as("a").out().as("b").where("a", P.eq("b"));
        SelectOp selectOp = (SelectOp) getApplyWithSelect(traversal).get(0);

        Assert.assertEquals("@a == @b", selectOp.getPredicate().get().applyArg());
    }

    @Test
    public void g_V_where_a_eq_b_or_eq_c() {
        Traversal traversal = g.V().as("a")
                .out().as("b").out().as("c").where("a", P.eq("b").or(P.eq("c")));
        SelectOp selectOp = (SelectOp) getApplyWithSelect(traversal).get(0);

        Assert.assertEquals("@a == @b || (@a == @c)", selectOp.getPredicate().get().applyArg());
    }

    @Test
    public void g_V_where_eq_a_age() {
        Traversal traversal = g.V().as("a").out().where(P.eq("a")).by("age");
        SelectOp selectOp = (SelectOp) getApplyWithSelect(traversal).get(0);

        Assert.assertEquals("@.age && @a.age && @.age == @a.age", selectOp.getPredicate().get().applyArg());
    }

    @Test
    public void g_V_where_eq_a_values_age() {
        Traversal traversal = g.V().as("a").out().where(P.eq("a")).by(__.values("age"));
        SelectOp selectOp = (SelectOp) getApplyWithSelect(traversal).get(0);

        Assert.assertEquals("@.age && @a.age && @.age == @a.age", selectOp.getPredicate().get().applyArg());
    }

    @Test
    public void g_V_where_a_id_eq_b_age() {
        Traversal traversal = g.V().as("a").out().as("b").where("a", P.eq("b")).by("id").by("age");
        SelectOp selectOp = (SelectOp) getApplyWithSelect(traversal).get(0);

        Assert.assertEquals("@a.id && @b.age && @a.id == @b.age", selectOp.getPredicate().get().applyArg());
    }

    @Test
    public void g_V_where_a_id_eq_b_age_or_c_id() {
        Traversal traversal = g.V().as("a")
                .out().as("b").out().as("c").where("a", P.eq("b").or(P.eq("c"))).by("id").by("age").by("id");
        SelectOp selectOp = (SelectOp) getApplyWithSelect(traversal).get(0);

        Assert.assertEquals("@a.id && @b.age && @a.id == @b.age || (@a.id && @c.id && @a.id == @c.id)",
                selectOp.getPredicate().get().applyArg());
    }

    //todo: support subtask, where("a", P.eq("b")).by(out().count())
}

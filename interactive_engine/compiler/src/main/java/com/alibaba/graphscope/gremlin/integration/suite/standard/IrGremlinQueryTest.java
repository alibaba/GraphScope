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

package com.alibaba.graphscope.gremlin.integration.suite.standard;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.dedup;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outE;

import com.alibaba.graphscope.gremlin.plugin.traversal.IrCustomizedTraversal;

import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public abstract class IrGremlinQueryTest extends AbstractGremlinProcessTest {
    public abstract Traversal<Vertex, Map<Object, Long>>
            get_g_V_group_by_by_dedup_count_order_by_key();

    public abstract Traversal<Vertex, Map<Object, List>> get_g_V_group_by_outE_count_order_by_key();

    public abstract Traversal<Vertex, Object> get_g_VX4X_bothE_as_otherV();

    public abstract Traversal<Vertex, Object> get_g_V_hasLabel_hasId_values();

    public abstract Traversal<Vertex, Object>
            get_g_V_out_as_a_in_select_a_as_b_select_b_by_values();

    @Test
    public void g_V_group_by_by_dedup_count_test() {
        Traversal<Vertex, Map<Object, Long>> traversal =
                this.get_g_V_group_by_by_dedup_count_order_by_key();
        this.printTraversalForm(traversal);

        String expected =
                "{v[1]=1, v[2]=1, v[72057594037927939]=1, v[4]=1, v[72057594037927941]=1, v[6]=1}";

        Map<Object, Long> result = traversal.next();
        Assert.assertEquals(expected, result.toString());
        Assert.assertFalse(traversal.hasNext());
    }

    @Test
    public void g_V_group_by_outE_count_test() {
        Traversal<Vertex, Map<Object, List>> traversal =
                this.get_g_V_group_by_outE_count_order_by_key();
        this.printTraversalForm(traversal);
        Map<Object, List> result = traversal.next();
        Assert.assertEquals(4, result.size());
        result.forEach(
                (k, v) -> {
                    Collections.sort(
                            v,
                            (Object o1, Object o2) -> {
                                long v1Id = (long) ((Vertex) o1).id();
                                long v2Id = (long) ((Vertex) o2).id();
                                return (int) (v1Id - v2Id);
                            });
                });
        Assert.assertEquals(
                "{0=[v[2], v[72057594037927939], v[72057594037927941]], 1=[v[6]], 2=[v[4]],"
                        + " 3=[v[1]]}",
                result.toString());
        Assert.assertFalse(traversal.hasNext());
    }

    @Test
    public void g_VX4X_bothE_as_otherV() {
        Traversal<Vertex, Object> traversal = this.get_g_VX4X_bothE_as_otherV();
        this.printTraversalForm(traversal);
        int counter = 0;

        List<String> expected = Arrays.asList("1", "3", "5");

        while (traversal.hasNext()) {
            Object result = traversal.next();
            Assert.assertTrue(expected.contains(result.toString()));
            ++counter;
        }

        Assert.assertEquals(expected.size(), counter);
    }

    @Test
    public void g_V_hasLabel_hasId_values() {
        Traversal<Vertex, Object> traversal = this.get_g_V_hasLabel_hasId_values();
        this.printTraversalForm(traversal);
        int counter = 0;

        String expected = "marko";

        while (traversal.hasNext()) {
            Object result = traversal.next();
            Assert.assertTrue(expected.contains(result.toString()));
            ++counter;
        }

        Assert.assertEquals(1, counter);
    }

    @Test
    public void g_V_out_as_a_in_select_a_as_b_select_b_by_valueMap() {
        Traversal<Vertex, Object> traversal =
                this.get_g_V_out_as_a_in_select_a_as_b_select_b_by_values();
        this.printTraversalForm(traversal);
        int counter = 0;

        List<String> expected = Arrays.asList("lop", "vadas", "josh", "ripple");

        while (traversal.hasNext()) {
            Object result = traversal.next();
            Assert.assertTrue(expected.contains(result.toString()));
            ++counter;
        }

        Assert.assertEquals(12, counter);
    }

    public static class Traversals extends IrGremlinQueryTest {

        @Override
        public Traversal<Vertex, Map<Object, Long>> get_g_V_group_by_by_dedup_count_order_by_key() {
            return (IrCustomizedTraversal)
                    g.V().group()
                            .by()
                            .by(dedup().count())
                            .order()
                            .by(__.select(Column.keys).values("id"), Order.asc);
        }

        @Override
        public Traversal<Vertex, Map<Object, List>> get_g_V_group_by_outE_count_order_by_key() {
            return (IrCustomizedTraversal)
                    g.V().group().by(outE().count()).order().by(__.select(Column.keys));
        }

        @Override
        public Traversal<Vertex, Object> get_g_VX4X_bothE_as_otherV() {
            return g.V().has("id", 4).bothE().as("a").otherV().values("id");
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_hasLabel_hasId_values() {
            return g.V().hasLabel("person").has("id", 1).values("name");
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_out_as_a_in_select_a_as_b_select_b_by_values() {
            return g.V().out().as("a").in().select("a").as("b").select("b").values("name");
        }
    }
}

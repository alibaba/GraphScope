/*
 * Copyright 2022 Alibaba Group Holding Limited.
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

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;

import com.alibaba.graphscope.gremlin.plugin.traversal.IrCustomizedTraversal;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public abstract class IrGremlinQueryTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Map<Object, Long>>
            get_g_V_group_by_by_dedup_count_order_by_key();

    public abstract Traversal<Vertex, Map<Object, List>> get_g_V_group_by_outE_count_order_by_key();

    public abstract Traversal<Vertex, Object> get_g_VX4X_bothE_as_otherV();

    public abstract Traversal<Vertex, Object> get_g_V_hasLabel_hasId_values();

    public abstract Traversal<Vertex, Object>
            get_g_V_out_as_a_in_select_a_as_b_select_b_by_values();

    public abstract Traversal<Vertex, Map<String, Vertex>>
            get_g_V_matchXa_in_b__b_out_c__not_c_out_aX();

    public abstract Traversal<Vertex, Object>
            get_g_V_matchXa_knows_b__b_created_cX_select_c_values();

    public abstract Traversal<Vertex, Object>
            get_g_V_matchXa_out_b__b_in_cX_select_c_out_dedup_values();

    public abstract Traversal<Vertex, String> get_g_V_has_name_marko_label();

    public abstract Traversal<Vertex, Object> get_g_V_has_name_marko_select_by_T_label();

    public abstract Traversal<Vertex, Object> get_g_V_has_name_marko_select_by_label();

    public abstract Traversal<Edge, String> get_g_E_has_weight_0_5_f_label();

    public abstract Traversal<Vertex, Map<String, Object>> get_g_V_a_out_b_select_a_b_by_label_id();

    public abstract Traversal<Vertex, Map<Object, Object>> get_g_V_group_by_by_sum();

    public abstract Traversal<Vertex, Map<Object, Object>> get_g_V_group_by_by_max();

    public abstract Traversal<Vertex, Map<Object, Object>> get_g_V_group_by_by_min();

    public abstract Traversal<Vertex, Map<Object, Object>> get_g_V_group_by_by_mean();

    public abstract Traversal<Vertex, Vertex> get_g_V_sample_by_2();

    public abstract Traversal<Vertex, Vertex> get_g_V_sample_by_7();

    public abstract Traversal<Vertex, Vertex> get_g_V_coin_by_ratio();

    public abstract Traversal<Vertex, Vertex> get_g_V_coin_by_ratio_1();

    public abstract Traversal<Vertex, Object> get_g_V_identity_values();

    public abstract Traversal<Vertex, Object> get_g_V_haslabel_as_identity_values();

    public abstract Traversal<Vertex, Object> get_g_V_has_union_identity_out_values();

    public abstract Traversal<Vertex, Object> get_g_V_fold_unfold_values();

    public abstract Traversal<Vertex, Object> get_g_V_fold_a_unfold_values();

    public abstract Traversal<Vertex, Object> get_g_V_fold_a_unfold_select_a_unfold_values();

    public abstract Traversal<Vertex, Object> get_g_V_has_select_unfold_values();

    public abstract Traversal<Vertex, Object>
            get_g_V_has_fold_select_as_unfold_select_unfold_values();

    public abstract Traversal<Vertex, Object> get_g_V_has_fold_select_as_unfold_values();

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_V_group_by_by_dedup_count_test() {
        Traversal<Vertex, Map<Object, Long>> traversal =
                this.get_g_V_group_by_by_dedup_count_order_by_key();
        this.printTraversalForm(traversal);

        String expected = "{1=1, 2=1, 3=1, 4=1, 5=1, 6=1}";

        Map<Object, Long> result = traversal.next();
        Assert.assertEquals(expected, result.toString());
        Assert.assertFalse(traversal.hasNext());
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_V_group_by_outE_count_test() {
        Traversal<Vertex, Map<Object, List>> traversal =
                this.get_g_V_group_by_outE_count_order_by_key();
        this.printTraversalForm(traversal);
        Map<Object, List> result = traversal.next();
        Assert.assertEquals(4, result.size());
        result.forEach((k, v) -> Collections.sort(v));
        Assert.assertEquals("{0=[2, 3, 5], 1=[6], 2=[4], 3=[1]}", result.toString());
        Assert.assertFalse(traversal.hasNext());
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
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

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
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

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
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

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_V_matchXa_in_b__b_out_c__not_c_out_aX() {
        final Traversal<Vertex, Map<String, Vertex>> traversal =
                get_g_V_matchXa_in_b__b_out_c__not_c_out_aX();
        printTraversalForm(traversal);
        checkResults(
                makeMapList(
                        3,
                        "a",
                        convertToVertex(graph, "vadas"),
                        "b",
                        convertToVertex(graph, "marko"),
                        "c",
                        convertToVertex(graph, "vadas"),
                        "a",
                        convertToVertex(graph, "josh"),
                        "b",
                        convertToVertex(graph, "marko"),
                        "c",
                        convertToVertex(graph, "vadas"),
                        "a",
                        convertToVertex(graph, "josh"),
                        "b",
                        convertToVertex(graph, "marko"),
                        "c",
                        convertToVertex(graph, "josh"),
                        "a",
                        convertToVertex(graph, "vadas"),
                        "b",
                        convertToVertex(graph, "marko"),
                        "c",
                        convertToVertex(graph, "josh")),
                traversal);
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_V_matchXa_knows_b__b_created_cX_select_c_values() {
        final Traversal<Vertex, Object> traversal =
                get_g_V_matchXa_knows_b__b_created_cX_select_c_values();
        printTraversalForm(traversal);
        int counter = 0;

        List<String> expected = Arrays.asList("lop", "ripple");

        while (traversal.hasNext()) {
            Object result = traversal.next();
            Assert.assertTrue(expected.contains(result.toString()));
            ++counter;
        }

        Assert.assertEquals(2, counter);
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_V_matchXa_out_b__b_in_cX_select_c_out_dedup_values() {
        final Traversal<Vertex, Object> traversal =
                get_g_V_matchXa_out_b__b_in_cX_select_c_out_dedup_values();
        printTraversalForm(traversal);
        int counter = 0;

        List<String> expected = Arrays.asList("josh", "vadas");

        while (traversal.hasNext()) {
            Object result = traversal.next();
            Assert.assertTrue(expected.contains(result.toString()));
            ++counter;
        }

        Assert.assertEquals(2, counter);
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_V_has_name_marko_label() {
        final Traversal<Vertex, String> traversal = get_g_V_has_name_marko_label();
        printTraversalForm(traversal);
        Assert.assertEquals("person", traversal.next());
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_E_has_weight_0_5_f_label() {
        final Traversal<Edge, String> traversal = get_g_E_has_weight_0_5_f_label();
        printTraversalForm(traversal);
        Assert.assertEquals("knows", traversal.next());
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_V_has_name_marko_select_by_T_label() {
        final Traversal<Vertex, Object> traversal = get_g_V_has_name_marko_select_by_T_label();
        printTraversalForm(traversal);
        Assert.assertEquals("person", traversal.next());
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_V_has_name_marko_select_by_label() {
        final Traversal<Vertex, Object> traversal = get_g_V_has_name_marko_select_by_label();
        printTraversalForm(traversal);
        Assert.assertEquals("person", traversal.next());
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_V_a_out_b_select_a_b_by_label_id() {
        final Traversal<Vertex, Map<String, Object>> traversal =
                get_g_V_a_out_b_select_a_b_by_label_id();
        printTraversalForm(traversal);
        Assert.assertEquals("{a=person, b=lop}", traversal.next().toString());
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_V_group_by_by_sum() {
        final Traversal<Vertex, Map<Object, Object>> traversal = get_g_V_group_by_by_sum();
        printTraversalForm(traversal);
        Map<Object, Object> result = traversal.next();
        Assert.assertEquals(4, result.size());
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_V_group_by_by_max() {
        final Traversal<Vertex, Map<Object, Object>> traversal = get_g_V_group_by_by_max();
        printTraversalForm(traversal);
        Map<Object, Object> result = traversal.next();
        Assert.assertEquals(4, result.size());
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_V_group_by_by_min() {
        final Traversal<Vertex, Map<Object, Object>> traversal = get_g_V_group_by_by_min();
        printTraversalForm(traversal);
        Map<Object, Object> result = traversal.next();
        Assert.assertEquals(4, result.size());
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_V_group_by_by_mean() {
        final Traversal<Vertex, Map<Object, Object>> traversal = get_g_V_group_by_by_mean();
        printTraversalForm(traversal);
        Map<Object, Object> result = traversal.next();
        Assert.assertEquals(4, result.size());
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_V_sample_by_2() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_sample_by_2();
        this.printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            traversal.next();
            ++counter;
        }
        Assert.assertEquals(2, counter);
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_V_sample_by_7() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_sample_by_7();
        this.printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            traversal.next();
            ++counter;
        }
        Assert.assertEquals(6, counter);
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_V_coin_by_ratio() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_coin_by_ratio();
        this.printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            traversal.next();
            ++counter;
        }
        Assert.assertTrue(counter < 6);
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_V_coin_by_ratio_1() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_coin_by_ratio_1();
        this.printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            traversal.next();
            ++counter;
        }
        Assert.assertEquals(6, counter);
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_V_haslabel_identity() {
        Traversal<Vertex, Object> traversal = this.get_g_V_identity_values();
        this.printTraversalForm(traversal);
        int counter = 0;

        List<String> expected = Arrays.asList("1", "2", "3", "4", "5", "6");

        while (traversal.hasNext()) {
            Object result = traversal.next();
            Assert.assertTrue(expected.contains(result.toString()));
            ++counter;
        }
        Assert.assertEquals(6, counter);
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_V_haslabel_as_identity_values() {
        Traversal<Vertex, Object> traversal = this.get_g_V_haslabel_as_identity_values();
        this.printTraversalForm(traversal);
        int counter = 0;

        List<String> expected = Arrays.asList("1", "2", "4", "6");

        while (traversal.hasNext()) {
            Object result = traversal.next();
            Assert.assertTrue(expected.contains(result.toString()));
            ++counter;
        }
        Assert.assertEquals(4, counter);
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_V_haslabel_union_identity_out_values() {
        Traversal<Vertex, Object> traversal = this.get_g_V_has_union_identity_out_values();
        this.printTraversalForm(traversal);
        int counter = 0;

        List<String> expected = Arrays.asList("1", "2", "3", "4");

        while (traversal.hasNext()) {
            Object result = traversal.next();
            Assert.assertTrue(expected.contains(result.toString()));
            ++counter;
        }
        Assert.assertEquals(4, counter);
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_V_fold_unfold_values() {
        Traversal<Vertex, Object> traversal = this.get_g_V_fold_unfold_values();
        this.printTraversalForm(traversal);
        int counter = 0;

        List<String> expected = Arrays.asList("1", "2", "3", "4", "5", "6");

        while (traversal.hasNext()) {
            Object result = traversal.next();
            Assert.assertTrue(expected.contains(result.toString()));
            ++counter;
        }
        Assert.assertEquals(6, counter);
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_V_fold_a_unfold_values() {
        Traversal<Vertex, Object> traversal = this.get_g_V_fold_a_unfold_values();
        this.printTraversalForm(traversal);
        int counter = 0;

        List<String> expected = Arrays.asList("1", "2", "3", "4", "5", "6");

        while (traversal.hasNext()) {
            Object result = traversal.next();
            Assert.assertTrue(expected.contains(result.toString()));
            ++counter;
        }
        Assert.assertEquals(6, counter);
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_V_fold_a_unfold_select_a_unfold_values() {
        Traversal<Vertex, Object> traversal = this.get_g_V_fold_a_unfold_select_a_unfold_values();
        this.printTraversalForm(traversal);

        List<String> expected = Arrays.asList("1", "2", "3", "4", "5", "6");
        int counter[] = {6, 6, 6, 6, 6, 6};

        while (traversal.hasNext()) {
            Object result = traversal.next();
            Assert.assertTrue(expected.contains(result.toString()));
            int index = Integer.valueOf(result.toString());
            counter[index - 1] -= 1;
        }
        for (int i = 0; i < 6; ++i) {
            Assert.assertEquals(counter[i], 0);
        }
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_V_has_select_unfold_values() {
        Traversal<Vertex, Object> traversal = this.get_g_V_has_select_unfold_values();
        this.printTraversalForm(traversal);
        int counter = 0;

        List<String> expected = Arrays.asList("1");

        while (traversal.hasNext()) {
            Object result = traversal.next();
            Assert.assertTrue(expected.contains(result.toString()));
            ++counter;
        }
        Assert.assertEquals(1, counter);
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_V_has_fold_select_as_unfold_select_unfold_values() {
        Traversal<Vertex, Object> traversal =
                this.get_g_V_has_fold_select_as_unfold_select_unfold_values();
        this.printTraversalForm(traversal);

        List<String> expected = Arrays.asList("1");
        int counter = 0;

        while (traversal.hasNext()) {
            Object result = traversal.next();
            Assert.assertTrue(expected.contains(result.toString()));
            ++counter;
        }
        Assert.assertEquals(1, counter);
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_V_has_fold_select_as_unfold_values() {
        Traversal<Vertex, Object> traversal = this.get_g_V_has_fold_select_as_unfold_values();
        this.printTraversalForm(traversal);

        List<String> expected = Arrays.asList("1");
        int counter = 0;

        while (traversal.hasNext()) {
            Object result = traversal.next();
            Assert.assertTrue(expected.contains(result.toString()));
            ++counter;
        }
        Assert.assertEquals(1, counter);
    }

    public static class Traversals extends IrGremlinQueryTest {

        @Override
        public Traversal<Vertex, Map<Object, Long>> get_g_V_group_by_by_dedup_count_order_by_key() {
            return (IrCustomizedTraversal)
                    g.V().group()
                            .by("id")
                            .by(dedup().count())
                            .order()
                            .by(__.select(Column.keys), Order.asc);
        }

        @Override
        public Traversal<Vertex, Map<Object, List>> get_g_V_group_by_outE_count_order_by_key() {
            return (IrCustomizedTraversal)
                    g.V().group().by(outE().count()).by("id").order().by(__.select(Column.keys));
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

        @Override
        public Traversal<Vertex, Map<String, Vertex>>
                get_g_V_matchXa_in_b__b_out_c__not_c_out_aX() {
            return g.V().match(
                            as("a").in("knows").as("b"),
                            as("b").out("knows").as("c"),
                            not(as("c").out("knows").as("a")));
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_matchXa_knows_b__b_created_cX_select_c_values() {
            return g.V().match(as("a").out("knows").as("b"), as("b").out("created").as("c"))
                    .select("c")
                    .values("name");
        }

        @Override
        public Traversal<Vertex, Object>
                get_g_V_matchXa_out_b__b_in_cX_select_c_out_dedup_values() {
            return g.V().match(
                            as("a").out("created").has("name", "lop").as("b"),
                            as("b").in("created").has("age", 29).as("c"))
                    .select("c")
                    .out("knows")
                    .dedup()
                    .values("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_V_has_name_marko_label() {
            return g.V().has("name", "marko").label();
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_has_name_marko_select_by_T_label() {
            return g.V().has("name", "marko").as("a").select("a").by(T.label);
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_has_name_marko_select_by_label() {
            return g.V().has("name", "marko").as("a").select("a").by(__.label());
        }

        @Override
        public Traversal<Edge, String> get_g_E_has_weight_0_5_f_label() {
            return g.E().has("weight", 0.5f).label();
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_a_out_b_select_a_b_by_label_id() {
            return g.V().has("name", "marko")
                    .as("a")
                    .out()
                    .has("name", "lop")
                    .as("b")
                    .select("a", "b")
                    .by(label())
                    .by("name");
        }

        @Override
        public Traversal<Vertex, Map<Object, Object>> get_g_V_group_by_by_sum() {
            return g.V().group().by().by(values("age").sum());
        }

        @Override
        public Traversal<Vertex, Map<Object, Object>> get_g_V_group_by_by_max() {
            return g.V().group().by().by(values("age").max());
        }

        @Override
        public Traversal<Vertex, Map<Object, Object>> get_g_V_group_by_by_min() {
            return g.V().group().by().by(values("age").min());
        }

        @Override
        public Traversal<Vertex, Map<Object, Object>> get_g_V_group_by_by_mean() {
            return g.V().group().by().by(values("age").mean());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_sample_by_2() {
            return g.V().sample(2);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_sample_by_7() {
            return g.V().sample(7);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_coin_by_ratio() {
            return g.V().coin(0.1);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_coin_by_ratio_1() {
            return g.V().coin(1.0);
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_identity_values() {
            return g.V().identity().values("id");
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_haslabel_as_identity_values() {
            return g.V().hasLabel("person").as("a").identity().values("id");
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_has_union_identity_out_values() {
            return g.V().has("name", "marko").union(identity(), out()).values("id");
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_fold_unfold_values() {
            return g.V().fold().unfold().values("id");
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_fold_a_unfold_values() {
            return g.V().fold().as("a").unfold().values("id");
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_fold_a_unfold_select_a_unfold_values() {
            return g.V().fold().as("a").unfold().select("a").unfold().values("id");
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_has_select_unfold_values() {
            return g.V().has("name", "marko").fold().as("a").select("a").unfold().values("id");
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_has_fold_select_as_unfold_select_unfold_values() {
            return g.V().has("name", "marko")
                    .fold()
                    .as("a")
                    .select("a")
                    .as("b")
                    .unfold()
                    .select("b")
                    .unfold()
                    .values("id");
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_has_fold_select_as_unfold_values() {
            return g.V().has("name", "marko")
                    .fold()
                    .as("a")
                    .select("a")
                    .as("b")
                    .unfold()
                    .values("id");
        }
    }
}

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

package com.alibaba.graphscope.gremlin.antlr4;

import com.alibaba.graphscope.gremlin.plugin.script.AntlrToJavaScriptEngine;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Assert;
import org.junit.Before;

import org.junit.Test;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.SimpleBindings;
import javax.script.SimpleScriptContext;

public class PositiveEvalTest {
    private Graph graph;
    private GraphTraversalSource g;
    private AntlrToJavaScriptEngine scriptEngine;
    private ScriptContext context;

    @Before
    public void before() {
        graph = TinkerFactory.createModern();
        g = graph.traversal();
        Bindings globalBindings = new SimpleBindings();
        globalBindings.put("g", g);
        context = new SimpleScriptContext();
        context.setBindings(globalBindings, ScriptContext.ENGINE_SCOPE);
        scriptEngine = new AntlrToJavaScriptEngine();
    }

    private Object eval(String query) {
        return scriptEngine.eval(query, context);
    }

    @Test
    public void eval_g_V_test() {
        Assert.assertEquals(g.V(), eval("g.V()"));
    }

    @Test
    public void eval_g_E_test() {
        Assert.assertEquals(g.E(), eval("g.E()"));
    }

    // expand
    @Test
    public void eval_g_V_out_test() {
        Assert.assertEquals(g.V().out(), eval("g.V().out()"));
    }

    @Test
    public void eval_g_V_outE_test() {
        Assert.assertEquals(g.V().outE(), eval("g.V().outE()"));
    }

    @Test
    public void eval_g_V_in_test() {
        Assert.assertEquals(g.V().in(), eval("g.V().in()"));
    }

    @Test
    public void eval_g_V_inE_test() {
        Assert.assertEquals(g.V().inE(), eval("g.V().inE()"));
    }

    @Test
    public void eval_g_V_both_test() {
        Assert.assertEquals(g.V().both(), eval("g.V().both()"));
    }

    @Test
    public void eval_g_V_bothE_test() {
        Assert.assertEquals(g.V().bothE(), eval("g.V().bothE()"));
    }

    @Test
    public void eval_g_V_outE_label_test() {
        Assert.assertEquals(g.V().outE("knows"), eval("g.V().outE('knows')"));
    }

    @Test
    public void eval_g_V_out_labels_test() {
        Assert.assertEquals(g.V().out("knows", "has"), eval("g.V().out('knows', 'has')"));
    }

    @Test
    public void eval_g_V_outE_labels_test() {
        Assert.assertEquals(g.V().outE("knows", "has"), eval("g.V().outE('knows', 'has')"));
    }

    @Test
    public void eval_g_V_in_labels_test() {
        Assert.assertEquals(g.V().in("knows", "has"), eval("g.V().in('knows', 'has')"));
    }

    @Test
    public void eval_g_V_inE_labels_test() {
        Assert.assertEquals(g.V().inE("knows", "has"), eval("g.V().inE('knows', 'has')"));
    }

    @Test
    public void eval_g_V_both_labels_test() {
        Assert.assertEquals(g.V().both("knows", "has"), eval("g.V().both('knows', 'has')"));
    }

    @Test
    public void eval_g_V_bothE_labels_test() {
        Assert.assertEquals(g.V().bothE("knows", "has"), eval("g.V().bothE('knows', 'has')"));
    }

    @Test
    public void eval_g_V_outE_label_inV_test() {
        Assert.assertEquals(g.V().out("knows"), eval("g.V().outE('knows').inV()"));
    }

    @Test
    public void eval_g_V_inE_label_outV_test() {
        Assert.assertEquals(g.V().in("knows"), eval("g.V().inE('knows').outV()"));
    }

    @Test
    public void eval_g_V_bothE_label_otherV_test() {
        Assert.assertEquals(g.V().both("knows"), eval("g.V().bothE('knows').otherV()"));
    }

    // has

    @Test
    public void g_V_hasLabel_test() {
        Assert.assertEquals(g.V().hasLabel("person"), eval("g.V().hasLabel('person')"));
    }

    @Test
    public void g_V_hasLabels_test() {
        Assert.assertEquals(g.V().hasLabel("person", "software"), eval("g.V().hasLabel('person', 'software')"));
    }

    @Test
    public void g_V_hasId_test() {
        Assert.assertEquals(g.V().hasId(1), eval("g.V().hasId(1)"));
    }

    @Test
    public void g_V_hasIds_test() {
        Assert.assertEquals(g.V().hasId(1, 2), eval("g.V().hasId(1, 2)"));
    }

    @Test
    public void g_V_has_name_str_test() {
        Assert.assertEquals(g.V().has("name", "marko"), eval("g.V().has('name', 'marko')"));
    }

    @Test
    public void g_V_has_age_int_test() {
        Assert.assertEquals(g.V().has("age", 10), eval("g.V().has('age', 10)"));
    }

    // predicate

    @Test
    public void g_V_has_age_P_eq_test() {
        Assert.assertEquals(g.V().has("age", P.eq(10)), eval("g.V().has('age', eq(10))"));
    }

    @Test
    public void g_V_has_age_P_eq_1_test() {
        Assert.assertEquals(g.V().has("age", P.eq(10)), eval("g.V().has('age', P.eq(10))"));
    }

    @Test
    public void g_V_has_age_P_neq_test() {
        Assert.assertEquals(g.V().has("age", P.neq(10)), eval("g.V().has('age', neq(10))"));
    }

    @Test
    public void g_V_has_age_P_lt_test() {
        Assert.assertEquals(g.V().has("age", P.lt(10)), eval("g.V().has('age', lt(10))"));
    }

    @Test
    public void g_V_has_age_P_lte_test() {
        Assert.assertEquals(g.V().has("age", P.lte(10)), eval("g.V().has('age', lte(10))"));
    }

    @Test
    public void g_V_has_age_P_gt_test() {
        Assert.assertEquals(g.V().has("age", P.gt(10)), eval("g.V().has('age', gt(10))"));
    }

    @Test
    public void g_V_has_age_P_gte_test() {
        Assert.assertEquals(g.V().has("age", P.gte(10)), eval("g.V().has('age', gte(10))"));
    }

    // limit

    @Test
    public void g_V_limit_test() {
        Assert.assertEquals(g.V().limit(1), eval("g.V().limit(1)"));
    }

    @Test
    public void g_V_hasLabel_limit_test() {
        Assert.assertEquals(g.V().limit(1), eval("g.V().limit(1)"));
    }

    @Test
    public void g_V_limit_hasLabel_test() {
        Assert.assertEquals(g.V().hasLabel("person").limit(1), eval("g.V().hasLabel('person').limit(1)"));
    }

    @Test
    public void g_V_out_limit_test() {
        Assert.assertEquals(g.V().out().limit(1), eval("g.V().out().limit(1)"));
    }

    @Test
    public void g_V_hasLabel_out_limit_test() {
        Assert.assertEquals(g.V().hasLabel("person").out().limit(1), eval("g.V().hasLabel('person').out().limit(1)"));
    }

    @Test
    public void g_V_limit_out_out_limit_test() {
        Assert.assertEquals(g.V().limit(1).out().out().limit(1), eval("g.V().limit(1).out().out().limit(1)"));
    }

    @Test
    public void g_V_valueMap_str_test() {
        Assert.assertEquals(g.V().valueMap("name"), eval("g.V().valueMap('name')"));
    }

    @Test
    public void g_V_valueMap_strs_test() {
        Assert.assertEquals(g.V().valueMap("name", "id"), eval("g.V().valueMap('name', 'id')"));
    }

    @Test
    public void g_V_select_a_test() {
        Traversal expected = g.V().as("a").select("a");
        Assert.assertEquals(expected, eval("g.V().as(\"a\").select(\"a\")"));
    }

    @Test
    public void g_V_select_a_by_str_test() {
        Traversal expected = g.V().as("a").select("a").by("name");
        Assert.assertEquals(expected, eval("g.V().as(\"a\").select(\"a\").by(\"name\")"));
    }

    @Test
    public void g_V_select_a_by_valueMap_key_test() {
        Traversal expected = g.V().as("a").select("a").by(__.valueMap("name"));
        Assert.assertEquals(expected, eval("g.V().as(\"a\").select(\"a\").by(valueMap(\"name\"))"));
    }

    @Test
    public void g_V_select_a_by_valueMap_key_1_test() {
        Traversal expected = g.V().as("a").select("a").by(__.valueMap("name"));
        Assert.assertEquals(expected, eval("g.V().as(\"a\").select(\"a\").by(__.valueMap(\"name\"))"));
    }

    @Test
    public void g_V_select_a_by_valueMap_keys_test() {
        Traversal expected = g.V().as("a").select("a").by(__.valueMap("name", "id"));
        Assert.assertEquals(expected, eval("g.V().as(\"a\").select(\"a\").by(valueMap(\"name\", \"id\"))"));
    }

    @Test
    public void g_V_select_a_b_test() {
        Assert.assertEquals(g.V().as("a").out().as("b").select("a", "b"),
                eval("g.V().as(\"a\").out().as(\"b\").select(\"a\", \"b\")"));
    }

    @Test
    public void g_V_select_a_b_by_strs_test() {
        Assert.assertEquals(g.V().as("a").out().as("b").select("a", "b").by("name").by("id"),
                eval("g.V().as(\"a\").out().as(\"b\").select(\"a\", \"b\").by(\"name\").by(\"id\")"));
    }

    @Test
    public void g_V_select_a_b_c_test() {
        Assert.assertEquals(g.V().as("a").out().as("b").out().as("c").select("a", "b", "c"),
                eval("g.V().as(\"a\").out().as(\"b\").out().as(\"c\").select(\"a\", \"b\", \"c\")"));
    }

    @Test
    public void g_V_select_a_b_by_str_test() {
        Assert.assertEquals(g.V().as("a").out().as("b").select("a", "b").by("name"),
                eval("g.V().as(\"a\").out().as(\"b\").select(\"a\", \"b\").by(\"name\")"));
    }

    @Test
    public void g_V_select_a_b_by_valueMap_key_test() {
        Assert.assertEquals(g.V().as("a").out().as("b").select("a", "b").by(__.valueMap("name")),
                eval("g.V().as(\"a\").out().as(\"b\").select(\"a\", \"b\").by(valueMap(\"name\"))"));
    }

    @Test
    public void g_V_select_a_b_by_valueMap_keys_test() {
        Assert.assertEquals(g.V().as("a").out().as("b").select("a", "b").by(__.valueMap("name", "id")).by(__.valueMap("age")),
                eval("g.V().as(\"a\").out().as(\"b\").select(\"a\", \"b\").by(valueMap(\"name\", \"id\")).by(valueMap(\"age\"))"));
    }

    @Test
    public void g_V_order_test() {
        Assert.assertEquals(g.V().order(), eval("g.V().order()"));
    }

    @Test
    public void g_V_order_by_asc_test() {
        Assert.assertEquals(g.V().order().by(Order.asc), eval("g.V().order().by(asc)"));
    }

    @Test
    public void g_V_order_by_desc_test() {
        Assert.assertEquals(g.V().order().by(Order.desc), eval("g.V().order().by(desc)"));
    }

    @Test
    public void g_V_order_by_key_asc_test() {
        Assert.assertEquals(g.V().order().by("name", Order.asc), eval("g.V().order().by('name', asc)"));
    }

    @Test
    public void g_V_order_by_key_desc_test() {
        Assert.assertEquals(g.V().order().by("name", Order.desc), eval("g.V().order().by('name', desc)"));
    }

    @Test
    public void g_V_order_by_keys() {
        Assert.assertEquals(g.V().order().by("name", Order.desc).by("id", Order.asc),
                eval("g.V().order().by('name', desc).by('id', asc)"));
    }

    @Test
    public void g_V_has_property_test() {
        Assert.assertEquals(g.V().has("name", "marko"), eval("g.V().has('name', 'marko')"));
    }

    @Test
    public void g_V_has_eq_test() {
        Assert.assertEquals(g.V().has("name", P.eq("marko")), eval("g.V().has('name', eq('marko'))"));
    }

    @Test
    public void g_V_has_neq_test() {
        Assert.assertEquals(g.V().has("name", P.neq("marko")), eval("g.V().has('name', neq('marko'))"));
    }

    @Test
    public void g_V_has_lt_test() {
        Assert.assertEquals(g.V().has("age", P.lt(10)), eval("g.V().has('age', lt(10))"));
    }

    @Test
    public void g_V_has_lte_test() {
        Assert.assertEquals(g.V().has("age", P.lte(10)), eval("g.V().has('age', lte(10))"));
    }

    @Test
    public void g_V_has_gt_test() {
        Assert.assertEquals(g.V().has("age", P.gt(10)), eval("g.V().has('age', gt(10))"));
    }

    @Test
    public void g_V_has_gte_test() {
        Assert.assertEquals(g.V().has("age", P.gte(10)), eval("g.V().has('age', gte(10))"));
    }

    @Test
    public void g_V_has_within_int_test() {
        Assert.assertEquals(g.V().has("age", P.within(10)), eval("g.V().has('age', within(10))"));
    }

    @Test
    public void g_V_has_within_ints_test() {
        Assert.assertEquals(g.V().has("age", P.within(10, 11)), eval("g.V().has('age', within(10, 11))"));
    }

    @Test
    public void g_V_has_within_ints_1_test() {
        Assert.assertEquals(g.V().has("age", P.within(10, 11)), eval("g.V().has('age', within([10, 11]))"));
    }

    @Test
    public void g_V_has_without_int_test() {
        Assert.assertEquals(g.V().has("age", P.without(10)), eval("g.V().has('age', without(10))"));
    }

    @Test
    public void g_V_has_without_ints_test() {
        Assert.assertEquals(g.V().has("age", P.without(10, 11)), eval("g.V().has('age', without(10, 11))"));
    }

    @Test
    public void g_V_has_within_strs_test() {
        Assert.assertEquals(g.V().has("name", P.within("marko", "josh")), eval("g.V().has('name', within('marko', 'josh'))"));
    }

    @Test
    public void g_V_has_without_strs_test() {
        Assert.assertEquals(g.V().has("name", P.without("marko", "josh")), eval("g.V().has('name', without('marko', 'josh'))"));
    }

    @Test
    public void g_V_has_and_p_test() {
        Assert.assertEquals(g.V().has("age", P.gt(25).and(P.lt(32))), eval("g.V().has(\"age\", P.gt(25).and(P.lt(32)))"));
    }

    @Test
    public void g_V_has_or_p_test() {
        Assert.assertEquals(g.V().has("age", P.gt(25).or(P.lt(32))), eval("g.V().has(\"age\", P.gt(25).or(P.lt(32)))"));
    }

    @Test
    public void g_V_has_label_property_test() {
        Assert.assertEquals(g.V().has("person", "age", 25), eval("g.V().has(\"person\", \"age\", 25)"));
    }

    @Test
    public void g_V_has_label_property_eq_test() {
        Assert.assertEquals(g.V().has("person", "age", P.eq(25)), eval("g.V().has(\"person\", \"age\", P.eq(25))"));
    }

    @Test
    public void g_V_values_is_val_test() {
        Assert.assertEquals(g.V().values("name").is(27), eval("g.V().values(\"name\").is(27)"));
    }

    @Test
    public void g_V_values_is_p_test() {
        Assert.assertEquals(g.V().values("name").is(P.eq(27)), eval("g.V().values(\"name\").is(P.eq(27))"));
    }

    @Test
    public void g_V_as_test() {
        Assert.assertEquals(g.V().as("a"), eval("g.V().as('a')"));
    }

    @Test
    public void g_V_out_as_test() {
        Assert.assertEquals(g.V().out().as("abc"), eval("g.V().out().as('abc'))"));
    }

    @Test
    public void g_V_has_as_test() {
        Assert.assertEquals(g.V().has("name", P.without("marko", "josh")).as("c"),
                eval("g.V().has('name', without('marko', 'josh')).as('c')"));
    }

    @Test
    public void g_V_group_test() {
        Assert.assertEquals(g.V().group(), eval("g.V().group()"));
    }

    @Test
    public void g_V_group_by_key_str_test() {
        Assert.assertEquals(g.V().group().by("name"), eval("g.V().group().by(\"name\")"));
    }

    @Test
    public void g_V_group_by_key_values_test() {
        Assert.assertEquals(g.V().group().by(__.values("name")), eval("g.V().group().by(values(\"name\"))"));
    }

    @Test
    public void g_V_group_by_key_values_1_test() {
        Assert.assertEquals(g.V().group().by(__.values("name")), eval("g.V().group().by(__.values(\"name\"))"));
    }

    @Test
    public void g_V_group_by_key_values_as_test() {
        Assert.assertEquals(g.V().group().by(__.values("name").as("name")),
                eval("g.V().group().by(values(\"name\").as(\"name\"))"));
    }

    @Test
    public void g_V_group_by_key_str_by_value_count_test() {
        Assert.assertEquals(g.V().group().by("name").by(__.count()), eval("g.V().group().by(\"name\").by(count())"));
    }

    @Test
    public void g_V_group_by_key_values_by_value_fold_test() {
        Assert.assertEquals(g.V().group().by(__.values("name")).by(__.fold()),
                eval("g.V().group().by(values(\"name\")).by(fold())"));
    }

    @Test
    public void g_V_group_by_key_values_by_value_fold_1_test() {
        Assert.assertEquals(g.V().group().by(__.values("name")).by(__.fold()),
                eval("g.V().group().by(values(\"name\")).by(__.fold())"));
    }

    @Test
    public void g_V_group_by_key_str_by_value_count_as_test() {
        Assert.assertEquals(g.V().group().by("name").by(__.count().as("a")),
                eval("g.V().group().by(\"name\").by(count().as(\"a\"))"));
    }

    @Test
    public void g_V_group_by_key_values_by_value_fold_as_test() {
        Assert.assertEquals(g.V().group().by(__.values("name")).by(__.fold().as("a")),
                eval("g.V().group().by(values(\"name\")).by(fold().as(\"a\"))"));
    }

    @Test
    public void g_V_groupCount_test() {
        Assert.assertEquals(g.V().groupCount(), eval("g.V().groupCount()"));
    }

    @Test
    public void g_V_groupCount_by_str_test() {
        Assert.assertEquals(g.V().groupCount().by("name"), eval("g.V().groupCount().by(\"name\")"));
    }

    @Test
    public void g_V_groupCount_by_values_test() {
        Assert.assertEquals(g.V().groupCount().by(__.values("name")), eval("g.V().groupCount().by(values(\"name\"))"));
    }

    @Test
    public void g_V_groupCount_by_values_as_test() {
        Assert.assertEquals(g.V().groupCount().by(__.values("name").as("a")), eval("g.V().groupCount().by(values(\"name\")).as(\"a\")"));
    }

    @Test
    public void g_V_dedup_test() {
        Assert.assertEquals(g.V().dedup(), eval("g.V().dedup()"));
    }

    @Test
    public void g_V_count_test() {
        Assert.assertEquals(g.V().count(), eval("g.V().count()"));
    }

    @Test
    public void g_V_count_as_test() {
        Assert.assertEquals(g.V().count().as("a"), eval("g.V().count().as(\"a\")"));
    }

    @Test
    public void g_V_values_test() {
        Assert.assertEquals(g.V().values("name"), eval("g.V().values(\"name\")"));
    }

    @Test
    public void g_V_where_eq_a_test() {
        Assert.assertEquals(g.V().where(P.eq("a")), eval("g.V().where(P.eq(\"a\"))"));
    }

    @Test
    public void g_V_where_eq_a_age_test() {
        Assert.assertEquals(g.V().where(P.eq("a")).by("age"), eval("g.V().where(P.eq(\"a\")).by(\"age\")"));
    }

    @Test
    public void g_V_where_a_eq_b_test() {
        Assert.assertEquals(g.V().where("a", P.eq("b")), eval("g.V().where(\"a\", P.eq(\"b\"))"));
    }

    @Test
    public void g_V_where_a_eq_b_age_test() {
        Assert.assertEquals(g.V().where("a", P.eq("b")).by("age"), eval("g.V().where(\"a\", P.eq(\"b\")).by(\"age\")"));
    }

    @Test
    public void g_V_where_a_eq_b_value_age_test() {
        Assert.assertEquals(g.V().where("a", P.eq("b")).by("age").by(__.values("age")),
                eval("g.V().where(\"a\", P.eq(\"b\")).by(\"age\").by(values(\"age\"))"));
    }

    @Test
    public void g_V_where_a_eq_b_nested_value_age_test() {
        Assert.assertEquals(g.V().where("a", P.eq("b")).by("age").by(__.values("age")),
                eval("g.V().where(\"a\", P.eq(\"b\")).by(\"age\").by(__.values(\"age\"))"));
    }
}

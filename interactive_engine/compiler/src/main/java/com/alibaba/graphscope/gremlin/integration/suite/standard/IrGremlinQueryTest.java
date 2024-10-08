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

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;
import static org.junit.Assert.*;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

import com.alibaba.graphscope.gremlin.plugin.step.ExprStep;
import com.alibaba.graphscope.gremlin.plugin.traversal.IrCustomizedTraversal;
import com.google.common.collect.Lists;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
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

    public abstract Traversal<Vertex, Map<Object, Object>> get_g_V_outE_hasLabel_inV_elementMap();

    public abstract Traversal<Vertex, Map<Object, Object>> get_g_V_limit_elementMap();

    public abstract Traversal<Vertex, String> get_g_V_limit_label();

    public abstract Traversal<Vertex, Map<String, Object>> get_g_V_a_out_b_select_by_elementMap();

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

    public abstract Traversal<Vertex, Edge>
            get_g_VX1X_outE_asXhereX_inV_hasXname_vadasX_selectXhereX(final Object v1Id);

    public abstract Traversal<Vertex, Edge>
            get_g_VX1X_outEXknowsX_hasXweight_1X_asXhereX_inV_hasXname_joshX_selectXhereX(
                    final Object v1Id);

    public abstract Traversal<Vertex, Edge>
            get_g_VX1X_outEXknowsX_asXhereX_hasXweight_1X_inV_hasXname_joshX_selectXhereX(
                    final Object v1Id);

    public abstract Traversal<Edge, Edge> get_g_E_hasLabelXknowsX();

    // g.V().out().union(out(), in(), in()).count()
    public abstract Traversal<Vertex, Long> get_g_V_out_union_out_in_in_count();

    // g.V().union(out(), in().union(out(), out())).count()
    public abstract Traversal<Vertex, Long> get_g_V_union_out_inXunion_out_outX_count();

    // g.V().out().union(out(), in().union(out(), out())).count()
    public abstract Traversal<Vertex, Long> get_g_V_out_union_out_inXunion_out_outX_count();

    // g.V().out().union(out().union(out(), out()), in().union(out(), out())).count()
    public abstract Traversal<Vertex, Long>
            get_g_V_out_union_outXunion_out_outX_inXunion_out_outX_count();

    // g.V().union(in().union(out(), out()), in().union(out(), out())).count()
    public abstract Traversal<Vertex, Long>
            get_g_V_union_inXunion_out_outX_inXunion_out_outX_count();

    // g.V().out().union(in().union(out(), out()), in().union(out(), out())).count()
    public abstract Traversal<Vertex, Long>
            get_g_V_out_union_inXunion_out_outX_inXunion_out_outX_count();

    public abstract Traversal<Vertex, String> get_g_V_unionXout__inX_name();

    public abstract Traversal<Vertex, Number>
            get_g_VX1_2X_unionXoutE_count__inE_count__outE_weight_sumX(
                    final Object v1Id, final Object v2Id);

    // g.V().hasLabel('person').as('a').select(expr(POWER(a.age, 2)))
    public abstract Traversal<Vertex, Object> get_g_V_select_expr_power_age_by_2();

    // g.V().hasLabel('person').as('a').select(expr(sum(a.age) * 2 / 3))
    public abstract Traversal<Vertex, Object> get_g_V_select_expr_sum_age_mult_2_div_3();

    // g.V().select(expr(2 ^ 3 * 2))
    public abstract Traversal<Vertex, Object> get_g_V_select_expr_2_xor_3_mult_2_limit_1();

    // g.V().hasLabel('person').as('a').where(expr(a.name = 'marko' and (a.age > 20 OR a.age <
    // 10))).values('name')
    public abstract Traversal<Vertex, Object>
            get_g_V_where_expr_name_equal_marko_and_age_gt_20_or_age_lt_10_name();

    public abstract Traversal<Vertex, String> get_g_V_both_both_dedup_byXoutE_countX_name();

    public abstract Traversal<Vertex, String> get_g_V_whereXinXcreatedX_count_isX1XX_valuesXnameX();

    public abstract Traversal<Vertex, String>
            get_g_V_whereXinXcreatedX_count_isXgte_2XX_valuesXnameX();

    public abstract Traversal<Vertex, String>
            get_g_V_asXaX_outXcreatedX_whereXasXaX_name_isXjoshXX_inXcreatedX_name();

    public abstract Traversal<Vertex, String> get_g_V_whereXnotXoutXcreatedXXX_name();

    public abstract Traversal<Vertex, String>
            get_g_V_whereXinXknowsX_outXcreatedX_count_is_0XX_name();

    public abstract Traversal<Vertex, Vertex> get_g_V_order_byXoutE_count_descX();

    public abstract Traversal<Vertex, Vertex> get_g_V_asXaX_whereXoutXknowsXX_selectXaX();

    public abstract Traversal<Vertex, Map<Long, Long>> get_g_V_groupCount_byXbothE_countX();

    public abstract Traversal<Vertex, Map<Long, Collection<String>>>
            get_g_V_group_byXoutE_countX_byXnameX();

    public abstract Traversal<Vertex, Map<Integer, Collection<Vertex>>> get_g_V_group_byXageX();

    public abstract Traversal<Vertex, Object> get_g_V_path_expand_until_age_gt_30_values_age();

    // g.V().has("id",2).both("1..5").with("PATH_OPT","ARBITRARY").with("RESULT_OPT","END_V").count()
    public abstract Traversal<Vertex, Long> get_g_VX2X_both_with_arbitrary_endv_count();

    // g.V().has("id",2).both("1..5").with("PATH_OPT","SIMPLE").with("RESULT_OPT","END_V").count()
    public abstract Traversal<Vertex, Long> get_g_VX2X_both_with_simple_endv_count();

    // g.V().has("id",2).both("1..5").with("PATH_OPT","TRAIL").with("RESULT_OPT","END_V").count()
    public abstract Traversal<Vertex, Long> get_g_VX2X_both_with_trail_endv_count();

    // g.V().has("id",2).both("1..5").with("PATH_OPT","ARBITRARY").with("RESULT_OPT","ALL_V").count()
    public abstract Traversal<Vertex, Long> get_g_VX2X_both_with_arbitrary_allv_count();

    // g.V().has("id",2).both("1..5").with("PATH_OPT","SIMPLE").with("RESULT_OPT","ALL_V").count()
    public abstract Traversal<Vertex, Long> get_g_VX2X_both_with_simple_allv_count();

    // g.V().has("id",2).both("1..5").with("PATH_OPT","TRAIL").with("RESULT_OPT","ALL_V").count()
    public abstract Traversal<Vertex, Long> get_g_VX2X_both_with_trail_allv_count();

    // g.V().has("id",2).both("1..5").with("PATH_OPT","ARBITRARY").with("RESULT_OPT","ALL_V_E").count()
    public abstract Traversal<Vertex, Long> get_g_VX2X_both_with_arbitrary_allve_count();

    // g.V().has("id",2).both("1..5").with("PATH_OPT","SIMPLE").with("RESULT_OPT","ALL_V_E").count()
    public abstract Traversal<Vertex, Long> get_g_VX2X_both_with_simple_allve_count();

    // g.V().has("id",2).both("1..5").with("PATH_OPT","TRAIL").with("RESULT_OPT","ALL_V_E").count()
    public abstract Traversal<Vertex, Long> get_g_VX2X_both_with_trail_allve_count();

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_V_path_expand_until_age_gt_30_values_age() {
        assumeTrue("antlr_gremlin_calcite".equals(System.getenv("GREMLIN_SCRIPT_LANGUAGE_NAME")));
        final Traversal<Vertex, Object> traversal =
                get_g_V_path_expand_until_age_gt_30_values_age();
        printTraversalForm(traversal);
        Assert.assertEquals(32, traversal.next());
        Assert.assertFalse(traversal.hasNext());
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_V_select_expr_power_age_by_2() {
        // the expr test follows a sql-like expression syntax, which can only be opened when
        // language type is antlr_gremlin_calcite
        assumeTrue("antlr_gremlin_calcite".equals(System.getenv("GREMLIN_SCRIPT_LANGUAGE_NAME")));
        final Traversal<Vertex, Object> traversal = get_g_V_select_expr_power_age_by_2();
        printTraversalForm(traversal);
        checkResults(Arrays.asList(841, 729, 1024, 1225), traversal);
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_V_select_expr_sum_age_mult_2_div_3() {
        assumeTrue("antlr_gremlin_calcite".equals(System.getenv("GREMLIN_SCRIPT_LANGUAGE_NAME")));
        final Traversal<Vertex, Object> traversal = get_g_V_select_expr_sum_age_mult_2_div_3();
        printTraversalForm(traversal);
        Assert.assertEquals(82, traversal.next());
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_V_select_expr_2_xor_3_mult_2() {
        assumeTrue("antlr_gremlin_calcite".equals(System.getenv("GREMLIN_SCRIPT_LANGUAGE_NAME")));
        final Traversal<Vertex, Object> traversal = get_g_V_select_expr_2_xor_3_mult_2_limit_1();
        printTraversalForm(traversal);
        Assert.assertEquals(4, traversal.next());
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_V_where_expr_name_equal_marko_and_age_gt_20_or_age_lt_10_name() {
        assumeTrue("antlr_gremlin_calcite".equals(System.getenv("GREMLIN_SCRIPT_LANGUAGE_NAME")));
        final Traversal<Vertex, Object> traversal =
                get_g_V_where_expr_name_equal_marko_and_age_gt_20_or_age_lt_10_name();
        printTraversalForm(traversal);
        Assert.assertEquals("marko", traversal.next());
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void g_VX2X_both_with_arbitrary_endv_count() {
        assumeFalse("hiactor".equals(System.getenv("ENGINE_TYPE")));
        final Traversal<Vertex, Long> traversal = get_g_VX2X_both_with_arbitrary_endv_count();
        printTraversalForm(traversal);
        Assert.assertEquals(28, traversal.next().intValue());
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void g_VX2X_both_with_simple_endv_count() {
        assumeFalse("hiactor".equals(System.getenv("ENGINE_TYPE")));
        final Traversal<Vertex, Long> traversal = get_g_VX2X_both_with_simple_endv_count();
        printTraversalForm(traversal);
        Assert.assertEquals(9, traversal.next().intValue());
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void g_VX2X_both_with_trail_endv_count() {
        // Skip this test in distributed settings because edge ids might differ
        // across partitions in experimental store.
        assumeFalse("hiactor".equals(System.getenv("ENGINE_TYPE")));
        assumeFalse("true".equals(System.getenv("DISTRIBUTED_ENV")));
        final Traversal<Vertex, Long> traversal = get_g_VX2X_both_with_trail_endv_count();
        printTraversalForm(traversal);
        Assert.assertEquals(11, traversal.next().intValue());
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void g_VX2X_both_with_arbitrary_allv_count() {
        assumeFalse("hiactor".equals(System.getenv("ENGINE_TYPE")));
        final Traversal<Vertex, Long> traversal = get_g_VX2X_both_with_arbitrary_allv_count();
        printTraversalForm(traversal);
        Assert.assertEquals(28, traversal.next().intValue());
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void g_VX2X_both_with_simple_allv_count() {
        assumeFalse("hiactor".equals(System.getenv("ENGINE_TYPE")));
        final Traversal<Vertex, Long> traversal = get_g_VX2X_both_with_simple_allv_count();
        printTraversalForm(traversal);
        Assert.assertEquals(9, traversal.next().intValue());
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void g_VX2X_both_with_trail_allv_count() {
        // Skip this test in distributed settings because edge ids might differ
        // across partitions in experimental store.
        assumeFalse("hiactor".equals(System.getenv("ENGINE_TYPE")));
        assumeFalse("true".equals(System.getenv("DISTRIBUTED_ENV")));
        final Traversal<Vertex, Long> traversal = get_g_VX2X_both_with_trail_allv_count();
        printTraversalForm(traversal);
        Assert.assertEquals(11, traversal.next().intValue());
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void g_VX2X_both_with_arbitrary_allve_count() {
        assumeFalse("hiactor".equals(System.getenv("ENGINE_TYPE")));
        final Traversal<Vertex, Long> traversal = get_g_VX2X_both_with_arbitrary_allve_count();
        printTraversalForm(traversal);
        Assert.assertEquals(28, traversal.next().intValue());
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void g_VX2X_both_with_simple_allve_count() {
        assumeFalse("hiactor".equals(System.getenv("ENGINE_TYPE")));
        final Traversal<Vertex, Long> traversal = get_g_VX2X_both_with_simple_allve_count();
        printTraversalForm(traversal);
        Assert.assertEquals(9, traversal.next().intValue());
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void g_VX2X_both_with_trail_allve_count() {
        // Skip this test in distributed settings because edge ids might differ
        // across partitions in experimental store.
        assumeFalse("hiactor".equals(System.getenv("ENGINE_TYPE")));
        assumeFalse("true".equals(System.getenv("DISTRIBUTED_ENV")));
        final Traversal<Vertex, Long> traversal = get_g_VX2X_both_with_trail_allve_count();
        printTraversalForm(traversal);
        Assert.assertEquals(11, traversal.next().intValue());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_both_both_dedup_byXoutE_countX_name() {
        assumeFalse("hiactor".equals(System.getenv("ENGINE_TYPE")));
        final Traversal<Vertex, String> traversal = get_g_V_both_both_dedup_byXoutE_countX_name();
        printTraversalForm(traversal);
        final List<String> names = traversal.toList();
        assertEquals(4, names.size());
        assertTrue(names.contains("josh"));
        assertTrue(names.contains("peter"));
        assertTrue(names.contains("marko"));
        // the 4th is vadas, ripple, or lop
        assertEquals(4, new HashSet<>(names).size());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_whereXinXcreatedX_count_isX1XX_valuesXnameX() {
        assumeFalse("hiactor".equals(System.getenv("ENGINE_TYPE")));
        final Traversal<Vertex, String> traversal =
                get_g_V_whereXinXcreatedX_count_isX1XX_valuesXnameX();
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        assertEquals("ripple", traversal.next());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_whereXinXcreatedX_count_isXgte_2XX_valuesXnameX() {
        assumeFalse("hiactor".equals(System.getenv("ENGINE_TYPE")));
        final Traversal<Vertex, String> traversal =
                get_g_V_whereXinXcreatedX_count_isXgte_2XX_valuesXnameX();
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        assertEquals("lop", traversal.next());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXaX_outXcreatedX_whereXasXaX_name_isXjoshXX_inXcreatedX_name() {
        assumeFalse("hiactor".equals(System.getenv("ENGINE_TYPE")));
        final Traversal<Vertex, String> traversal =
                get_g_V_asXaX_outXcreatedX_whereXasXaX_name_isXjoshXX_inXcreatedX_name();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("marko", "josh", "peter", "josh"), traversal);
    }

    public abstract Traversal<Vertex, Long> get_g_V_where_out_out_count();

    public abstract Traversal<Vertex, Long> get_g_V_where_not_out_out_count();

    public abstract Traversal<Vertex, Long> get_g_V_where_a_neq_b_by_out_count_count();

    public abstract Traversal<Vertex, Map<String, Object>> get_g_V_select_a_b_by_out_count();

    public abstract Traversal<Vertex, Map<String, Object>>
            get_g_V_select_a_b_by_out_count_by_name();

    public abstract Traversal<Vertex, Long> get_g_V_dedup_a_b_by_out_count_count();

    public abstract Traversal<Vertex, Object> get_g_V_order_by_out_count_by_name();

    public abstract Traversal<Vertex, Map<Object, Long>> get_g_V_groupCount_by_out_count_by_age();

    public abstract Traversal<Vertex, Long> get_g_V_where_values_age_count();

    public abstract Traversal<Vertex, Long> get_g_V_where_not_values_age_count();

    public abstract Traversal<Vertex, Map<String, Object>>
            get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXa_outXknowsX_bX();

    public abstract Traversal<Vertex, Map<String, Object>>
            get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXb_hasXname_markoXX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXa_outXknowsX_bX() {
        assumeFalse("hiactor".equals(System.getenv("ENGINE_TYPE")));
        final Traversal<Vertex, Map<String, Object>> traversal =
                get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXa_outXknowsX_bX();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final Map<String, Object> map = traversal.next();
            assertEquals(2, map.size());
            assertTrue(map.containsKey("a"));
            assertTrue(map.containsKey("b"));
            assertEquals(convertToVertexId("marko"), ((Vertex) map.get("a")).id());
            assertEquals(convertToVertexId("josh"), ((Vertex) map.get("b")).id());
        }
        assertEquals(1, counter);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXb_hasXname_markoXX() {
        assumeFalse("hiactor".equals(System.getenv("ENGINE_TYPE")));
        final Traversal<Vertex, Map<String, Object>> traversal =
                get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXb_hasXname_markoXX();
        printTraversalForm(traversal);
        int counter = 0;
        int markoCounter = 0;
        while (traversal.hasNext()) {
            counter++;
            final Map<String, Object> map = traversal.next();
            assertEquals(2, map.size());
            assertTrue(map.containsKey("a"));
            assertTrue(map.containsKey("b"));
            assertEquals(convertToVertexId("marko"), ((Vertex) map.get("b")).id());
            if (((Vertex) map.get("a")).id().equals(convertToVertexId("marko"))) markoCounter++;
            else
                assertTrue(
                        ((Vertex) map.get("a")).id().equals(convertToVertexId("josh"))
                                || ((Vertex) map.get("a")).id().equals(convertToVertexId("peter")));
        }
        assertEquals(3, markoCounter);
        assertEquals(5, counter);
        assertFalse(traversal.hasNext());
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_V_where_out_out_count() {
        assumeFalse("hiactor".equals(System.getenv("ENGINE_TYPE")));
        Traversal<Vertex, Long> traversal = this.get_g_V_where_out_out_count();
        this.printTraversalForm(traversal);
        Assert.assertEquals(1, traversal.next().intValue());
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_V_where_not_out_out_count() {
        assumeFalse("hiactor".equals(System.getenv("ENGINE_TYPE")));
        Traversal<Vertex, Long> traversal = this.get_g_V_where_not_out_out_count();
        this.printTraversalForm(traversal);
        Assert.assertEquals(5, traversal.next().intValue());
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_V_where_a_neq_b_by_out_count_count() {
        assumeFalse("hiactor".equals(System.getenv("ENGINE_TYPE")));
        Traversal<Vertex, Long> traversal = this.get_g_V_where_a_neq_b_by_out_count_count();
        this.printTraversalForm(traversal);
        Assert.assertEquals(6, traversal.next().intValue());
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_V_select_a_b_by_out_count() {
        assumeFalse("hiactor".equals(System.getenv("ENGINE_TYPE")));
        Traversal<Vertex, Map<String, Object>> traversal = this.get_g_V_select_a_b_by_out_count();
        this.printTraversalForm(traversal);
        int a_2_b_0_count = 0;
        int a_1_b_0_count = 0;
        int a_3_b_0_count = 0;
        int a_3_b_2_count = 0;
        while (traversal.hasNext()) {
            Map<String, Object> result = traversal.next();
            if (((Long) result.get("a")) == 2 && ((Long) result.get("b")) == 0) {
                a_2_b_0_count++;
            } else if (((Long) result.get("a")) == 1 && ((Long) result.get("b")) == 0) {
                a_1_b_0_count++;
            } else if (((Long) result.get("a")) == 3 && ((Long) result.get("b")) == 0) {
                a_3_b_0_count++;
            } else if (((Long) result.get("a")) == 3 && ((Long) result.get("b")) == 2) {
                a_3_b_2_count++;
            }
        }
        Assert.assertEquals(2, a_2_b_0_count);
        Assert.assertEquals(1, a_1_b_0_count);
        Assert.assertEquals(2, a_3_b_0_count);
        Assert.assertEquals(1, a_3_b_2_count);
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_V_select_a_b_by_out_count_by_name() {
        assumeFalse("hiactor".equals(System.getenv("ENGINE_TYPE")));
        Traversal<Vertex, Map<String, Object>> traversal =
                this.get_g_V_select_a_b_by_out_count_by_name();
        this.printTraversalForm(traversal);
        int a_3_b_lop_count = 0;
        int a_2_b_lop_count = 0;
        int a_1_b_lop_count = 0;
        int a_2_b_ripple_count = 0;
        int a_3_b_vadas_count = 0;
        int a_3_b_josh_count = 0;
        while (traversal.hasNext()) {
            Map<String, Object> result = traversal.next();
            if (((Long) result.get("a")) == 3 && result.get("b").equals("lop")) {
                a_3_b_lop_count++;
            } else if (((Long) result.get("a")) == 2 && result.get("b").equals("lop")) {
                a_2_b_lop_count++;
            } else if (((Long) result.get("a")) == 1 && result.get("b").equals("lop")) {
                a_1_b_lop_count++;
            } else if (((Long) result.get("a")) == 2 && result.get("b").equals("ripple")) {
                a_2_b_ripple_count++;
            } else if (((Long) result.get("a")) == 3 && result.get("b").equals("vadas")) {
                a_3_b_vadas_count++;
            } else if (((Long) result.get("a")) == 3 && result.get("b").equals("josh")) {
                a_3_b_josh_count++;
            }
        }
        Assert.assertEquals(1, a_3_b_lop_count);
        Assert.assertEquals(1, a_2_b_lop_count);
        Assert.assertEquals(1, a_1_b_lop_count);
        Assert.assertEquals(1, a_2_b_ripple_count);
        Assert.assertEquals(1, a_3_b_vadas_count);
        Assert.assertEquals(1, a_3_b_josh_count);
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_V_dedup_a_b_by_out_count() {
        assumeFalse("hiactor".equals(System.getenv("ENGINE_TYPE")));
        Traversal<Vertex, Long> traversal = this.get_g_V_dedup_a_b_by_out_count_count();
        this.printTraversalForm(traversal);
        Assert.assertEquals(4, traversal.next().intValue());
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_V_order_by_out_count_by_name() {
        assumeFalse("hiactor".equals(System.getenv("ENGINE_TYPE")));
        Traversal<Vertex, Object> traversal = this.get_g_V_order_by_out_count_by_name();
        this.printTraversalForm(traversal);
        List names = Lists.newArrayList();
        while (traversal.hasNext()) {
            names.add(traversal.next());
        }
        Assert.assertEquals(
                Lists.newArrayList("vadas", "ripple", "lop", "peter", "josh", "marko"), names);
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_V_groupCount_by_out_count_by_age() {
        assumeFalse("hiactor".equals(System.getenv("ENGINE_TYPE")));
        Traversal<Vertex, Map<Object, Long>> traversal =
                this.get_g_V_groupCount_by_out_count_by_age();
        this.printTraversalForm(traversal);
        Map<Object, Long> result = traversal.next();
        Assert.assertEquals(1, result.get(2L).intValue());
        Assert.assertEquals(5, result.get(0L).intValue());
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_V_where_values_age_count() {
        Traversal<Vertex, Long> traversal = this.get_g_V_where_values_age_count();
        this.printTraversalForm(traversal);
        Assert.assertEquals(4, traversal.next().intValue());
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_V_where_not_values_age_count() {
        Traversal<Vertex, Long> traversal = this.get_g_V_where_not_values_age_count();
        this.printTraversalForm(traversal);
        Assert.assertEquals(2, traversal.next().intValue());
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_V_out_union_out_in_in_count() {
        assumeFalse("hiactor".equals(System.getenv("ENGINE_TYPE")));
        // union step is currently not supported in hqps
        Traversal<Vertex, Long> traversal = this.get_g_V_out_union_out_in_in_count();
        this.printTraversalForm(traversal);
        Assert.assertEquals(26, traversal.next().intValue());
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_V_union_out_inXunion_out_outX_count() {
        assumeFalse("hiactor".equals(System.getenv("ENGINE_TYPE")));
        // union step is currently not supported  in hqps
        Traversal<Vertex, Long> traversal = this.get_g_V_union_out_inXunion_out_outX_count();
        this.printTraversalForm(traversal);
        Assert.assertEquals(34, traversal.next().intValue());
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_V_out_union_out_inXunion_out_outX_count() {
        assumeFalse("hiactor".equals(System.getenv("ENGINE_TYPE")));
        // union step is currently not supported in hqps
        Traversal<Vertex, Long> traversal = this.get_g_V_out_union_out_inXunion_out_outX_count();
        this.printTraversalForm(traversal);
        Assert.assertEquals(54, traversal.next().intValue());
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_V_out_union_outXunion_out_outX_inXunion_out_outX_count() {
        assumeFalse("hiactor".equals(System.getenv("ENGINE_TYPE")));
        // union step is currently not supported  in hqps
        Traversal<Vertex, Long> traversal =
                this.get_g_V_out_union_outXunion_out_outX_inXunion_out_outX_count();
        this.printTraversalForm(traversal);
        Assert.assertEquals(52, traversal.next().intValue());
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_V_union_inXunion_out_outX_inXunion_out_outX_count() {
        assumeFalse("hiactor".equals(System.getenv("ENGINE_TYPE")));
        // union step is currently not supported in hqps
        Traversal<Vertex, Long> traversal =
                this.get_g_V_union_inXunion_out_outX_inXunion_out_outX_count();
        this.printTraversalForm(traversal);
        Assert.assertEquals(56, traversal.next().intValue());
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_V_out_union_inXunion_out_outX_inXunion_out_outX_count() {
        assumeFalse("hiactor".equals(System.getenv("ENGINE_TYPE")));
        // union step is currently not supported in hqps
        Traversal<Vertex, Long> traversal =
                this.get_g_V_out_union_inXunion_out_outX_inXunion_out_outX_count();
        this.printTraversalForm(traversal);
        Assert.assertEquals(104, traversal.next().intValue());
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_V_unionXout__inX_name() {
        assumeFalse("hiactor".equals(System.getenv("ENGINE_TYPE")));
        // union step is currently not supported in hqps
        final Traversal<Vertex, String> traversal = get_g_V_unionXout__inX_name();
        printTraversalForm(traversal);
        checkResults(
                new HashMap<String, Long>() {
                    {
                        put("marko", 3L);
                        put("lop", 3L);
                        put("peter", 1L);
                        put("ripple", 1L);
                        put("josh", 3L);
                        put("vadas", 1L);
                    }
                },
                traversal);
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_VX1_2X_unionXoutE_count__inE_count__outE_weight_sumX() {
        assumeFalse("hiactor".equals(System.getenv("ENGINE_TYPE")));
        // union step is currently not supported in hqps
        final Traversal<Vertex, Number> traversal =
                get_g_VX1_2X_unionXoutE_count__inE_count__outE_weight_sumX(
                        convertToVertexId("marko"), convertToVertexId("vadas"));
        printTraversalForm(traversal);
        checkResults(Arrays.asList(3L, 1.9d, 1L), traversal);
    }

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
        assumeFalse("hiactor".equals(System.getenv("ENGINE_TYPE")));
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
    public void g_V_outE_hasLabel_inV_elementMap() {
        final Traversal<Vertex, Map<Object, Object>> traversal =
                get_g_V_outE_hasLabel_inV_elementMap();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            Map values = traversal.next();
            Assert.assertTrue(values.containsKey(T.id));
            String name = (String) values.get("name");
            if (name.equals("vadas")) {
                Assert.assertEquals(27, values.get("age"));
                Assert.assertEquals("person", values.get(T.label));
            } else if (name.equals("josh")) {
                Assert.assertEquals(32, values.get("age"));
                Assert.assertEquals("person", values.get(T.label));
            } else {
                throw new IllegalStateException("It is not possible to reach here: " + values);
            }
            ++counter;
        }
        Assert.assertEquals(2, counter);
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_V_limit_elementMap() {
        final Traversal<Vertex, Map<Object, Object>> traversal = get_g_V_limit_elementMap();
        printTraversalForm(traversal);
        Map values = traversal.next();
        Assert.assertTrue(values.containsKey(T.id));
        String name = (String) values.get("name");
        if (name.equals("marko")) {
            Assert.assertEquals(29, values.get("age"));
            Assert.assertEquals("person", values.get(T.label));
        } else if (name.equals("josh")) {
            Assert.assertEquals(32, values.get("age"));
            Assert.assertEquals("person", values.get(T.label));
        } else if (name.equals("peter")) {
            Assert.assertEquals(35, values.get("age"));
            Assert.assertEquals("person", values.get(T.label));
        } else if (name.equals("vadas")) {
            Assert.assertEquals(27, values.get("age"));
            Assert.assertEquals("person", values.get(T.label));
        } else if (name.equals("lop")) {
            Assert.assertEquals("java", values.get("lang"));
            Assert.assertEquals("software", values.get(T.label));
        } else {
            if (!name.equals("ripple")) {
                throw new IllegalStateException("It is not possible to reach here: " + values);
            }

            Assert.assertEquals("java", values.get("lang"));
            Assert.assertEquals("software", values.get(T.label));
        }
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_V_limit_label() {
        final Traversal<Vertex, String> traversal = get_g_V_limit_label();
        printTraversalForm(traversal);
        String value = traversal.next();
        Assert.assertTrue(value.equals("person") || value.equals("software"));
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_V_a_out_b_select_by_elementMap() {
        final Traversal<Vertex, Map<String, Object>> traversal =
                get_g_V_a_out_b_select_by_elementMap();
        printTraversalForm(traversal);

        Map<String, Object> values = traversal.next();

        Map value1 = (Map) values.get("a");
        Assert.assertTrue(value1.containsKey(T.id));
        Assert.assertEquals("marko", value1.get("name"));
        Assert.assertEquals(29, value1.get("age"));
        Assert.assertEquals("person", value1.get(T.label));

        Map value2 = (Map) values.get("b");
        Assert.assertTrue(value2.containsKey(T.id));
        Assert.assertEquals("josh", value2.get("name"));
        Assert.assertEquals(32, value2.get("age"));
        Assert.assertEquals("person", value2.get(T.label));

        Assert.assertTrue(!traversal.hasNext());
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
        assumeFalse("hiactor".equals(System.getenv("ENGINE_TYPE")));
        // union step is currently not supported in hqps
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

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void g_VX1X_outE_asXhereX_inV_hasXname_vadasX_selectXhereX() {
        final Traversal<Vertex, Edge> traversal =
                get_g_VX1X_outE_asXhereX_inV_hasXname_vadasX_selectXhereX(
                        convertToVertexId("marko"));
        printTraversalForm(traversal);
        final Edge edge = traversal.next();
        Assert.assertEquals("knows", edge.label());
        Assert.assertEquals(convertToVertexId("vadas"), edge.inVertex().id());
        Assert.assertEquals(convertToVertexId("marko"), edge.outVertex().id());
        Assert.assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void g_VX1X_outEXknowsX_hasXweight_1X_asXhereX_inV_hasXname_joshX_selectXhereX() {
        final Traversal<Vertex, Edge> traversal =
                get_g_VX1X_outEXknowsX_hasXweight_1X_asXhereX_inV_hasXname_joshX_selectXhereX(
                        convertToVertexId("marko"));
        printTraversalForm(traversal);
        Assert.assertTrue(traversal.hasNext());
        final Edge edge = traversal.next();
        Assert.assertEquals("knows", edge.label());
        Assert.assertEquals(convertToVertexId("josh"), edge.inVertex().id());
        Assert.assertEquals(convertToVertexId("marko"), edge.outVertex().id());
        Assert.assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void g_VX1X_outEXknowsX_asXhereX_hasXweight_1X_inV_hasXname_joshX_selectXhereX() {
        final Traversal<Vertex, Edge> traversal =
                get_g_VX1X_outEXknowsX_asXhereX_hasXweight_1X_inV_hasXname_joshX_selectXhereX(
                        convertToVertexId("marko"));
        printTraversalForm(traversal);
        Assert.assertTrue(traversal.hasNext());
        final Edge edge = traversal.next();
        Assert.assertEquals("knows", edge.label());
        Assert.assertEquals(convertToVertexId("josh"), edge.inVertex().id());
        Assert.assertEquals(convertToVertexId("marko"), edge.outVertex().id());
        Assert.assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void g_E_hasLabelXknowsX() {
        // Hqps engine doesn't support scan from edge index scan.
        assumeFalse("hiactor".equals(System.getenv("ENGINE_TYPE")));
        final Traversal<Edge, Edge> traversal = get_g_E_hasLabelXknowsX();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            Assert.assertEquals("knows", traversal.next().label());
        }
        Assert.assertEquals(2, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_whereXnotXoutXcreatedXXX_name() {
        assumeFalse("hiactor".equals(System.getenv("ENGINE_TYPE")));
        final Traversal<Vertex, String> traversal = get_g_V_whereXnotXoutXcreatedXXX_name();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("vadas", "lop", "ripple"), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_whereXinXkknowsX_outXcreatedX_count_is_0XX_name() {
        assumeFalse("hiactor".equals(System.getenv("ENGINE_TYPE")));
        final Traversal<Vertex, String> traversal =
                get_g_V_whereXinXknowsX_outXcreatedX_count_is_0XX_name();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("marko", "lop", "ripple", "peter"), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_order_byXoutE_count_descX() {
        assumeFalse("hiactor".equals(System.getenv("ENGINE_TYPE")));
        Arrays.asList(get_g_V_order_byXoutE_count_descX())
                .forEach(
                        traversal -> {
                            printTraversalForm(traversal);
                            final List<Vertex> vertices = traversal.toList();
                            assertEquals(vertices.size(), 6);
                            assertEquals("marko", vertices.get(0).value("name"));
                            assertEquals("josh", vertices.get(1).value("name"));
                            assertEquals("peter", vertices.get(2).value("name"));
                            assertTrue(
                                    vertices.get(3).value("name").equals("vadas")
                                            || vertices.get(3).value("name").equals("ripple")
                                            || vertices.get(3).value("name").equals("lop"));
                            assertTrue(
                                    vertices.get(4).value("name").equals("vadas")
                                            || vertices.get(4).value("name").equals("ripple")
                                            || vertices.get(4).value("name").equals("lop"));
                            assertTrue(
                                    vertices.get(5).value("name").equals("vadas")
                                            || vertices.get(5).value("name").equals("ripple")
                                            || vertices.get(5).value("name").equals("lop"));
                        });
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXaX_whereXoutXknowsXX_selectXaX() {
        assumeFalse("hiactor".equals(System.getenv("ENGINE_TYPE")));
        final Traversal<Vertex, Vertex> traversal = get_g_V_asXaX_whereXoutXknowsXX_selectXaX();
        printTraversalForm(traversal);
        assertEquals(convertToVertex(graph, "marko"), traversal.next());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_groupCount_byXbothE_countX() {
        assumeFalse("hiactor".equals(System.getenv("ENGINE_TYPE")));
        final Traversal<Vertex, Map<Long, Long>> traversal = get_g_V_groupCount_byXbothE_countX();
        printTraversalForm(traversal);
        checkMap(
                new HashMap<Long, Long>() {
                    {
                        put(1L, 3L);
                        put(3L, 3L);
                    }
                },
                traversal.next());
        checkSideEffects(traversal.asAdmin().getSideEffects());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_group_byXoutE_countX_byXnameX() {
        assumeFalse("hiactor".equals(System.getenv("ENGINE_TYPE")));
        final Traversal<Vertex, Map<Long, Collection<String>>> traversal =
                get_g_V_group_byXoutE_countX_byXnameX();
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        final Map<Long, Collection<String>> map = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(4, map.size());
        assertTrue(map.containsKey(0L));
        assertTrue(map.containsKey(1L));
        assertTrue(map.containsKey(2L));
        assertTrue(map.containsKey(3L));
        assertEquals(3, map.get(0L).size());
        assertEquals(1, map.get(1L).size());
        assertEquals(1, map.get(2L).size());
        assertEquals(1, map.get(3L).size());
        assertTrue(map.get(0L).contains("lop"));
        assertTrue(map.get(0L).contains("ripple"));
        assertTrue(map.get(0L).contains("vadas"));
        assertTrue(map.get(1L).contains("peter"));
        assertTrue(map.get(2L).contains("josh"));
        assertTrue(map.get(3L).contains("marko"));
        checkSideEffects(traversal.asAdmin().getSideEffects());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_group_byXageX() {
        assumeFalse("hiactor".equals(System.getenv("ENGINE_TYPE")));
        final Traversal<Vertex, Map<Integer, Collection<Vertex>>> traversal =
                get_g_V_group_byXageX();
        printTraversalForm(traversal);

        final Map<Integer, Collection<Vertex>> map = traversal.next();
        assertEquals(5, map.size());
        map.forEach(
                (key, values) -> {
                    if (null == key) assertEquals(2, values.size());
                    else assertEquals(1, values.size());
                });
        assertFalse(traversal.hasNext());

        checkSideEffects(traversal.asAdmin().getSideEffects());
    }

    public static class Traversals extends IrGremlinQueryTest {

        // g.V().out().union(out(), in(), in()).count()
        @Override
        public Traversal<Vertex, Long> get_g_V_out_union_out_in_in_count() {
            return g.V().out().union(out(), in(), in()).count();
        }

        // g.V().union(out(), in().union(out(), out())).count()
        @Override
        public Traversal<Vertex, Long> get_g_V_union_out_inXunion_out_outX_count() {
            return g.V().union(out(), in().union(out(), out())).count();
        }

        // g.V().out().union(out(), in().union(out(), out())).count()
        @Override
        public Traversal<Vertex, Long> get_g_V_out_union_out_inXunion_out_outX_count() {
            return g.V().out().union(out(), in().union(out(), out())).count();
        }

        // g.V().out().union(out().union(out(), out()), in().union(out(), out())).count()
        @Override
        public Traversal<Vertex, Long>
                get_g_V_out_union_outXunion_out_outX_inXunion_out_outX_count() {
            return g.V().out().union(out().union(out(), out()), in().union(out(), out())).count();
        }

        // g.V().union(in().union(out(), out()), in().union(out(), out())).count()
        @Override
        public Traversal<Vertex, Long> get_g_V_union_inXunion_out_outX_inXunion_out_outX_count() {
            return g.V().union(in().union(out(), out()), in().union(out(), out())).count();
        }

        // g.V().out().union(in().union(out(), out()), in().union(out(), out())).count()
        @Override
        public Traversal<Vertex, Long>
                get_g_V_out_union_inXunion_out_outX_inXunion_out_outX_count() {
            return g.V().out().union(in().union(out(), out()), in().union(out(), out())).count();
        }

        @Override
        public Traversal<Vertex, String> get_g_V_unionXout__inX_name() {
            return g.V().union(out(), in()).values("name");
        }

        @Override
        public Traversal<Vertex, Number> get_g_VX1_2X_unionXoutE_count__inE_count__outE_weight_sumX(
                final Object v1Id, final Object v2Id) {
            return g.V(v1Id, v2Id)
                    .union(
                            outE().count(),
                            inE().count(),
                            (Traversal) outE().values("weight").sum());
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_select_expr_power_age_by_2() {
            return ((IrCustomizedTraversal) g.V().hasLabel("person"))
                    .as("a")
                    .select(
                            com.alibaba.graphscope.gremlin.integration.suite.utils.__.expr(
                                    "POWER(a.age, 2)", ExprStep.Type.PROJECTION));
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_select_expr_sum_age_mult_2_div_3() {
            return ((IrCustomizedTraversal) g.V().hasLabel("person"))
                    .as("a")
                    .select(
                            com.alibaba.graphscope.gremlin.integration.suite.utils.__.expr(
                                    "sum(a.age) * 2 / 3", ExprStep.Type.PROJECTION));
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_select_expr_2_xor_3_mult_2_limit_1() {
            return ((IrCustomizedTraversal) g.V())
                    .select(
                            com.alibaba.graphscope.gremlin.integration.suite.utils.__.expr(
                                    "2 ^ 3 * 2", ExprStep.Type.PROJECTION))
                    .limit(1);
        }

        @Override
        public Traversal<Vertex, Object>
                get_g_V_where_expr_name_equal_marko_and_age_gt_20_or_age_lt_10_name() {
            return ((IrCustomizedTraversal) g.V().hasLabel("person"))
                    .as("a")
                    .where(
                            com.alibaba.graphscope.gremlin.integration.suite.utils.__.expr(
                                    "a.name = 'marko' and (a.age > 20 OR a.age < 10)",
                                    ExprStep.Type.FILTER))
                    .values("name");
        }

        @Override
        public Traversal<Vertex, Long> get_g_VX2X_both_with_arbitrary_endv_count() {
            return ((IrCustomizedTraversal)
                    g.V().has("id", 2)
                            .both("1..5")
                            .with("PATH_OPT", "ARBITRARY")
                            .with("RESULT_OPT", "END_V")
                            .count());
        }

        @Override
        public Traversal<Vertex, Long> get_g_VX2X_both_with_simple_endv_count() {
            return ((IrCustomizedTraversal)
                    g.V().has("id", 2)
                            .both("1..5")
                            .with("PATH_OPT", "SIMPLE")
                            .with("RESULT_OPT", "END_V")
                            .count());
        }

        @Override
        public Traversal<Vertex, Long> get_g_VX2X_both_with_trail_endv_count() {
            return ((IrCustomizedTraversal)
                    g.V().has("id", 2)
                            .both("1..5")
                            .with("PATH_OPT", "TRAIL")
                            .with("RESULT_OPT", "END_V")
                            .count());
        }

        @Override
        public Traversal<Vertex, Long> get_g_VX2X_both_with_arbitrary_allv_count() {
            return ((IrCustomizedTraversal)
                    g.V().has("id", 2)
                            .both("1..5")
                            .with("PATH_OPT", "ARBITRARY")
                            .with("RESULT_OPT", "ALL_V")
                            .count());
        }

        @Override
        public Traversal<Vertex, Long> get_g_VX2X_both_with_simple_allv_count() {
            return ((IrCustomizedTraversal)
                    g.V().has("id", 2)
                            .both("1..5")
                            .with("PATH_OPT", "SIMPLE")
                            .with("RESULT_OPT", "ALL_V")
                            .count());
        }

        @Override
        public Traversal<Vertex, Long> get_g_VX2X_both_with_trail_allv_count() {
            return ((IrCustomizedTraversal)
                    g.V().has("id", 2)
                            .both("1..5")
                            .with("PATH_OPT", "TRAIL")
                            .with("RESULT_OPT", "ALL_V")
                            .count());
        }

        @Override
        public Traversal<Vertex, Long> get_g_VX2X_both_with_arbitrary_allve_count() {
            return ((IrCustomizedTraversal)
                    g.V().has("id", 2)
                            .both("1..5")
                            .with("PATH_OPT", "ARBITRARY")
                            .with("RESULT_OPT", "ALL_V_E")
                            .count());
        }

        @Override
        public Traversal<Vertex, Long> get_g_VX2X_both_with_simple_allve_count() {
            return ((IrCustomizedTraversal)
                    g.V().has("id", 2)
                            .both("1..5")
                            .with("PATH_OPT", "SIMPLE")
                            .with("RESULT_OPT", "ALL_V_E")
                            .count());
        }

        @Override
        public Traversal<Vertex, Long> get_g_VX2X_both_with_trail_allve_count() {
            return ((IrCustomizedTraversal)
                    g.V().has("id", 2)
                            .both("1..5")
                            .with("PATH_OPT", "TRAIL")
                            .with("RESULT_OPT", "ALL_V_E")
                            .count());
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_path_expand_until_age_gt_30_values_age() {
            return ((IrCustomizedTraversal)
                            g.V().out("1..100", "knows")
                                    .with(
                                            "UNTIL",
                                            com.alibaba.graphscope.gremlin.integration.suite.utils
                                                    .__.expr("_.age > 30", ExprStep.Type.FILTER)))
                    .endV()
                    .values("age");
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_where_out_out_count() {
            return g.V().where(__.out().out()).count();
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_where_not_out_out_count() {
            return g.V().where(__.not(__.out().out())).count();
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_where_a_neq_b_by_out_count_count() {
            return g.V().as("a").out().as("b").where("a", P.neq("b")).by(out().count()).count();
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_select_a_b_by_out_count() {
            return g.V().as("a").out().as("b").select("a", "b").by(__.out().count());
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_select_a_b_by_out_count_by_name() {
            return g.V().as("a").out().as("b").select("a", "b").by(__.out().count()).by("name");
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_dedup_a_b_by_out_count_count() {
            return g.V().as("a").out().as("b").dedup("a", "b").by(out().count()).count();
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_order_by_out_count_by_name() {
            return g.V().as("a")
                    .values("name")
                    .order()
                    .by(select("a").out().count(), Order.asc)
                    .by(select("a").values("name"), Order.desc);
        }

        @Override
        public Traversal<Vertex, Map<Object, Long>> get_g_V_groupCount_by_out_count_by_age() {
            return g.V().out().groupCount().by(__.out().count().as("key1"));
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_where_values_age_count() {
            return g.V().where(__.values("age")).count();
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_where_not_values_age_count() {
            return g.V().where(__.not(values("age"))).count();
        }

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
        public Traversal<Vertex, Map<Object, Object>> get_g_V_outE_hasLabel_inV_elementMap() {
            return g.V().outE().hasLabel("knows").inV().elementMap();
        }

        @Override
        public Traversal<Vertex, Map<Object, Object>> get_g_V_limit_elementMap() {
            return g.V().limit(1).elementMap();
        }

        @Override
        public Traversal<Vertex, String> get_g_V_limit_label() {
            return g.V().limit(1).label();
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_a_out_b_select_by_elementMap() {
            return g.V().has("name", "marko")
                    .as("a")
                    .out()
                    .has("name", "josh")
                    .as("b")
                    .select("a", "b")
                    .by(elementMap());
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

        @Override
        public Traversal<Vertex, Edge>
                get_g_VX1X_outEXknowsX_hasXweight_1X_asXhereX_inV_hasXname_joshX_selectXhereX(
                        final Object v1Id) {
            return g.V(v1Id)
                    .outE("knows")
                    .has("weight", 1.0d)
                    .as("here")
                    .inV()
                    .has("name", "josh")
                    .select("here");
        }

        @Override
        public Traversal<Vertex, Edge>
                get_g_VX1X_outEXknowsX_asXhereX_hasXweight_1X_inV_hasXname_joshX_selectXhereX(
                        final Object v1Id) {
            return g.V(v1Id)
                    .outE("knows")
                    .as("here")
                    .has("weight", 1.0d)
                    .inV()
                    .has("name", "josh")
                    .select("here");
        }

        @Override
        public Traversal<Vertex, Edge> get_g_VX1X_outE_asXhereX_inV_hasXname_vadasX_selectXhereX(
                final Object v1Id) {
            return g.V(v1Id).outE().as("here").inV().has("name", "vadas").select("here");
        }

        @Override
        public Traversal<Edge, Edge> get_g_E_hasLabelXknowsX() {
            return g.E().hasLabel("knows");
        }

        @Override
        public Traversal<Vertex, Map<String, Object>>
                get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXa_outXknowsX_bX() {
            return g.V().has("age")
                    .as("a")
                    .out()
                    .in()
                    .has("age")
                    .as("b")
                    .select("a", "b")
                    .where(as("a").out("knows").as("b"));
        }

        @Override
        public Traversal<Vertex, Map<String, Object>>
                get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXb_hasXname_markoXX() {
            return g.V().has("age")
                    .as("a")
                    .out()
                    .in()
                    .has("age")
                    .as("b")
                    .select("a", "b")
                    .where(as("b").has("name", "marko"));
        }

        @Override
        public Traversal<Vertex, String> get_g_V_both_both_dedup_byXoutE_countX_name() {
            return g.V().both().both().dedup().by(__.outE().count()).values("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_V_whereXinXcreatedX_count_isX1XX_valuesXnameX() {
            return g.V().where(in("created").count().is(1)).values("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_V_whereXinXcreatedX_count_isXgte_2XX_valuesXnameX() {
            return g.V().where(in("created").count().is(P.gte(2L))).values("name");
        }

        @Override
        public Traversal<Vertex, String>
                get_g_V_asXaX_outXcreatedX_whereXasXaX_name_isXjoshXX_inXcreatedX_name() {
            return g.V().as("a")
                    .out("created")
                    .where(as("a").values("name").is("josh"))
                    .in("created")
                    .values("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_V_whereXnotXoutXcreatedXXX_name() {
            return g.V().where(not(out("created"))).values("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_V_whereXinXknowsX_outXcreatedX_count_is_0XX_name() {
            return g.V().where(in("knows").out("created").count().is(0)).values("name");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_order_byXoutE_count_descX() {
            return g.V().order().by(outE().count(), Order.desc);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_asXaX_whereXoutXknowsXX_selectXaX() {
            return g.V().as("a").where(__.out("knows")).<Vertex>select("a");
        }

        @Override
        public Traversal<Vertex, Map<Long, Long>> get_g_V_groupCount_byXbothE_countX() {
            return g.V().<Long>groupCount().by(bothE().count());
        }

        @Override
        public Traversal<Vertex, Map<Long, Collection<String>>>
                get_g_V_group_byXoutE_countX_byXnameX() {
            return g.V().<Long, Collection<String>>group().by(outE().count()).by("name");
        }

        @Override
        public Traversal<Vertex, Map<Integer, Collection<Vertex>>> get_g_V_group_byXageX() {
            return g.V().<Integer, Collection<Vertex>>group().by("age");
        }
    }
}

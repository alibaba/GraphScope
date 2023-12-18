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

package com.alibaba.graphscope.gremlin.antlr4x;

import com.alibaba.graphscope.common.ir.Utils;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.gremlin.antlr4x.parser.GremlinAntlr4Parser;
import com.alibaba.graphscope.gremlin.antlr4x.visitor.GraphBuilderVisitor;

import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.calcite.rel.RelNode;
import org.junit.Assert;
import org.junit.Test;

public class GraphBuilderTest {
    public static RelNode eval(String query) {
        GraphBuilder builder = Utils.mockGraphBuilder();
        GraphBuilderVisitor visitor = new GraphBuilderVisitor(builder);
        ParseTree parseTree = new GremlinAntlr4Parser().parse(query);
        return visitor.visit(parseTree).build();
    }

    @Test
    public void g_V_test() {
        RelNode node = eval("g.V()");
        Assert.assertEquals(
                "GraphLogicalProject(DEFAULT=[DEFAULT], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[DEFAULT], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_id_test() {
        RelNode node = eval("g.V(1, 2, 3)");
        Assert.assertEquals(
                "GraphLogicalProject(DEFAULT=[DEFAULT], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[DEFAULT], opt=[VERTEX], uniqueKeyFilters=[SEARCH(DEFAULT.~id,"
                    + " Sarg[1, 2, 3])])",
                node.explain().trim());
    }

    @Test
    public void g_V_hasLabel_test() {
        RelNode node = eval("g.V().hasLabel('person')");
        Assert.assertEquals(
                "GraphLogicalProject(DEFAULT=[DEFAULT], isAppend=[false])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[DEFAULT], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_hasLabel_as_test() {
        RelNode node = eval("g.V().hasLabel('person').as('a')");
        Assert.assertEquals(
                "GraphLogicalProject(a=[a], isAppend=[false])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[a], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_hasLabels_as_test() {
        RelNode node = eval("g.V().hasLabel('person', 'software').as('a')");
        Assert.assertEquals(
                "GraphLogicalProject(a=[a], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[a], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_hasLabel_error_test() {
        try {
            RelNode node = eval("g.V().hasLabel('person').hasLabel('software')");
        } catch (IllegalArgumentException e) {
            Assert.assertEquals(
                    "cannot find common labels between values= [software] and label="
                            + " [[VertexLabel(person)]]",
                    e.getMessage());
            return;
        }
        Assert.fail();
    }

    @Test
    public void g_V_hasId_test() {
        RelNode node = eval("g.V().hasId(1)");
        Assert.assertEquals(
                "GraphLogicalProject(DEFAULT=[DEFAULT], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[DEFAULT], opt=[VERTEX], uniqueKeyFilters=[=(DEFAULT.~id, 1)])",
                node.explain().trim());
    }

    @Test
    public void g_V_hasIds_test() {
        RelNode node = eval("g.V().hasId(1, 2, 3)");
        Assert.assertEquals(
                "GraphLogicalProject(DEFAULT=[DEFAULT], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[DEFAULT], opt=[VERTEX], uniqueKeyFilters=[SEARCH(DEFAULT.~id,"
                    + " Sarg[1, 2, 3])])",
                node.explain().trim());
    }

    @Test
    public void g_V_has_name_marko_test() {
        RelNode node = eval("g.V().has('name', 'marko')");
        Assert.assertEquals(
                "GraphLogicalProject(DEFAULT=[DEFAULT], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[DEFAULT], fusedFilter=[[=(DEFAULT.name, _UTF-8'marko')]],"
                    + " opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_has_name_eq_marko_test() {
        RelNode node = eval("g.V().has('name', eq('marko'))");
        Assert.assertEquals(
                "GraphLogicalProject(DEFAULT=[DEFAULT], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[DEFAULT], fusedFilter=[[=(DEFAULT.name, _UTF-8'marko')]],"
                    + " opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_has_name_neq_marko_test() {
        RelNode node = eval("g.V().has('name', neq('marko'))");
        Assert.assertEquals(
                "GraphLogicalProject(DEFAULT=[DEFAULT], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[DEFAULT], fusedFilter=[[<>(DEFAULT.name, _UTF-8'marko')]],"
                    + " opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_has_age_gt_17_test() {
        RelNode node = eval("g.V().has('age', gt(17))");
        Assert.assertEquals(
                "GraphLogicalProject(DEFAULT=[DEFAULT], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[DEFAULT], fusedFilter=[[>(DEFAULT.age, 17)]], opt=[VERTEX])",
                node.explain().trim());
    }

    // hasNot('age') -> age is null
    @Test
    public void g_V_hasNot_age_test() {
        RelNode node = eval("g.V().hasNot('age')");
        Assert.assertEquals(
                "GraphLogicalProject(DEFAULT=[DEFAULT], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[DEFAULT], fusedFilter=[[IS NULL(DEFAULT.age)]], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_has_age_inside_17_20_test() {
        RelNode node = eval("g.V().has('age', inside(17, 20))");
        Assert.assertEquals(
                "GraphLogicalProject(DEFAULT=[DEFAULT], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[DEFAULT], fusedFilter=[[SEARCH(DEFAULT.age, Sarg[[18..19]])]],"
                    + " opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_has_age_outside_17_20_test() {
        RelNode node = eval("g.V().has('age', outside(17, 20))");
        Assert.assertEquals(
                "GraphLogicalProject(DEFAULT=[DEFAULT], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[DEFAULT], fusedFilter=[[SEARCH(DEFAULT.age, Sarg[(-∞..18),"
                    + " (19..+∞)])]], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_has_age_within_test() {
        RelNode node = eval("g.V().has('age', within(17, 20))");
        Assert.assertEquals(
                "GraphLogicalProject(DEFAULT=[DEFAULT], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[DEFAULT], fusedFilter=[[SEARCH(DEFAULT.age, Sarg[17, 20])]],"
                    + " opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_has_age_without_test() {
        RelNode node = eval("g.V().has('age', without(17, 20))");
        Assert.assertEquals(
                "GraphLogicalProject(DEFAULT=[DEFAULT], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[DEFAULT], fusedFilter=[[SEARCH(DEFAULT.age, Sarg[(-∞..17), (17..20),"
                    + " (20..+∞)])]], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_has_name_endingWith_test() {
        RelNode node = eval("g.V().has('name', endingWith('mar'))");
        Assert.assertEquals(
                "GraphLogicalProject(DEFAULT=[DEFAULT], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[DEFAULT], fusedFilter=[[POSIX REGEX CASE SENSITIVE(DEFAULT.name,"
                    + " _UTF-8'mar$')]], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_has_name_not_endingWith_test() {
        RelNode node = eval("g.V().has('name', notEndingWith('mar'))");
        Assert.assertEquals(
                "GraphLogicalProject(DEFAULT=[DEFAULT], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[DEFAULT], fusedFilter=[[NOT(POSIX REGEX CASE SENSITIVE(DEFAULT.name,"
                    + " _UTF-8'mar$'))]], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_has_name_containing_test() {
        RelNode node = eval("g.V().has('name', containing('mar'))");
        Assert.assertEquals(
                "GraphLogicalProject(DEFAULT=[DEFAULT], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[DEFAULT], fusedFilter=[[POSIX REGEX CASE SENSITIVE(DEFAULT.name,"
                    + " _UTF-8'.*mar.*')]], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_has_name_not_containing_test() {
        RelNode node = eval("g.V().has('name', notContaining('mar'))");
        Assert.assertEquals(
                "GraphLogicalProject(DEFAULT=[DEFAULT], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[DEFAULT], fusedFilter=[[NOT(POSIX REGEX CASE SENSITIVE(DEFAULT.name,"
                    + " _UTF-8'.*mar.*'))]], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_has_name_startingWith_test() {
        RelNode node = eval("g.V().has('name', startingWith('mar'))");
        Assert.assertEquals(
                "GraphLogicalProject(DEFAULT=[DEFAULT], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[DEFAULT], fusedFilter=[[POSIX REGEX CASE SENSITIVE(DEFAULT.name,"
                    + " _UTF-8'^mar')]], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_has_name_not_startingWith_test() {
        RelNode node = eval("g.V().has('name', notStartingWith('mar'))");
        Assert.assertEquals(
                "GraphLogicalProject(DEFAULT=[DEFAULT], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[DEFAULT], fusedFilter=[[NOT(POSIX REGEX CASE SENSITIVE(DEFAULT.name,"
                    + " _UTF-8'^mar'))]], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_has_name_marko_age_17_has_label_test() {
        RelNode node = eval("g.V().has('name', 'marko').has('age', 17).hasLabel('person')");
        Assert.assertEquals(
                "GraphLogicalProject(DEFAULT=[DEFAULT], isAppend=[false])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[DEFAULT], fusedFilter=[[AND(=(DEFAULT.name, _UTF-8'marko'),"
                        + " =(DEFAULT.age, 17))]], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_has_name_marko_age_17_has_label_error_test() {
        try {
            RelNode node = eval("g.V().has('name', 'marko').has('age', 17).hasLabel('software')");
        } catch (IllegalArgumentException e) {
            Assert.assertEquals(
                    "{property=age} not found; expected properties are: [id, name, lang,"
                            + " creationDate]",
                    e.getMessage());
            return;
        }
        Assert.fail();
    }

    @Test
    public void g_V_out_test() {
        RelNode node = eval("g.V().out()");
        Assert.assertEquals(
                "GraphLogicalProject(DEFAULT=[DEFAULT], isAppend=[false])\n"
                    + "  GraphLogicalGetV(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[DEFAULT], opt=[END])\n"
                    + "    GraphLogicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[DEFAULT], opt=[OUT])\n"
                    + "      GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[DEFAULT], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_out_label_test() {
        RelNode node = eval("g.V().out('knows')");
        Assert.assertEquals(
                "GraphLogicalProject(DEFAULT=[DEFAULT], isAppend=[false])\n"
                    + "  GraphLogicalGetV(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[DEFAULT], opt=[END])\n"
                    + "    GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}],"
                    + " alias=[DEFAULT], opt=[OUT])\n"
                    + "      GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[DEFAULT], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_out_label_as_test() {
        RelNode node = eval("g.V().as('a').out().as('b')");
        Assert.assertEquals(
                "GraphLogicalProject(b=[b], isAppend=[false])\n"
                    + "  GraphLogicalGetV(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[b], opt=[END])\n"
                    + "    GraphLogicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[DEFAULT], opt=[OUT])\n"
                    + "      GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[a], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_path_expand_test() {
        RelNode node =
                eval(
                        "g.V().as('a').out('1..2', 'knows').with('path_opt',"
                                + " 'simple').with('result_opt', 'all_v').as('b').endV().as('c')");
        Assert.assertEquals(
                "GraphLogicalProject(c=[c], isAppend=[false])\n"
                    + "  GraphLogicalGetV(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[c], opt=[END])\n"
                    + "    GraphLogicalPathExpand(expand=[GraphLogicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[knows]}], alias=[DEFAULT], opt=[OUT])\n"
                    + "], getV=[GraphLogicalGetV(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[DEFAULT], opt=[END])\n"
                    + "], offset=[1], fetch=[1], path_opt=[SIMPLE], result_opt=[ALL_V],"
                    + " alias=[b])\n"
                    + "      GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[a], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_values_name_test() {
        RelNode node = eval("g.V().values('name').as('a')");
        Assert.assertEquals(
                "GraphLogicalProject(a=[a], isAppend=[false])\n"
                        + "  GraphLogicalProject(a=[DEFAULT.name], isAppend=[true])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                        + " person]}], alias=[DEFAULT], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_valueMap_name_test() {
        RelNode node = eval("g.V().valueMap('name').as('a')");
        Assert.assertEquals(
                "GraphLogicalProject(a=[a], isAppend=[false])\n"
                        + "  GraphLogicalProject(a=[MAP(_UTF-8'name', DEFAULT.name)],"
                        + " isAppend=[true])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                        + " person]}], alias=[DEFAULT], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_valueMap_all_test() {
        RelNode node = eval("g.V().valueMap().as('a')");
        Assert.assertEquals(
                "GraphLogicalProject(a=[a], isAppend=[false])\n"
                        + "  GraphLogicalProject(a=[MAP(_UTF-8'id', DEFAULT.id, _UTF-8'name',"
                        + " DEFAULT.name, _UTF-8'lang', DEFAULT.lang, _UTF-8'creationDate',"
                        + " DEFAULT.creationDate, _UTF-8'age', DEFAULT.age)], isAppend=[true])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                        + " person]}], alias=[DEFAULT], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_elementMap_all_test() {
        RelNode node = eval("g.V().elementMap().as('a')");
        Assert.assertEquals(
                "GraphLogicalProject(a=[a], isAppend=[false])\n"
                    + "  GraphLogicalProject(a=[MAP(_UTF-8'~label', DEFAULT.~label, _UTF-8'~id',"
                    + " DEFAULT.~id, _UTF-8'id', DEFAULT.id, _UTF-8'name', DEFAULT.name,"
                    + " _UTF-8'lang', DEFAULT.lang, _UTF-8'creationDate', DEFAULT.creationDate,"
                    + " _UTF-8'age', DEFAULT.age)], isAppend=[true])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[DEFAULT], opt=[VERTEX])",
                node.explain().trim());
    }

    // todo: optimize select('a'), provide a tag representing 'NONE'
    @Test
    public void g_V_select_a_test() {
        RelNode node = eval("g.V().as('a').select('a')");
        Assert.assertEquals(
                "GraphLogicalProject(a=[a], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[a], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_select_a_by_name_test() {
        RelNode node = eval("g.V().as('a').select('a').by('name')");
        Assert.assertEquals(
                "GraphLogicalProject(name=[name], isAppend=[false])\n"
                        + "  GraphLogicalProject(name=[a.name], isAppend=[true])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                        + " person]}], alias=[a], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_select_a_by_valueMap_test() {
        RelNode node = eval("g.V().as('a').select('a').by(valueMap())");
        Assert.assertEquals(
                "GraphLogicalProject($f0=[$f0], isAppend=[false])\n"
                    + "  GraphLogicalProject($f0=[MAP(_UTF-8'id', a.id, _UTF-8'name', a.name,"
                    + " _UTF-8'lang', a.lang, _UTF-8'creationDate', a.creationDate, _UTF-8'age',"
                    + " a.age)], isAppend=[true])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[a], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_select_a_b_test() {
        RelNode node = eval("g.V().as('a').out().as('b').select('a', 'b')");
        Assert.assertEquals(
                "GraphLogicalProject($f0=[$f0], isAppend=[false])\n"
                    + "  GraphLogicalProject($f0=[MAP(_UTF-8'a', a, _UTF-8'b', b)],"
                    + " isAppend=[true])\n"
                    + "    GraphLogicalGetV(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[b], opt=[END])\n"
                    + "      GraphLogicalExpand(tableConfig=[{isAll=true, tables=[created,"
                    + " knows]}], alias=[DEFAULT], opt=[OUT])\n"
                    + "        GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[a], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_select_a_b_by_name_valueMap_test() {
        RelNode node =
                eval("g.V().as('a').out().as('b').select('a', 'b').by('name').by(valueMap())");
        Assert.assertEquals(
                "GraphLogicalProject($f0=[$f0], isAppend=[false])\n"
                    + "  GraphLogicalProject($f0=[MAP(_UTF-8'a', a.name, _UTF-8'b', MAP(_UTF-8'id',"
                    + " b.id, _UTF-8'name', b.name, _UTF-8'lang', b.lang, _UTF-8'creationDate',"
                    + " b.creationDate, _UTF-8'age', b.age))], isAppend=[true])\n"
                    + "    GraphLogicalGetV(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[b], opt=[END])\n"
                    + "      GraphLogicalExpand(tableConfig=[{isAll=true, tables=[created,"
                    + " knows]}], alias=[DEFAULT], opt=[OUT])\n"
                    + "        GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[a], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_order_test() {
        RelNode node = eval("g.V().order()");
        Assert.assertEquals(
                "GraphLogicalProject(DEFAULT=[DEFAULT], isAppend=[false])\n"
                        + "  GraphLogicalSort(sort0=[DEFAULT], dir0=[ASC])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                        + " person]}], alias=[DEFAULT], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_order_by_name_age_test() {
        RelNode node = eval("g.V().order().by('name', desc).by('age', asc)");
        Assert.assertEquals(
                "GraphLogicalProject(DEFAULT=[DEFAULT], isAppend=[false])\n"
                    + "  GraphLogicalSort(sort0=[DEFAULT.name], sort1=[DEFAULT.age], dir0=[DESC],"
                    + " dir1=[ASC])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[DEFAULT], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_order_by_select_test() {
        RelNode node = eval("g.V().as('a').order().by(select('a').by('name'))");
        Assert.assertEquals(
                "GraphLogicalProject(a=[a], isAppend=[false])\n"
                        + "  GraphLogicalSort(sort0=[a.name], dir0=[ASC])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                        + " person]}], alias=[a], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_limit_test() {
        RelNode node = eval("g.V().limit(10)");
        Assert.assertEquals(
                "GraphLogicalProject(DEFAULT=[DEFAULT], isAppend=[false])\n"
                        + "  GraphLogicalSort(fetch=[10])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                        + " person]}], alias=[DEFAULT], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_order_limit_test() {
        RelNode node = eval("g.V().order().by('name', desc).by('age', asc).limit(10)");
        Assert.assertEquals(
                "GraphLogicalProject(DEFAULT=[DEFAULT], isAppend=[false])\n"
                    + "  GraphLogicalSort(sort0=[DEFAULT.name], sort1=[DEFAULT.age], dir0=[DESC],"
                    + " dir1=[ASC], fetch=[10])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[DEFAULT], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_group_test() {
        RelNode node = eval("g.V().group()");
        Assert.assertEquals(
                "GraphLogicalAggregate(keys=[{variables=[DEFAULT], aliases=[keys]}],"
                    + " values=[[{operands=[DEFAULT], aggFunction=COLLECT, alias='values',"
                    + " distinct=false}]])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[DEFAULT], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_group_by_label_by_name_test() {
        RelNode node = eval("g.V().group().by('~label').by('name')");
        Assert.assertEquals(
                "GraphLogicalAggregate(keys=[{variables=[DEFAULT.~label], aliases=[keys]}],"
                    + " values=[[{operands=[DEFAULT.name], aggFunction=COLLECT, alias='values',"
                    + " distinct=false}]])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[DEFAULT], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_group_by_select_a_name_by_count_test() {
        RelNode node =
                eval(
                        "g.V().as('a').group().by(select('a').by('name').as('b')).by(select('a').count().as('c'))");
        Assert.assertEquals(
                "GraphLogicalAggregate(keys=[{variables=[a.name], aliases=[b]}],"
                    + " values=[[{operands=[a], aggFunction=COUNT, alias='c', distinct=false}]])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[a], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_groupCount_by_name_test() {
        RelNode node = eval("g.V().groupCount().by('name')");
        Assert.assertEquals(
                "GraphLogicalAggregate(keys=[{variables=[DEFAULT.name], aliases=[keys]}],"
                    + " values=[[{operands=[DEFAULT], aggFunction=COUNT, alias='values',"
                    + " distinct=false}]])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[DEFAULT], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_dedup_test() {
        RelNode node = eval("g.V().dedup()");
        Assert.assertEquals(
                "GraphLogicalProject(DEFAULT=[DEFAULT], isAppend=[false])\n"
                        + "  GraphLogicalDedupBy(dedupByKeys=[[DEFAULT]])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                        + " person]}], alias=[DEFAULT], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_dedup_by_name_test() {
        RelNode node = eval("g.V().dedup().by('name')");
        Assert.assertEquals(
                "GraphLogicalProject(DEFAULT=[DEFAULT], isAppend=[false])\n"
                        + "  GraphLogicalDedupBy(dedupByKeys=[[DEFAULT.name]])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                        + " person]}], alias=[DEFAULT], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_dedup_by_a_name_test() {
        RelNode node = eval("g.V().as('a').dedup('a').by('name')");
        Assert.assertEquals(
                "GraphLogicalProject(a=[a], isAppend=[false])\n"
                        + "  GraphLogicalDedupBy(dedupByKeys=[[a.name]])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                        + " person]}], alias=[a], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_dedup_by_a_b_name_test() {
        RelNode node = eval("g.V().as('a').out().as('b').dedup('a', 'b').by('name')");
        Assert.assertEquals(
                "GraphLogicalProject(b=[b], isAppend=[false])\n"
                    + "  GraphLogicalDedupBy(dedupByKeys=[[a.name, b.name]])\n"
                    + "    GraphLogicalGetV(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[b], opt=[END])\n"
                    + "      GraphLogicalExpand(tableConfig=[{isAll=true, tables=[created,"
                    + " knows]}], alias=[DEFAULT], opt=[OUT])\n"
                    + "        GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[a], opt=[VERTEX])",
                node.explain().trim());
    }
}

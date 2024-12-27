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

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.exception.FrontendException;
import com.alibaba.graphscope.common.ir.Utils;
import com.alibaba.graphscope.common.ir.meta.IrMeta;
import com.alibaba.graphscope.common.ir.planner.GraphIOProcessor;
import com.alibaba.graphscope.common.ir.planner.GraphRelOptimizer;
import com.alibaba.graphscope.common.ir.planner.rules.ExpandGetVFusionRule;
import com.alibaba.graphscope.common.ir.runtime.proto.RexToProtoConverter;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphStdOperatorTable;
import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;
import com.alibaba.graphscope.common.ir.tools.config.SourceConfig;
import com.alibaba.graphscope.common.ir.type.ArbitraryMapType;
import com.alibaba.graphscope.common.ir.type.GraphLabelType;
import com.alibaba.graphscope.common.ir.type.GraphProperty;
import com.alibaba.graphscope.common.utils.FileUtils;
import com.alibaba.graphscope.gaia.proto.OuterExpression;
import com.alibaba.graphscope.gremlin.antlr4x.parser.GremlinAntlr4Parser;
import com.alibaba.graphscope.gremlin.antlr4x.visitor.GraphBuilderVisitor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.util.JsonFormat;

import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class GraphBuilderTest {
    private static Configs configs;
    private static IrMeta irMeta;
    private static GraphRelOptimizer optimizer;

    @BeforeClass
    public static void beforeClass() {
        configs =
                new Configs(
                        ImmutableMap.of(
                                "graph.planner.is.on",
                                "true",
                                "graph.planner.opt",
                                "CBO",
                                "graph.planner.rules",
                                "FilterIntoJoinRule, FilterMatchRule, ExtendIntersectRule,"
                                        + " ExpandGetVFusionRule",
                                "graph.planner.cbo.glogue.schema",
                                "target/test-classes/statistics/modern_statistics.txt"));
        optimizer = new GraphRelOptimizer(configs);
        irMeta =
                Utils.mockIrMeta(
                        "schema/modern.json",
                        "statistics/modern_statistics.json",
                        optimizer.getGlogueHolder());
    }

    public static RelNode eval(String query) {
        GraphBuilder builder = Utils.mockGraphBuilder();
        GraphBuilderVisitor visitor = new GraphBuilderVisitor(builder);
        ParseTree parseTree = new GremlinAntlr4Parser().parse(query);
        return visitor.visit(parseTree).build();
    }

    public static RelNode eval(String query, GraphBuilder builder) {
        GraphBuilderVisitor visitor = new GraphBuilderVisitor(builder);
        ParseTree parseTree = new GremlinAntlr4Parser().parse(query);
        return visitor.visit(parseTree).build();
    }

    @Test
    public void g_V_elementMap_test() {
        GraphRelOptimizer optimizer = new GraphRelOptimizer(configs);
        IrMeta irMeta =
                Utils.mockIrMeta(
                        "schema/ldbc.json",
                        "statistics/ldbc30_statistics.json",
                        optimizer.getGlogueHolder());
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode node =
                eval(
                        "g.V(72057594037928268).as(\"a\").outE(\"KNOWS\").as(\"b\").inV().as(\"c\").select('a',"
                            + " \"b\").by(elementMap())",
                        builder);
        RelDataType projectType = node.getRowType().getFieldList().get(0).getType();
        RelDataType bValueType = projectType.getValueType();
        Assert.assertTrue(bValueType instanceof ArbitraryMapType);
        GraphLabelType labelType =
                (GraphLabelType)
                        ((ArbitraryMapType) bValueType)
                                .getKeyValueTypeMap().values().stream()
                                        .filter(k -> k.getValue() instanceof GraphLabelType)
                                        .findFirst()
                                        .get()
                                        .getValue();
        // make sure the inferred type contains the label type
        Assert.assertTrue(
                labelType.getLabelsEntry().stream().anyMatch(k -> k.getLabel().equals("KNOWS")));
    }

    @Test
    public void g_V_test() {
        RelNode node = eval("g.V()");
        Assert.assertEquals(
                "GraphLogicalProject($f0=[_], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[_], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_id_test() {
        RelNode node = eval("g.V(1, 2, 3)");
        Assert.assertEquals(
                "GraphLogicalProject($f0=[_], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[_], opt=[VERTEX], uniqueKeyFilters=[SEARCH(_.~id, Sarg[1, 2, 3])])",
                node.explain().trim());
    }

    @Test
    public void g_V_hasLabel_test() {
        RelNode node = eval("g.V().hasLabel('person')");
        Assert.assertEquals(
                "GraphLogicalProject($f0=[_], isAppend=[false])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[_], opt=[VERTEX])",
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
                "GraphLogicalProject($f0=[_], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[_], opt=[VERTEX], uniqueKeyFilters=[=(_.~id, 1)])",
                node.explain().trim());
    }

    @Test
    public void g_V_hasIds_test() {
        RelNode node = eval("g.V().hasId(1, 2, 3)");
        Assert.assertEquals(
                "GraphLogicalProject($f0=[_], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[_], opt=[VERTEX], uniqueKeyFilters=[SEARCH(_.~id, Sarg[1, 2, 3])])",
                node.explain().trim());
    }

    @Test
    public void g_V_has_name_marko_test() {
        RelNode node = eval("g.V().has('name', 'marko')");
        Assert.assertEquals(
                "GraphLogicalProject($f0=[_], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[_], fusedFilter=[[=(_.name, _UTF-8'marko')]], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_has_name_eq_marko_test() {
        RelNode node = eval("g.V().has('name', eq('marko'))");
        Assert.assertEquals(
                "GraphLogicalProject($f0=[_], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[_], fusedFilter=[[=(_.name, _UTF-8'marko')]], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_has_name_neq_marko_test() {
        RelNode node = eval("g.V().has('name', neq('marko'))");
        Assert.assertEquals(
                "GraphLogicalProject($f0=[_], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[_], fusedFilter=[[<>(_.name, _UTF-8'marko')]], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_has_age_gt_17_test() {
        RelNode node = eval("g.V().has('age', gt(17))");
        Assert.assertEquals(
                "GraphLogicalProject($f0=[_], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[_], fusedFilter=[[>(_.age, 17)]], opt=[VERTEX])",
                node.explain().trim());
    }

    // hasNot('age') -> age is null
    @Test
    public void g_V_hasNot_age_test() {
        RelNode node = eval("g.V().hasNot('age')");
        Assert.assertEquals(
                "GraphLogicalProject($f0=[_], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[_], fusedFilter=[[IS NULL(_.age)]], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_has_age_inside_17_20_test() {
        RelNode node = eval("g.V().has('age', inside(17, 20))");
        Assert.assertEquals(
                "GraphLogicalProject($f0=[_], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[_], fusedFilter=[[SEARCH(_.age, Sarg[[18..19]])]], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_has_age_outside_17_20_test() {
        RelNode node = eval("g.V().has('age', outside(17, 20))");
        Assert.assertEquals(
                "GraphLogicalProject($f0=[_], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[_], fusedFilter=[[SEARCH(_.age, Sarg[(-∞..18), (19..+∞)])]],"
                    + " opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_has_age_within_test() {
        RelNode node = eval("g.V().has('age', within(17, 20))");
        Assert.assertEquals(
                "GraphLogicalProject($f0=[_], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[_], fusedFilter=[[SEARCH(_.age, Sarg[17, 20])]], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_has_age_without_test() {
        RelNode node = eval("g.V().has('age', without(17, 20))");
        Assert.assertEquals(
                "GraphLogicalProject($f0=[_], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[_], fusedFilter=[[SEARCH(_.age, Sarg[(-∞..17), (17..20),"
                    + " (20..+∞)])]], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_has_name_endingWith_test() {
        RelNode node = eval("g.V().has('name', endingWith('mar'))");
        Assert.assertEquals(
                "GraphLogicalProject($f0=[_], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[_], fusedFilter=[[POSIX REGEX CASE SENSITIVE(_.name,"
                    + " _UTF-8'.*mar$')]], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_has_name_not_endingWith_test() {
        RelNode node = eval("g.V().has('name', notEndingWith('mar'))");
        Assert.assertEquals(
                "GraphLogicalProject($f0=[_], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[_], fusedFilter=[[NOT(POSIX REGEX CASE SENSITIVE(_.name,"
                    + " _UTF-8'.*mar$'))]], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_has_name_containing_test() {
        RelNode node = eval("g.V().has('name', containing('mar'))");
        Assert.assertEquals(
                "GraphLogicalProject($f0=[_], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[_], fusedFilter=[[POSIX REGEX CASE SENSITIVE(_.name,"
                    + " _UTF-8'.*mar.*')]], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_has_name_not_containing_test() {
        RelNode node = eval("g.V().has('name', notContaining('mar'))");
        Assert.assertEquals(
                "GraphLogicalProject($f0=[_], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[_], fusedFilter=[[NOT(POSIX REGEX CASE SENSITIVE(_.name,"
                    + " _UTF-8'.*mar.*'))]], opt=[VERTEX])",
                node.explain().trim());
    }

    // @.name not containing 'mar'
    @Test
    public void name_not_containing_expr_test() throws Exception {
        GraphBuilder builder = Utils.mockGraphBuilder();
        RexNode expr =
                builder.source(new SourceConfig(GraphOpt.Source.VERTEX))
                        .not(
                                builder.call(
                                        GraphStdOperatorTable.POSIX_REGEX_CASE_SENSITIVE,
                                        builder.variable(null, "name"),
                                        builder.literal(".*mar.*")));
        RexToProtoConverter converter = new RexToProtoConverter(true, false, Utils.rexBuilder);
        OuterExpression.Expression exprProto = expr.accept(converter);
        Assert.assertEquals(
                FileUtils.readJsonFromResource("proto/name_not_containing_expr.json"),
                JsonFormat.printer().print(exprProto));
    }

    @Test
    public void g_V_has_name_startingWith_test() {
        RelNode node = eval("g.V().has('name', startingWith('mar'))");
        Assert.assertEquals(
                "GraphLogicalProject($f0=[_], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[_], fusedFilter=[[POSIX REGEX CASE SENSITIVE(_.name,"
                    + " _UTF-8'^mar.*')]], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_has_name_not_startingWith_test() {
        RelNode node = eval("g.V().has('name', notStartingWith('mar'))");
        Assert.assertEquals(
                "GraphLogicalProject($f0=[_], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[_], fusedFilter=[[NOT(POSIX REGEX CASE SENSITIVE(_.name,"
                    + " _UTF-8'^mar.*'))]], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_has_name_marko_age_17_has_label_test() {
        RelNode node = eval("g.V().has('name', 'marko').has('age', 17).hasLabel('person')");
        Assert.assertEquals(
                "GraphLogicalProject($f0=[_], isAppend=[false])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[_], fusedFilter=[[AND(=(_.name, _UTF-8'marko'),"
                        + " =(_.age, 17))]], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_has_name_marko_age_17_has_label_error_test() {
        try {
            RelNode node = eval("g.V().has('name', 'marko').has('age', 17).hasLabel('software')");
        } catch (FrontendException e) {
            Assert.assertTrue(
                    e.getMessage()
                            .contains(
                                    "{property=age} not found; expected properties are: [id, name,"
                                            + " lang, creationDate]"));
            return;
        }
        Assert.fail();
    }

    @Test
    public void g_V_out_test() {
        RelNode node = eval("g.V().out()");
        Assert.assertEquals(
                "GraphLogicalProject($f0=[_], isAppend=[false])\n"
                    + "  GraphLogicalGetV(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[_], opt=[END])\n"
                    + "    GraphLogicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[_], opt=[OUT])\n"
                    + "      GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[_], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_out_label_test() {
        RelNode node = eval("g.V().out('knows')");
        Assert.assertEquals(
                "GraphLogicalProject($f0=[_], isAppend=[false])\n"
                    + "  GraphLogicalGetV(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[_], opt=[END])\n"
                    + "    GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}],"
                    + " alias=[_], opt=[OUT])\n"
                    + "      GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[_], opt=[VERTEX])",
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
                    + " alias=[_], opt=[OUT])\n"
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
                    + " tables=[knows]}], alias=[_], opt=[OUT])\n"
                    + "], getV=[GraphLogicalGetV(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[_], opt=[END])\n"
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
                        + "  GraphLogicalProject(a=[_.name], isAppend=[true])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                        + " person]}], alias=[_], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_valueMap_name_test() {
        RelNode node = eval("g.V().valueMap('name').as('a')");
        Assert.assertEquals(
                "GraphLogicalProject(a=[a], isAppend=[false])\n"
                        + "  GraphLogicalProject(a=[MAP(_UTF-8'name', _.name)],"
                        + " isAppend=[true])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                        + " person]}], alias=[_], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_valueMap_all_test() {
        RelNode node = eval("g.V().valueMap().as('a')");
        Assert.assertEquals(
                "GraphLogicalProject(a=[a], isAppend=[false])\n"
                        + "  GraphLogicalProject(a=[MAP(_UTF-8'id', _.id, _UTF-8'name',"
                        + " _.name, _UTF-8'lang', _.lang, _UTF-8'creationDate',"
                        + " _.creationDate, _UTF-8'age', _.age)], isAppend=[true])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                        + " person]}], alias=[_], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_elementMap_all_test() {
        RelNode node = eval("g.V().elementMap().as('a')");
        Assert.assertEquals(
                "GraphLogicalProject(a=[a], isAppend=[false])\n"
                        + "  GraphLogicalProject(a=[MAP(_UTF-8'~label', _.~label, _UTF-8'~id',"
                        + " _.~id, _UTF-8'id', _.id, _UTF-8'name', _.name,"
                        + " _UTF-8'lang', _.lang, _UTF-8'creationDate', _.creationDate,"
                        + " _UTF-8'age', _.age)], isAppend=[true])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                        + " person]}], alias=[_], opt=[VERTEX])",
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
                    + " knows]}], alias=[_], opt=[OUT])\n"
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
                    + " knows]}], alias=[_], opt=[OUT])\n"
                    + "        GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[a], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_order_test() {
        RelNode node = eval("g.V().order()");
        Assert.assertEquals(
                "GraphLogicalProject($f0=[_], isAppend=[false])\n"
                        + "  GraphLogicalSort(sort0=[_], dir0=[ASC])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                        + " person]}], alias=[_], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_order_by_name_age_test() {
        RelNode node = eval("g.V().order().by('name', desc).by('age', asc)");
        Assert.assertEquals(
                "GraphLogicalProject($f0=[_], isAppend=[false])\n"
                        + "  GraphLogicalSort(sort0=[_.name], sort1=[_.age], dir0=[DESC],"
                        + " dir1=[ASC])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                        + " person]}], alias=[_], opt=[VERTEX])",
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
                "GraphLogicalProject($f0=[_], isAppend=[false])\n"
                        + "  GraphLogicalSort(fetch=[10])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                        + " person]}], alias=[_], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_order_limit_test() {
        RelNode node = eval("g.V().order().by('name', desc).by('age', asc).limit(10)");
        Assert.assertEquals(
                "GraphLogicalProject($f0=[_], isAppend=[false])\n"
                        + "  GraphLogicalSort(sort0=[_.name], sort1=[_.age], dir0=[DESC],"
                        + " dir1=[ASC], fetch=[10])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                        + " person]}], alias=[_], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_group_test() {
        RelNode node = eval("g.V().group()");
        Assert.assertEquals(
                "GraphLogicalAggregate(keys=[{variables=[_], aliases=[keys]}],"
                    + " values=[[{operands=[_], aggFunction=COLLECT, alias='values',"
                    + " distinct=false}]])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[_], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_group_by_label_by_name_test() {
        RelNode node = eval("g.V().group().by('~label').by('name')");
        Assert.assertEquals(
                "GraphLogicalAggregate(keys=[{variables=[_.~label], aliases=[keys]}],"
                    + " values=[[{operands=[_.name], aggFunction=COLLECT, alias='values',"
                    + " distinct=false}]])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[_], opt=[VERTEX])",
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
                "GraphLogicalAggregate(keys=[{variables=[_.name], aliases=[keys]}],"
                    + " values=[[{operands=[_], aggFunction=COUNT, alias='values',"
                    + " distinct=false}]])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[_], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_dedup_test() {
        RelNode node = eval("g.V().dedup()");
        Assert.assertEquals(
                "GraphLogicalProject($f0=[_], isAppend=[false])\n"
                        + "  GraphLogicalDedupBy(dedupByKeys=[[_]])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                        + " person]}], alias=[_], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_dedup_by_name_test() {
        RelNode node = eval("g.V().dedup().by('name')");
        Assert.assertEquals(
                "GraphLogicalProject($f0=[_], isAppend=[false])\n"
                        + "  GraphLogicalDedupBy(dedupByKeys=[[_.name]])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                        + " person]}], alias=[_], opt=[VERTEX])",
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
                    + " knows]}], alias=[_], opt=[OUT])\n"
                    + "        GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[a], opt=[VERTEX])",
                node.explain().trim());
    }

    // ~id within [0, 72057594037927937]
    @Test
    public void id_in_list_expr_test() throws Exception {
        GraphBuilder builder = Utils.mockGraphBuilder();
        RexBuilder rexBuilder = Utils.rexBuilder;
        RexNode expr =
                rexBuilder.makeIn(
                        builder.source(new SourceConfig(GraphOpt.Source.VERTEX))
                                .variable(null, GraphProperty.ID_KEY),
                        ImmutableList.of(builder.literal(0), builder.literal(72057594037927937L)));
        OuterExpression.Expression exprProto =
                expr.accept(new RexToProtoConverter(true, false, rexBuilder));
        Assert.assertEquals(
                FileUtils.readJsonFromResource("proto/id_in_list_expr.json"),
                JsonFormat.printer().print(exprProto));
    }

    @Test
    public void g_V_identity() {
        // = g.V().as('a'), `identity()` do nothing
        RelNode node = eval("g.V().identity().as('a')");
        Assert.assertEquals(
                "GraphLogicalProject(a=[a], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[a], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_union_out_in_test() {
        RelNode node = eval("g.V().union(out(), in())");
        Assert.assertEquals(
                "GraphLogicalProject($f0=[_], isAppend=[false])\n"
                    + "  LogicalUnion(all=[true])\n"
                    + "    GraphLogicalGetV(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[_], opt=[END])\n"
                    + "      GraphLogicalExpand(tableConfig=[{isAll=true, tables=[created,"
                    + " knows]}], alias=[_], opt=[OUT])\n"
                    + "        GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[_], opt=[VERTEX])\n"
                    + "    GraphLogicalGetV(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[_], opt=[START])\n"
                    + "      GraphLogicalExpand(tableConfig=[{isAll=true, tables=[created,"
                    + " knows]}], alias=[_], opt=[IN])\n"
                    + "        GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[_], opt=[VERTEX])",
                node.explain().trim());
        RelOptPlanner planner =
                Utils.mockPlanner(ExpandGetVFusionRule.BasicExpandGetVFusionRule.Config.DEFAULT);
        planner.setRoot(node);
        RelNode after = planner.findBestExp();
        Assert.assertEquals(
                "GraphLogicalProject($f0=[_], isAppend=[false])\n"
                    + "  LogicalUnion(all=[true])\n"
                    + "    GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[_], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "      GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[_], opt=[VERTEX])\n"
                    + "    GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[_], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "      GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[_], opt=[VERTEX])",
                after.explain().trim());
    }

    @Test
    public void g_V_out_union_out_in_test() {
        RelNode node = eval("g.V().out().union(out(), in())");
        RelOptPlanner planner =
                Utils.mockPlanner(ExpandGetVFusionRule.BasicExpandGetVFusionRule.Config.DEFAULT);
        planner.setRoot(node);
        RelNode after = planner.findBestExp();
        Assert.assertEquals(
                "root:\n"
                    + "GraphLogicalProject($f0=[_], isAppend=[false])\n"
                    + "  LogicalUnion(all=[true])\n"
                    + "    GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[_], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "      CommonTableScan(table=[[common#1467015418]])\n"
                    + "    GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[_], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "      CommonTableScan(table=[[common#1467015418]])\n"
                    + "common#1467015418:\n"
                    + "GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[_], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[_], opt=[VERTEX])",
                com.alibaba.graphscope.common.ir.tools.Utils.toString(after).trim());
    }

    @Test
    public void g_V_out_union_out_in_inXin_test() {
        RelNode node = eval("g.V().out().union(out(), in(), in().in())");
        RelOptPlanner planner =
                Utils.mockPlanner(ExpandGetVFusionRule.BasicExpandGetVFusionRule.Config.DEFAULT);
        planner.setRoot(node);
        RelNode after = planner.findBestExp();
        Assert.assertEquals(
                "root:\n"
                    + "GraphLogicalProject($f0=[_], isAppend=[false])\n"
                    + "  LogicalUnion(all=[true])\n"
                    + "    GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[_], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "      CommonTableScan(table=[[common#1467015418]])\n"
                    + "    GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[_], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "      CommonTableScan(table=[[common#1467015418]])\n"
                    + "    GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[_], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "      GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created,"
                    + " knows]}], alias=[_], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "        CommonTableScan(table=[[common#1467015418]])\n"
                    + "common#1467015418:\n"
                    + "GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[_], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[_], opt=[VERTEX])",
                com.alibaba.graphscope.common.ir.tools.Utils.toString(after).trim());
    }

    @Test
    public void g_V_union_out_inXunion_out_outX_test() {
        RelNode node = eval("g.V().union(out(), in().union(out(), out()))");
        RelOptPlanner planner =
                Utils.mockPlanner(ExpandGetVFusionRule.BasicExpandGetVFusionRule.Config.DEFAULT);
        planner.setRoot(node);
        RelNode after = planner.findBestExp();
        Assert.assertEquals(
                "root:\n"
                    + "GraphLogicalProject($f0=[_], isAppend=[false])\n"
                    + "  LogicalUnion(all=[true])\n"
                    + "    GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[_], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "      GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[_], opt=[VERTEX])\n"
                    + "    LogicalUnion(all=[true])\n"
                    + "      GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created,"
                    + " knows]}], alias=[_], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "        CommonTableScan(table=[[common#-957851013]])\n"
                    + "      GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created,"
                    + " knows]}], alias=[_], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "        CommonTableScan(table=[[common#-957851013]])\n"
                    + "common#-957851013:\n"
                    + "GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[_], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[_], opt=[VERTEX])",
                com.alibaba.graphscope.common.ir.tools.Utils.toString(after).trim());
    }

    @Test
    public void g_V_out_union_out_inXunion_out_outX_test() {
        RelNode node = eval("g.V().out().union(out(), in().union(out(), out()))");
        RelOptPlanner planner =
                Utils.mockPlanner(ExpandGetVFusionRule.BasicExpandGetVFusionRule.Config.DEFAULT);
        planner.setRoot(node);
        RelNode after = planner.findBestExp();
        Assert.assertEquals(
                "root:\n"
                    + "GraphLogicalProject($f0=[_], isAppend=[false])\n"
                    + "  LogicalUnion(all=[true])\n"
                    + "    GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[_], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "      CommonTableScan(table=[[common#1467015418]])\n"
                    + "    LogicalUnion(all=[true])\n"
                    + "      GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created,"
                    + " knows]}], alias=[_], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "        CommonTableScan(table=[[common#910068342]])\n"
                    + "      GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created,"
                    + " knows]}], alias=[_], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "        CommonTableScan(table=[[common#910068342]])\n"
                    + "common#1467015418:\n"
                    + "GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[_], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[_], opt=[VERTEX])\n"
                    + "common#910068342:\n"
                    + "GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[_], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "  CommonTableScan(table=[[common#1467015418]])",
                com.alibaba.graphscope.common.ir.tools.Utils.toString(after).trim());
    }

    @Test
    public void g_V_out_union_outXunion_out_outX_inXunion_out_outX_test() {
        RelNode node =
                eval("g.V().out().union(out().union(out(), out()), in().union(out(), out()))");
        RelOptPlanner planner =
                Utils.mockPlanner(ExpandGetVFusionRule.BasicExpandGetVFusionRule.Config.DEFAULT);
        planner.setRoot(node);
        RelNode after = planner.findBestExp();
        Assert.assertEquals(
                "root:\n"
                    + "GraphLogicalProject($f0=[_], isAppend=[false])\n"
                    + "  LogicalUnion(all=[true])\n"
                    + "    LogicalUnion(all=[true])\n"
                    + "      GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created,"
                    + " knows]}], alias=[_], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "        CommonTableScan(table=[[common#-53889483]])\n"
                    + "      GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created,"
                    + " knows]}], alias=[_], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "        CommonTableScan(table=[[common#-53889483]])\n"
                    + "    LogicalUnion(all=[true])\n"
                    + "      GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created,"
                    + " knows]}], alias=[_], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "        CommonTableScan(table=[[common#910068342]])\n"
                    + "      GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created,"
                    + " knows]}], alias=[_], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "        CommonTableScan(table=[[common#910068342]])\n"
                    + "common#-53889483:\n"
                    + "GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[_], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "  CommonTableScan(table=[[common#1467015418]])\n"
                    + "common#1467015418:\n"
                    + "GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[_], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[_], opt=[VERTEX])\n"
                    + "common#910068342:\n"
                    + "GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[_], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "  CommonTableScan(table=[[common#1467015418]])",
                com.alibaba.graphscope.common.ir.tools.Utils.toString(after).trim());
    }

    // `g.V().in()` is extracted as the common sub-plan of the root union
    @Test
    public void g_V_union_inXunion_out_outX_inXunion_out_outX_test() {
        RelNode node = eval("g.V().union(in().union(out(), out()), in().union(out(), out()))");
        RelOptPlanner planner =
                Utils.mockPlanner(ExpandGetVFusionRule.BasicExpandGetVFusionRule.Config.DEFAULT);
        planner.setRoot(node);
        RelNode after = planner.findBestExp();
        Assert.assertEquals(
                "root:\n"
                    + "GraphLogicalProject($f0=[_], isAppend=[false])\n"
                    + "  LogicalUnion(all=[true])\n"
                    + "    LogicalUnion(all=[true])\n"
                    + "      GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created,"
                    + " knows]}], alias=[_], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "        CommonTableScan(table=[[common#-957851013]])\n"
                    + "      GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created,"
                    + " knows]}], alias=[_], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "        CommonTableScan(table=[[common#-957851013]])\n"
                    + "    LogicalUnion(all=[true])\n"
                    + "      GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created,"
                    + " knows]}], alias=[_], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "        CommonTableScan(table=[[common#-957851013]])\n"
                    + "      GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created,"
                    + " knows]}], alias=[_], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "        CommonTableScan(table=[[common#-957851013]])\n"
                    + "common#-957851013:\n"
                    + "GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[_], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[_], opt=[VERTEX])",
                com.alibaba.graphscope.common.ir.tools.Utils.toString(after).trim());
    }

    // `g.V().out().in()` is extracted as the common sub-plan of the root union
    @Test
    public void g_V_out_union_inXunion_out_outX_inXunion_out_outX_test() {
        RelNode node =
                eval("g.V().out().union(in().union(out(), out()), in().union(out(), out()))");
        RelOptPlanner planner =
                Utils.mockPlanner(ExpandGetVFusionRule.BasicExpandGetVFusionRule.Config.DEFAULT);
        planner.setRoot(node);
        RelNode after = planner.findBestExp();
        Assert.assertEquals(
                "root:\n"
                    + "GraphLogicalProject($f0=[_], isAppend=[false])\n"
                    + "  LogicalUnion(all=[true])\n"
                    + "    LogicalUnion(all=[true])\n"
                    + "      GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created,"
                    + " knows]}], alias=[_], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "        CommonTableScan(table=[[common#910068342]])\n"
                    + "      GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created,"
                    + " knows]}], alias=[_], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "        CommonTableScan(table=[[common#910068342]])\n"
                    + "    LogicalUnion(all=[true])\n"
                    + "      GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created,"
                    + " knows]}], alias=[_], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "        CommonTableScan(table=[[common#910068342]])\n"
                    + "      GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created,"
                    + " knows]}], alias=[_], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "        CommonTableScan(table=[[common#910068342]])\n"
                    + "common#910068342:\n"
                    + "GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[_], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "  CommonTableScan(table=[[common#1467015418]])\n"
                    + "common#1467015418:\n"
                    + "GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[_], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[_], opt=[VERTEX])",
                com.alibaba.graphscope.common.ir.tools.Utils.toString(after).trim());
    }

    @Test
    public void g_V_union_identity_test() {
        RelNode node = eval("g.V().union(identity(), identity())");
        Assert.assertEquals(
                "GraphLogicalProject($f0=[_], isAppend=[false])\n"
                        + "  LogicalUnion(all=[true])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                        + " person]}], alias=[_], opt=[VERTEX])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                        + " person]}], alias=[_], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_has_union_identity_identity_test() {
        RelNode node = eval("g.V().has('name', 'marko').union(identity(), identity())");
        Assert.assertEquals(
                "root:\n"
                    + "GraphLogicalProject($f0=[_], isAppend=[false])\n"
                    + "  LogicalUnion(all=[true])\n"
                    + "    CommonTableScan(table=[[common#-8235311]])\n"
                    + "    CommonTableScan(table=[[common#-8235311]])\n"
                    + "common#-8235311:\n"
                    + "GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[_], fusedFilter=[[=(_.name, _UTF-8'marko')]], opt=[VERTEX])",
                com.alibaba.graphscope.common.ir.tools.Utils.toString(node).trim());
    }

    @Test
    public void g_V_select_expr_test() {
        RelNode node;

        node = eval("g.V().select(expr(_))");
        Assert.assertEquals(
                "GraphLogicalProject($f0=[_], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[_], opt=[VERTEX])",
                node.explain().trim());

        node = eval("g.V().select(expr(_.name))");
        Assert.assertEquals(
                "GraphLogicalProject(name=[name], isAppend=[false])\n"
                        + "  GraphLogicalProject(name=[_.name], isAppend=[true])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                        + " person]}], alias=[_], opt=[VERTEX])",
                node.explain().trim());

        node = eval("g.V().as('a').select(expr(a))");
        Assert.assertEquals(
                "GraphLogicalProject(a=[a], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[a], opt=[VERTEX])",
                node.explain().trim());

        node = eval("g.V().as('a').select(expr(a.name))");
        Assert.assertEquals(
                "GraphLogicalProject(name=[name], isAppend=[false])\n"
                        + "  GraphLogicalProject(name=[a.name], isAppend=[true])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                        + " person]}], alias=[a], opt=[VERTEX])",
                node.explain().trim());

        node = eval("g.V().as('a').out().as('b').select(expr((a.age+b.age)/2*3))");
        Assert.assertEquals(
                "GraphLogicalProject($f0=[$f0], isAppend=[false])\n"
                    + "  GraphLogicalProject($f0=[*(/(+(a.age, b.age), 2), 3)], isAppend=[true])\n"
                    + "    GraphLogicalGetV(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[b], opt=[END])\n"
                    + "      GraphLogicalExpand(tableConfig=[{isAll=true, tables=[created,"
                    + " knows]}], alias=[_], opt=[OUT])\n"
                    + "        GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[a], opt=[VERTEX])",
                node.explain().trim());

        node = eval("g.V().as('a').select(expr(POWER(a.age, 3)))");
        Assert.assertEquals(
                "GraphLogicalProject($f0=[$f0], isAppend=[false])\n"
                        + "  GraphLogicalProject($f0=[POWER(a.age, 3)], isAppend=[true])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                        + " person]}], alias=[a], opt=[VERTEX])",
                node.explain().trim());

        node = eval("g.V().as('a').select(expr(a.age & 1 | 2 ^ 3))");
        Assert.assertEquals(
                "GraphLogicalProject($f0=[$f0], isAppend=[false])\n"
                        + "  GraphLogicalProject($f0=[^(|(&(a.age, 1), 2), 3)], isAppend=[true])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                        + " person]}], alias=[a], opt=[VERTEX])",
                node.explain().trim());

        node = eval("g.V().as('a').select(expr(a.age << 2 >> 3))");
        Assert.assertEquals(
                "GraphLogicalProject($f0=[$f0], isAppend=[false])\n"
                        + "  GraphLogicalProject($f0=[>>(<<(a.age, 2), 3)], isAppend=[true])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                        + " person]}], alias=[a], opt=[VERTEX])",
                node.explain().trim());

        node = eval("g.V().as('a').select(expr(sum(a.age) * 2 / 3))");
        Assert.assertEquals(
                "GraphLogicalProject($f0=[$f0], isAppend=[false])\n"
                        + "  GraphLogicalProject($f0=[/(*(EXPR$0, 2), 3)], isAppend=[true])\n"
                        + "    GraphLogicalAggregate(keys=[{variables=[], aliases=[]}],"
                        + " values=[[{operands=[a.age], aggFunction=SUM, alias='EXPR$0',"
                        + " distinct=false}]])\n"
                        + "      GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                        + " person]}], alias=[a], opt=[VERTEX])",
                node.explain().trim());

        node = eval("g.V().as('a').select(expr(head(collect(a.age))))");
        Assert.assertEquals(
                "GraphLogicalAggregate(keys=[{variables=[], aliases=[]}],"
                    + " values=[[{operands=[a.age], aggFunction=FIRST_VALUE, alias='$f0',"
                    + " distinct=false}]])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[a], opt=[VERTEX])",
                node.explain().trim());

        // test operator precedence of bit manipulation
        node = eval("g.V().select(expr(2 ^ 3 * 2))");
        Assert.assertEquals(
                "GraphLogicalProject($f0=[$f0], isAppend=[false])\n"
                        + "  GraphLogicalProject($f0=[^(2, *(3, 2))], isAppend=[true])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                        + " person]}], alias=[_], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_where_expr_test() {
        RelNode node;

        node = eval("g.V().where(expr(_.name = 'marko'))");
        Assert.assertEquals(
                "GraphLogicalProject($f0=[_], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[_], fusedFilter=[[=(_.name, _UTF-8'marko')]], opt=[VERTEX])",
                node.explain().trim());

        node = eval("g.V().as('a').out().where(expr(_.age + 1 > a.age))");
        Assert.assertEquals(
                "GraphLogicalProject($f0=[_], isAppend=[false])\n"
                    + "  LogicalFilter(condition=[>(+(_.age, 1), a.age)])\n"
                    + "    GraphLogicalGetV(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[_], opt=[END])\n"
                    + "      GraphLogicalExpand(tableConfig=[{isAll=true, tables=[created,"
                    + " knows]}], alias=[_], opt=[OUT])\n"
                    + "        GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[a], opt=[VERTEX])",
                node.explain().trim());

        node = eval("g.V().as('a').outE().as('b').where(expr(labels(a) = type(b)))");
        Assert.assertEquals(
                "GraphLogicalProject(b=[b], isAppend=[false])\n"
                    + "  LogicalFilter(condition=[=(a.~label, b.~label)])\n"
                    + "    GraphLogicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[b], opt=[OUT])\n"
                    + "      GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[a], opt=[VERTEX])",
                node.explain().trim());

        node = eval("g.V().as('a').where(expr(a.name = 'marko' and (a.age > 30 OR a.age < 20)))");
        Assert.assertEquals(
                "GraphLogicalProject(a=[a], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[a], fusedFilter=[[AND(=(_.name, _UTF-8'marko'), OR(>(_.age, 30),"
                    + " <(_.age, 20)))]], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_where_out_out_test() {
        RelNode node = eval("g.V().where(out().out())");
        RelOptPlanner planner =
                Utils.mockPlanner(ExpandGetVFusionRule.BasicExpandGetVFusionRule.Config.DEFAULT);
        planner.setRoot(node);
        node = planner.findBestExp();
        Assert.assertEquals(
                "GraphLogicalProject($f0=[_], isAppend=[false])\n"
                    + "  LogicalFilter(condition=[EXISTS({\n"
                    + "GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[_], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "  GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[_], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "    CommonTableScan(table=[[common#129647922]])\n"
                    + "})])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[_], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_where_not_out_out_test() {
        RelNode node = eval("g.V().where(not(out().out()))");
        RelOptPlanner planner =
                Utils.mockPlanner(ExpandGetVFusionRule.BasicExpandGetVFusionRule.Config.DEFAULT);
        planner.setRoot(node);
        node = planner.findBestExp();
        Assert.assertEquals(
                "GraphLogicalProject($f0=[_], isAppend=[false])\n"
                    + "  LogicalFilter(condition=[NOT(EXISTS({\n"
                    + "GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[_], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "  GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[_], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "    CommonTableScan(table=[[common#129647922]])\n"
                    + "}))])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[_], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_where_as_a_out_as_b_select_a_b_where_as_a_out_as_b_test() {
        RelNode node =
                eval(
                        "g.V().as(\"a\").out().as(\"b\").select(\"a\","
                                + " \"b\").where(as('a').out('knows').as('b'))");
        RelOptPlanner planner =
                Utils.mockPlanner(ExpandGetVFusionRule.BasicExpandGetVFusionRule.Config.DEFAULT);
        planner.setRoot(node);
        node = planner.findBestExp();
        Assert.assertEquals(
                "GraphLogicalProject($f0=[$f0], isAppend=[false])\n"
                        + "  LogicalFilter(condition=[EXISTS({\n"
                        + "LogicalFilter(condition=[=(_, b)])\n"
                        + "  GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[knows]}],"
                        + " alias=[_], opt=[OUT], physicalOpt=[VERTEX])\n"
                        + "    GraphLogicalProject(_=[a], isAppend=[true])\n"
                        + "      CommonTableScan(table=[[common#-630412335]])\n"
                        + "})])\n"
                        + "    GraphLogicalProject($f0=[MAP(_UTF-8'a', a, _UTF-8'b', b)],"
                        + " isAppend=[true])\n"
                        + "      GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created,"
                        + " knows]}], alias=[b], opt=[OUT], physicalOpt=[VERTEX])\n"
                        + "        GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                        + " person]}], alias=[a], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_where_as_a_out_as_b_select_a_b_where_as_a_has_name_marko_test() {
        RelNode node =
                eval(
                        "g.V().as(\"a\").out().in().as(\"b\").select(\"a\","
                                + " \"b\").where(__.as(\"b\").has(\"name\", \"marko\"))");
        RelOptPlanner planner =
                Utils.mockPlanner(ExpandGetVFusionRule.BasicExpandGetVFusionRule.Config.DEFAULT);
        planner.setRoot(node);
        node = planner.findBestExp();
        Assert.assertEquals(
                "GraphLogicalProject($f0=[$f0], isAppend=[false])\n"
                        + "  LogicalFilter(condition=[EXISTS({\n"
                        + "LogicalFilter(condition=[=(_.name, _UTF-8'marko')])\n"
                        + "  GraphLogicalProject(_=[b], isAppend=[true])\n"
                        + "    CommonTableScan(table=[[common#1582012392]])\n"
                        + "})])\n"
                        + "    GraphLogicalProject($f0=[MAP(_UTF-8'a', a, _UTF-8'b', b)],"
                        + " isAppend=[true])\n"
                        + "      GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created,"
                        + " knows]}], alias=[b], opt=[IN], physicalOpt=[VERTEX])\n"
                        + "        GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created,"
                        + " knows]}], alias=[_], opt=[OUT], physicalOpt=[VERTEX])\n"
                        + "          GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                        + " person]}], alias=[a], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_where_a_neq_b_by_out_count_test() {
        RelNode node =
                eval("g.V().as(\"a\").out().as(\"b\").where(\"a\", neq(\"b\")).by(out().count())");
        RelOptPlanner planner =
                Utils.mockPlanner(ExpandGetVFusionRule.BasicExpandGetVFusionRule.Config.DEFAULT);
        planner.setRoot(node);
        node = planner.findBestExp();
        Assert.assertEquals(
                "GraphLogicalProject(b=[b], isAppend=[false])\n"
                    + "  LogicalFilter(condition=[<>($f0, $f1)])\n"
                    + "    GraphLogicalProject($f0=[$SCALAR_QUERY({\n"
                    + "GraphLogicalAggregate(keys=[{variables=[], aliases=[]}],"
                    + " values=[[{operands=[_], aggFunction=COUNT, alias='$f0',"
                    + " distinct=false}]])\n"
                    + "  GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[_], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "    GraphLogicalProject(_=[a], isAppend=[true])\n"
                    + "      CommonTableScan(table=[[common#-1583470443]])\n"
                    + "})], $f1=[$SCALAR_QUERY({\n"
                    + "GraphLogicalAggregate(keys=[{variables=[], aliases=[]}],"
                    + " values=[[{operands=[_], aggFunction=COUNT, alias='$f0',"
                    + " distinct=false}]])\n"
                    + "  GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[_], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "    GraphLogicalProject(_=[b], isAppend=[true])\n"
                    + "      CommonTableScan(table=[[common#-1583470443]])\n"
                    + "})], isAppend=[true])\n"
                    + "      GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created,"
                    + " knows]}], alias=[b], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "        GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[a], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_select_a_b_by_out_count_test() {
        RelNode node =
                eval("g.V().as(\"a\").out().as(\"b\").select(\"a\", \"b\").by(out().count())");
        RelOptPlanner planner =
                Utils.mockPlanner(ExpandGetVFusionRule.BasicExpandGetVFusionRule.Config.DEFAULT);
        planner.setRoot(node);
        node = planner.findBestExp();
        Assert.assertEquals(
                "GraphLogicalProject($f2=[$f2], isAppend=[false])\n"
                    + "  GraphLogicalProject($f2=[MAP(_UTF-8'a', $f0, _UTF-8'b', $f1)],"
                    + " isAppend=[true])\n"
                    + "    GraphLogicalProject($f0=[$SCALAR_QUERY({\n"
                    + "GraphLogicalAggregate(keys=[{variables=[], aliases=[]}],"
                    + " values=[[{operands=[_], aggFunction=COUNT, alias='$f0',"
                    + " distinct=false}]])\n"
                    + "  GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[_], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "    GraphLogicalProject(_=[a], isAppend=[true])\n"
                    + "      CommonTableScan(table=[[common#-1583470443]])\n"
                    + "})], $f1=[$SCALAR_QUERY({\n"
                    + "GraphLogicalAggregate(keys=[{variables=[], aliases=[]}],"
                    + " values=[[{operands=[_], aggFunction=COUNT, alias='$f0',"
                    + " distinct=false}]])\n"
                    + "  GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[_], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "    GraphLogicalProject(_=[b], isAppend=[true])\n"
                    + "      CommonTableScan(table=[[common#-1583470443]])\n"
                    + "})], isAppend=[true])\n"
                    + "      GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created,"
                    + " knows]}], alias=[b], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "        GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[a], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_select_a_b_by_out_count_by_name_test() {
        RelNode node =
                eval(
                        "g.V().as(\"a\").out().as(\"b\").select(\"a\","
                                + " \"b\").by(out().count()).by(values(\"name\"))");
        RelOptPlanner planner =
                Utils.mockPlanner(ExpandGetVFusionRule.BasicExpandGetVFusionRule.Config.DEFAULT);
        planner.setRoot(node);
        node = planner.findBestExp();
        Assert.assertEquals(
                "GraphLogicalProject($f1=[$f1], isAppend=[false])\n"
                    + "  GraphLogicalProject($f1=[MAP(_UTF-8'a', $f0, _UTF-8'b', b.name)],"
                    + " isAppend=[true])\n"
                    + "    GraphLogicalProject($f0=[$SCALAR_QUERY({\n"
                    + "GraphLogicalAggregate(keys=[{variables=[], aliases=[]}],"
                    + " values=[[{operands=[_], aggFunction=COUNT, alias='$f0',"
                    + " distinct=false}]])\n"
                    + "  GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[_], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "    GraphLogicalProject(_=[a], isAppend=[true])\n"
                    + "      CommonTableScan(table=[[common#-1583470443]])\n"
                    + "})], isAppend=[true])\n"
                    + "      GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created,"
                    + " knows]}], alias=[b], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "        GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[a], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_dedup_a_b_by_out_count_test() {
        RelNode node =
                eval("g.V().as(\"a\").out().as(\"b\").dedup(\"a\", \"b\").by(out().count())");
        RelOptPlanner planner =
                Utils.mockPlanner(ExpandGetVFusionRule.BasicExpandGetVFusionRule.Config.DEFAULT);
        planner.setRoot(node);
        node = planner.findBestExp();
        Assert.assertEquals(
                "GraphLogicalProject(b=[b], isAppend=[false])\n"
                    + "  GraphLogicalDedupBy(dedupByKeys=[[$f0, $f1]])\n"
                    + "    GraphLogicalProject($f0=[$SCALAR_QUERY({\n"
                    + "GraphLogicalAggregate(keys=[{variables=[], aliases=[]}],"
                    + " values=[[{operands=[_], aggFunction=COUNT, alias='$f0',"
                    + " distinct=false}]])\n"
                    + "  GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[_], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "    GraphLogicalProject(_=[a], isAppend=[true])\n"
                    + "      CommonTableScan(table=[[common#-1583470443]])\n"
                    + "})], $f1=[$SCALAR_QUERY({\n"
                    + "GraphLogicalAggregate(keys=[{variables=[], aliases=[]}],"
                    + " values=[[{operands=[_], aggFunction=COUNT, alias='$f0',"
                    + " distinct=false}]])\n"
                    + "  GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[_], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "    GraphLogicalProject(_=[b], isAppend=[true])\n"
                    + "      CommonTableScan(table=[[common#-1583470443]])\n"
                    + "})], isAppend=[true])\n"
                    + "      GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created,"
                    + " knows]}], alias=[b], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "        GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[a], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_order_by_out_count_by_name_test() {
        RelNode node =
                eval(
                        "g.V().as(\"a\").out().order().by(out().count(),"
                                + " asc).by(select(\"a\").values(\"name\"), desc)");
        RelOptPlanner planner =
                Utils.mockPlanner(ExpandGetVFusionRule.BasicExpandGetVFusionRule.Config.DEFAULT);
        planner.setRoot(node);
        node = planner.findBestExp();
        Assert.assertEquals(
                "GraphLogicalProject($f1=[$f1], isAppend=[false])\n"
                    + "  GraphLogicalSort(sort0=[$f0], sort1=[a.name], dir0=[ASC], dir1=[DESC])\n"
                    + "    GraphLogicalProject($f0=[$SCALAR_QUERY({\n"
                    + "GraphLogicalAggregate(keys=[{variables=[], aliases=[]}],"
                    + " values=[[{operands=[_], aggFunction=COUNT, alias='$f0',"
                    + " distinct=false}]])\n"
                    + "  GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[_], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "    CommonTableScan(table=[[common#-527129288]])\n"
                    + "})], isAppend=[true])\n"
                    + "      GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created,"
                    + " knows]}], alias=[$f1], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "        GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[a], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_groupCount_by_out_count_by_age_test() {
        RelNode node =
                eval(
                        "g.V().out().as(\"b\").groupCount().by(out().count().as(\"key1\"),"
                                + " select(\"b\").values(\"age\").as(\"key2\"))");
        RelOptPlanner planner =
                Utils.mockPlanner(ExpandGetVFusionRule.BasicExpandGetVFusionRule.Config.DEFAULT);
        planner.setRoot(node);
        node = planner.findBestExp();
        Assert.assertEquals(
                "GraphLogicalAggregate(keys=[{variables=[$f0, b.age], aliases=[key1, key2]}],"
                    + " values=[[{operands=[_], aggFunction=COUNT, alias='values',"
                    + " distinct=false}]])\n"
                    + "  GraphLogicalProject($f0=[$SCALAR_QUERY({\n"
                    + "GraphLogicalAggregate(keys=[{variables=[], aliases=[]}],"
                    + " values=[[{operands=[_], aggFunction=COUNT, alias='$f0',"
                    + " distinct=false}]])\n"
                    + "  GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[_], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "    CommonTableScan(table=[[common#410674263]])\n"
                    + "})], isAppend=[true])\n"
                    + "    GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[b], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "      GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[_], opt=[VERTEX])",
                node.explain().trim());
    }

    // The following subtask will be simplified as expression

    @Test
    public void g_V_where_values_property() {
        // existence of `age` is converted to operator `IS NOT NULL`
        RelNode node;
        node = eval("g.V().where(values(\"age\"))");
        Assert.assertEquals(
                "GraphLogicalProject($f0=[_], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[_], fusedFilter=[[IS NOT NULL(_.age)]], opt=[VERTEX])",
                node.explain().trim());

        // non-existence of `age` is converted to operator `IS NULL`
        node = eval("g.V().where(not(values(\"age\")))");
        Assert.assertEquals(
                "GraphLogicalProject($f0=[_], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[_], fusedFilter=[[IS NULL(_.age)]], opt=[VERTEX])",
                node.explain().trim());

        // if property exists for each label type, the condition will be omitted
        node = eval("g.V().where(values(\"name\"))");
        Assert.assertEquals(
                "GraphLogicalProject($f0=[_], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[_], opt=[VERTEX])",
                node.explain().trim());

        // todo: fuse the condition into the source
        node = eval("g.V().as(\"a\").out().where(select(\"a\").values(\"age\"))");
        RelOptPlanner planner =
                Utils.mockPlanner(ExpandGetVFusionRule.BasicExpandGetVFusionRule.Config.DEFAULT);
        planner.setRoot(node);
        node = planner.findBestExp();
        Assert.assertEquals(
                "GraphLogicalProject($f0=[_], isAppend=[false])\n"
                    + "  LogicalFilter(condition=[IS NOT NULL(a.age)])\n"
                    + "    GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[_], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "      GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[a], opt=[VERTEX])",
                node.explain().trim());

        node = eval("g.V().as(\"a\").out().where(not(select(\"a\").values(\"age\")))");
        planner.setRoot(node);
        node = planner.findBestExp();
        Assert.assertEquals(
                "GraphLogicalProject($f0=[_], isAppend=[false])\n"
                    + "  LogicalFilter(condition=[IS NULL(a.age)])\n"
                    + "    GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[_], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "      GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[a], opt=[VERTEX])",
                node.explain().trim());

        node = eval("g.V().as(\"a\").out().order().by(select(\"a\").by(\"name\"))");
        planner.setRoot(node);
        node = planner.findBestExp();
        Assert.assertEquals(
                "GraphLogicalProject($f0=[_], isAppend=[false])\n"
                    + "  GraphLogicalSort(sort0=[a.name], dir0=[ASC])\n"
                    + "    GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[_], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "      GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[a], opt=[VERTEX])",
                node.explain().trim());

        node = eval("g.V().as(\"a\").out().group().by(select(\"a\").by(\"name\"))");
        planner.setRoot(node);
        node = planner.findBestExp();
        Assert.assertEquals(
                "GraphLogicalAggregate(keys=[{variables=[a.name], aliases=[keys]}],"
                    + " values=[[{operands=[_], aggFunction=COLLECT, alias='values',"
                    + " distinct=false}]])\n"
                    + "  GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[_], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[a], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_select_expr_date_minus_date_test() {
        RelNode node =
                eval(
                        "g.V().as('a').both().as('b').select(expr((a.creationDate - b.creationDate)"
                                + " / 1000))");
        RelOptPlanner planner =
                Utils.mockPlanner(ExpandGetVFusionRule.BasicExpandGetVFusionRule.Config.DEFAULT);
        planner.setRoot(node);
        RelNode after = planner.findBestExp();
        Assert.assertEquals(
                "GraphLogicalProject($f0=[$f0], isAppend=[false])\n"
                    + "  GraphLogicalProject($f0=[/(DATETIME_MINUS(a.creationDate, b.creationDate,"
                    + " null:INTERVAL MILLISECOND), 1000)], isAppend=[true])\n"
                    + "    GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[b], opt=[BOTH], physicalOpt=[VERTEX])\n"
                    + "      GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[a], opt=[VERTEX])",
                after.explain().trim());
    }

    @Test
    public void g_V_select_expr_date_minus_interval_test() {
        RelNode node =
                eval(
                        "g.V().as('a').both().as('b').select(expr(a.creationDate - duration({years:"
                                + " 1})))");
        RelOptPlanner planner =
                Utils.mockPlanner(ExpandGetVFusionRule.BasicExpandGetVFusionRule.Config.DEFAULT);
        planner.setRoot(node);
        RelNode after = planner.findBestExp();
        Assert.assertEquals(
                "GraphLogicalProject($f0=[$f0], isAppend=[false])\n"
                    + "  GraphLogicalProject($f0=[-(a.creationDate, 1:INTERVAL YEAR)],"
                    + " isAppend=[true])\n"
                    + "    GraphPhysicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[b], opt=[BOTH], physicalOpt=[VERTEX])\n"
                    + "      GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[a], opt=[VERTEX])",
                after.explain().trim());
    }

    @Test
    public void g_V_select_expr_interval_minus_interval_test() {
        RelNode node = eval("g.V().select(expr(duration({years: 1}) - duration({months: 1})))");
        RelOptPlanner planner =
                Utils.mockPlanner(ExpandGetVFusionRule.BasicExpandGetVFusionRule.Config.DEFAULT);
        planner.setRoot(node);
        RelNode after = planner.findBestExp();
        Assert.assertEquals(
                "GraphLogicalProject($f0=[$f0], isAppend=[false])\n"
                        + "  GraphLogicalProject($f0=[-(1:INTERVAL YEAR, 1:INTERVAL MONTH)],"
                        + " isAppend=[true])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                        + " person]}], alias=[_], opt=[VERTEX])",
                after.explain().trim());
    }

    @Test
    public void g_V_as_a_select_expr_id_a_test() {
        // the condition is fused into source and identified as primary key filtering
        RelNode node = eval("g.V().as('a').where(expr(elementId(a) = 2))");
        Assert.assertEquals(
                "GraphLogicalProject(a=[a], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[a], opt=[VERTEX], uniqueKeyFilters=[=(_.~id, 2)])",
                node.explain().trim());
    }

    @Test
    public void g_V_match_as_a_out_as_b_count_test() {
        RelNode node = eval("g.V().match(as(\"a\").out().as(\"b\")).count()");
        Assert.assertEquals(
                "GraphLogicalAggregate(keys=[{variables=[], aliases=[]}], values=[[{operands=[a,"
                    + " b], aggFunction=COUNT, alias='$f0', distinct=false}]])\n"
                    + "  GraphLogicalSingleMatch(input=[null],"
                    + " sentence=[GraphLogicalGetV(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[b], opt=[END])\n"
                    + "  GraphLogicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[_], opt=[OUT])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[a], opt=[VERTEX])\n"
                    + "], matchOpt=[INNER])",
                node.explain().trim());
    }

    // the filter conditions `has('age', 10)` and `has("name", "male")` should be composed and fused
    // into the person 'b'
    @Test
    public void g_V_match_as_a_out_as_b_hasLabel_has_out_as_c_count_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode node =
                eval(
                        "g.V().match(__.as(\"a\").out(\"knows\").as(\"b\").has('age', 10),"
                                + " __.as(\"b\").hasLabel(\"person\").has(\"name\","
                                + " \"male\").out(\"knows\").as(\"c\")).count()",
                        builder);
        RelNode after = optimizer.optimize(node, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalAggregate(keys=[{variables=[], aliases=[]}], values=[[{operands=[a, b,"
                        + " c], aggFunction=COUNT, alias='$f0', distinct=false}]])\n"
                        + "  GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[knows]}],"
                        + " alias=[a], startAlias=[b], opt=[IN], physicalOpt=[VERTEX])\n"
                        + "    GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[knows]}],"
                        + " alias=[c], startAlias=[b], opt=[OUT], physicalOpt=[VERTEX])\n"
                        + "      GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[b], fusedFilter=[[AND(=(_.name, _UTF-8'male'), =(_.age, 10))]],"
                        + " opt=[VERTEX])",
                after.explain().trim());
    }

    // id is the primary key of label 'person', should be fused as 'uniqueKeyFilters'
    @Test
    public void g_V_has_label_person_id_test() {
        RelNode node = eval("g.V().has(\"person\", \"id\", 1)");
        Assert.assertEquals(
                "GraphLogicalProject($f0=[_], isAppend=[false])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[_], opt=[VERTEX], uniqueKeyFilters=[=(_.id, 1)])",
                node.explain().trim());
    }

    // id is not the primary key of label 'software', should be fused as 'fusedFilter'
    @Test
    public void g_V_has_label_software_id_test() {
        RelNode node = eval("g.V().has(\"software\", \"id\", 1)");
        Assert.assertEquals(
                "GraphLogicalProject($f0=[_], isAppend=[false])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[software]}],"
                        + " alias=[_], fusedFilter=[[=(_.id, 1)]], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_path_expand_until_age_gt_30_values_age() {
        RelNode node =
                eval("g.V().out('1..3').with('UNTIL', expr(_.age > 30)).endV().values('age')");
        Assert.assertEquals(
                "GraphLogicalProject(age=[age], isAppend=[false])\n"
                    + "  GraphLogicalProject(age=[_.age], isAppend=[true])\n"
                    + "    GraphLogicalGetV(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[_], opt=[END])\n"
                    + "     "
                    + " GraphLogicalPathExpand(expand=[GraphLogicalExpand(tableConfig=[{isAll=true,"
                    + " tables=[created, knows]}], alias=[_], opt=[OUT])\n"
                    + "], getV=[GraphLogicalGetV(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[_], opt=[END])\n"
                    + "], offset=[1], fetch=[2], path_opt=[ARBITRARY], result_opt=[END_V],"
                    + " until_condition=[>(_.age, 30)], alias=[_])\n"
                    + "        GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[_], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_has_label_person_limit_10_as_a() {
        RelNode node = eval("g.V().hasLabel('person').limit(10).as('a')");
        Assert.assertEquals(
                "GraphLogicalProject($f0=[_], isAppend=[false])\n"
                        + "  GraphLogicalSort(fetch=[10])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[a], opt=[VERTEX])",
                node.explain().trim());
    }

    @Test
    public void g_V_select_a_b_valueMap() {
        RelNode rel =
                eval(
                        "g.V().hasLabel('person').as('a').out('knows').as('b').select('a',"
                                + " 'b').by(valueMap())");
        Assert.assertEquals(
                "({_UTF-8'a'=<CHAR(1), ({_UTF-8'name'=<CHAR(4), CHAR(1)>, _UTF-8'id'=<CHAR(2),"
                        + " BIGINT>, _UTF-8'age'=<CHAR(3), INTEGER>}) MAP>, _UTF-8'b'=<CHAR(1),"
                        + " ({_UTF-8'name'=<CHAR(4), CHAR(1)>, _UTF-8'id'=<CHAR(2), BIGINT>,"
                        + " _UTF-8'creationDate'=<CHAR(12), DATE>, _UTF-8'age'=<CHAR(3), INTEGER>,"
                        + " _UTF-8'lang'=<CHAR(4), CHAR(1)>}) MAP>}) MAP",
                rel.getRowType().getFieldList().get(0).getType().toString());
    }

    @Test
    public void g_V_select_expr_property_id() {
        RelNode rel = eval("g.V().select(expr(_.id))");
        Assert.assertEquals(
                "GraphLogicalProject(id=[id], isAppend=[false])\n"
                        + "  GraphLogicalProject(id=[_.id], isAppend=[true])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                        + " person]}], alias=[_], opt=[VERTEX])",
                rel.explain().trim());
    }

    @Test
    public void g_V_path_as_a_select_a_valueMap() {
        RelNode rel =
                eval(
                        "g.V().out('2..3').with('RESULT_OPT',"
                                + " 'ALL_V_E').as('a').select('a').valueMap('~len')");
        Assert.assertEquals(
                "GraphLogicalProject($f0=[$f0], isAppend=[false])\n"
                    + "  GraphLogicalProject($f0=[MAP(_UTF-8'~len', a.~len)], isAppend=[true])\n"
                    + "    GraphLogicalPathExpand(expand=[GraphLogicalExpand(tableConfig=[{isAll=true,"
                    + " tables=[created, knows]}], alias=[_], opt=[OUT])\n"
                    + "], getV=[GraphLogicalGetV(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[_], opt=[END])\n"
                    + "], offset=[2], fetch=[1], path_opt=[ARBITRARY], result_opt=[ALL_V_E],"
                    + " alias=[a])\n"
                    + "      GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[_], opt=[VERTEX])",
                rel.explain().trim());

        rel =
                eval(
                        "g.V().out('2..3').with('RESULT_OPT',"
                                + " 'ALL_V_E').as('a').select('a').valueMap('name', 'weight')");
        Assert.assertEquals(
                "GraphLogicalProject($f0=[$f0], isAppend=[false])\n"
                    + "  GraphLogicalProject($f0=[PATH_FUNCTION(a, FLAG(VERTEX_EDGE),"
                    + " MAP(_UTF-8'weight', a.weight, _UTF-8'name', a.name))], isAppend=[true])\n"
                    + "    GraphLogicalPathExpand(expand=[GraphLogicalExpand(tableConfig=[{isAll=true,"
                    + " tables=[created, knows]}], alias=[_], opt=[OUT])\n"
                    + "], getV=[GraphLogicalGetV(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[_], opt=[END])\n"
                    + "], offset=[2], fetch=[1], path_opt=[ARBITRARY], result_opt=[ALL_V_E],"
                    + " alias=[a])\n"
                    + "      GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[_], opt=[VERTEX])",
                rel.explain().trim());
    }

    @Test
    public void g_V_match_as_a_person_both_as_b_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode node1 =
                eval("g.V().match(as('a').hasLabel('person').both().as('b')).count()", builder);
        RelNode after1 = optimizer.optimize(node1, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalAggregate(keys=[{variables=[], aliases=[]}], values=[[{operands=[a,"
                        + " b], aggFunction=COUNT, alias='$f0', distinct=false}]])\n"
                        + "  GraphPhysicalExpand(tableConfig=[[EdgeLabel(knows, person, person),"
                        + " EdgeLabel(created, person, software)]], alias=[b], startAlias=[a],"
                        + " opt=[BOTH], physicalOpt=[VERTEX])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[a], opt=[VERTEX])",
                after1.explain().trim());
        RelNode node2 =
                eval("g.V().match(as('a').hasLabel('software').both().as('b')).count()", builder);
        RelNode after2 = optimizer.optimize(node2, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalAggregate(keys=[{variables=[], aliases=[]}], values=[[{operands=[a,"
                    + " b], aggFunction=COUNT, alias='$f0', distinct=false}]])\n"
                    + "  GraphPhysicalGetV(tableConfig=[{isAll=false, tables=[person]}], alias=[b],"
                    + " opt=[OTHER], physicalOpt=[ITSELF])\n"
                    + "    GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[created]}],"
                    + " alias=[_], startAlias=[a], opt=[BOTH], physicalOpt=[VERTEX])\n"
                    + "      GraphLogicalSource(tableConfig=[{isAll=false, tables=[software]}],"
                    + " alias=[a], opt=[VERTEX])",
                after2.explain().trim());
        RelNode node3 = eval("g.V().match(as('a').both().as('b')).count()", builder);
        RelNode after3 = optimizer.optimize(node3, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalAggregate(keys=[{variables=[], aliases=[]}], values=[[{operands=[a,"
                        + " b], aggFunction=COUNT, alias='$f0', distinct=false}]])\n"
                        + "  GraphPhysicalExpand(tableConfig=[[EdgeLabel(knows, person, person),"
                        + " EdgeLabel(created, person, software)]], alias=[b], startAlias=[a],"
                        + " opt=[BOTH], physicalOpt=[VERTEX])\n"
                        + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[software,"
                        + " person]}], alias=[a], opt=[VERTEX])",
                after3.explain().trim());
    }
}

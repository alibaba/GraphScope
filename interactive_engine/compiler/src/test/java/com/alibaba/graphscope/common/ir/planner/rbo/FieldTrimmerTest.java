package com.alibaba.graphscope.common.ir.planner.rbo;

import com.alibaba.graphscope.common.ir.Utils;
import com.alibaba.graphscope.common.ir.planner.GraphFieldTrimmer;
import com.alibaba.graphscope.common.ir.rel.type.group.GraphAggCall;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphStdOperatorTable;
import com.alibaba.graphscope.common.ir.tools.config.*;

import org.apache.calcite.rel.RelNode;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class FieldTrimmerTest {
    private String rowTypeString(RelNode rel) {
        StringBuilder builder = new StringBuilder();
        rowTypeStringInternal(rel, builder);
        return builder.toString();
    }

    private void rowTypeStringInternal(RelNode rel, StringBuilder builder) {
        builder.append(rel.getRowType()).append("\n");
        for (RelNode node : rel.getInputs()) {
            rowTypeStringInternal(node, builder);
        }
    }

    // Match(x:person)-[y:knows]->( ) where x.name = "Alice" return x.name as name , x.age as age
    @Test
    public void field_trimmer_1_test() {
        GraphBuilder builder = Utils.mockGraphBuilder();
        RelNode sentence =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person"),
                                        "x"))
                        .expand(
                                new ExpandConfig(
                                        GraphOpt.Expand.OUT,
                                        new LabelConfig(false).addLabel("knows"),
                                        "y"))
                        .build();
        RelNode project =
                builder.match(sentence, GraphOpt.Match.INNER)
                        .filter(
                                builder.call(
                                        GraphStdOperatorTable.EQUALS,
                                        builder.variable("x", "name"),
                                        builder.literal("Alice")))
                        .project(
                                List.of(
                                        builder.variable("x", "name"),
                                        builder.variable("x", "age")),
                                List.of("name", "age"))
                        .build();

        GraphFieldTrimmer trimmer = new GraphFieldTrimmer(builder);
        RelNode after = trimmer.trim(project);
        Assert.assertEquals(
                "GraphLogicalProject(name=[x.name], age=[x.age], isAppend=[false])\n"
                    + "  LogicalFilter(condition=[=(x.name, _UTF-8'Alice')])\n"
                    + "    GraphLogicalSingleMatch(input=[null],"
                    + " sentence=[GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}],"
                    + " alias=[y], opt=[OUT])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[x], opt=[VERTEX])\n"
                    + "], matchOpt=[INNER])",
                after.explain().trim());

        Assert.assertEquals(
                "RecordType(CHAR(1) name, INTEGER age)\n"
                    + "RecordType(Graph_Schema_Type(labels=[VertexLabel(person)],"
                    + " properties=[CHAR(1) name, INTEGER age]) x,"
                    + " Graph_Schema_Type(labels=[EdgeLabel(knows, person, person)], properties=[])"
                    + " y)\n"
                    + "RecordType(Graph_Schema_Type(labels=[VertexLabel(person)],"
                    + " properties=[CHAR(1) name, INTEGER age]) x,"
                    + " Graph_Schema_Type(labels=[EdgeLabel(knows, person, person)], properties=[])"
                    + " y)\n",
                rowTypeString(after));
    }

    // Match(x:person)-[y:knows]->(v:person) with x as p, x.age as age, v as friend where p.name =
    // "Alice" and age > 10
    // return p
    @Test
    public void field_trimmer_2_test() {
        GraphBuilder builder = Utils.mockGraphBuilder();
        RelNode sentence =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person"),
                                        "x"))
                        .expand(
                                new ExpandConfig(
                                        GraphOpt.Expand.OUT,
                                        new LabelConfig(false).addLabel("knows"),
                                        "y"))
                        .getV(
                                new GetVConfig(
                                        GraphOpt.GetV.END,
                                        new LabelConfig(false).addLabel("person"),
                                        "v"))
                        .build();
        RelNode project =
                builder.match(sentence, GraphOpt.Match.INNER)
                        .project(
                                List.of(
                                        builder.variable("x"),
                                        builder.variable("x", "age"),
                                        builder.variable("v")),
                                List.of("p", "age", "friend"))
                        .filter(
                                builder.call(
                                        GraphStdOperatorTable.AND,
                                        builder.call(
                                                GraphStdOperatorTable.EQUALS,
                                                builder.variable("p", "name"),
                                                builder.literal("Alice")),
                                        builder.call(
                                                GraphStdOperatorTable.GREATER_THAN,
                                                builder.variable("age"),
                                                builder.literal("10"))))
                        .project(List.of(builder.variable("p")))
                        .build();
        GraphFieldTrimmer trimmer = new GraphFieldTrimmer(builder);
        RelNode after = trimmer.trim(project);

        // for column trimming
        Assert.assertEquals(
                "GraphLogicalProject(p=[p], isAppend=[false])\n"
                    + "  LogicalFilter(condition=[AND(=(p.name, _UTF-8'Alice'), >(age,"
                    + " _UTF-8'10'))])\n"
                    + "    GraphLogicalProject(p=[x], age=[x.age], isAppend=[false])\n"
                    + "      GraphLogicalSingleMatch(input=[null],"
                    + " sentence=[GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[v], opt=[END])\n"
                    + "  GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}], alias=[y],"
                    + " opt=[OUT])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[x], opt=[VERTEX])\n"
                    + "], matchOpt=[INNER])",
                after.explain().trim());
        Assert.assertEquals(
                "RecordType(Graph_Schema_Type(labels=[VertexLabel(person)], properties=[]) p)\n"
                    + "RecordType(Graph_Schema_Type(labels=[VertexLabel(person)],"
                    + " properties=[CHAR(1) name]) p, INTEGER age)\n"
                    + "RecordType(Graph_Schema_Type(labels=[VertexLabel(person)],"
                    + " properties=[CHAR(1) name]) p, INTEGER age)\n"
                    + "RecordType(Graph_Schema_Type(labels=[VertexLabel(person)],"
                    + " properties=[CHAR(1) name, INTEGER age]) x,"
                    + " Graph_Schema_Type(labels=[EdgeLabel(knows, person, person)], properties=[])"
                    + " y, Graph_Schema_Type(labels=[VertexLabel(person)], properties=[]) v)\n",
                rowTypeString(after));
        // for property trimming

    }

    // Match(x:person)-[y:knows]->(friend:person) where x.age >10 return x, count(friend) as cnt
    // order
    // by cnt limit 3
    @Test
    public void field_trimmer_3_test() {
        GraphBuilder builder = Utils.mockGraphBuilder();
        RelNode sentence =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person"),
                                        "x"))
                        .expand(
                                new ExpandConfig(
                                        GraphOpt.Expand.OUT,
                                        new LabelConfig(false).addLabel("knows"),
                                        "y"))
                        .getV(
                                new GetVConfig(
                                        GraphOpt.GetV.END,
                                        new LabelConfig(false).addLabel("person"),
                                        "friend"))
                        .build();
        RelNode sort =
                builder.match(sentence, GraphOpt.Match.INNER)
                        .filter(
                                builder.call(
                                        GraphStdOperatorTable.GREATER_THAN,
                                        builder.variable("x", "age"),
                                        builder.literal(10)))
                        .aggregate(
                                builder.groupKey(builder.variable("x")),
                                List.of(
                                        new GraphAggCall(
                                                        builder.getCluster(),
                                                        GraphStdOperatorTable.COUNT,
                                                        List.of(builder.variable("friend")))
                                                .as("cnt")))
                        .sortLimit(
                                builder.literal(0),
                                builder.literal(3),
                                List.of(builder.variable("cnt")))
                        .build();
        GraphFieldTrimmer trimmer = new GraphFieldTrimmer(builder);
        RelNode after = trimmer.trim(sort);

        // for column trimming
        Assert.assertEquals(
                "GraphLogicalSort(sort0=[cnt], dir0=[ASC], offset=[0], fetch=[3])\n"
                    + "  GraphLogicalAggregate(keys=[{variables=[x], aliases=[x]}],"
                    + " values=[[{operands=[friend], aggFunction=COUNT, alias='cnt',"
                    + " distinct=false}]])\n"
                    + "    LogicalFilter(condition=[>(x.age, 10)])\n"
                    + "      GraphLogicalSingleMatch(input=[null],"
                    + " sentence=[GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[friend], opt=[END])\n"
                    + "  GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}], alias=[y],"
                    + " opt=[OUT])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[x], opt=[VERTEX])\n"
                    + "], matchOpt=[INNER])",
                after.explain().trim());
        Assert.assertEquals(
                "RecordType(Graph_Schema_Type(labels=[VertexLabel(person)], properties=[]) x,"
                    + " BIGINT cnt)\n"
                    + "RecordType(Graph_Schema_Type(labels=[VertexLabel(person)], properties=[]) x,"
                    + " BIGINT cnt)\n"
                    + "RecordType(Graph_Schema_Type(labels=[VertexLabel(person)],"
                    + " properties=[INTEGER age]) x, Graph_Schema_Type(labels=[EdgeLabel(knows,"
                    + " person, person)], properties=[]) y,"
                    + " Graph_Schema_Type(labels=[VertexLabel(person)], properties=[]) friend)\n"
                    + "RecordType(Graph_Schema_Type(labels=[VertexLabel(person)],"
                    + " properties=[INTEGER age]) x, Graph_Schema_Type(labels=[EdgeLabel(knows,"
                    + " person, person)], properties=[]) y,"
                    + " Graph_Schema_Type(labels=[VertexLabel(person)], properties=[]) friend)\n",
                rowTypeString(after));
    }

    // Match(x:person)-[y:knows]->( ) with name=x.name where name = "Alice" return x.id
    // test append=true
    @Test
    public void field_trimmer_4_test() {
        GraphBuilder builder = Utils.mockGraphBuilder();
        RelNode sentence =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person"),
                                        "x"))
                        .expand(
                                new ExpandConfig(
                                        GraphOpt.Expand.OUT,
                                        new LabelConfig(false).addLabel("knows"),
                                        "y"))
                        .build();
        RelNode project =
                builder.match(sentence, GraphOpt.Match.INNER)
                        .project(List.of(builder.variable("x", "name")), List.of("name"), true)
                        .filter(
                                builder.call(
                                        GraphStdOperatorTable.EQUALS,
                                        builder.variable("name"),
                                        builder.literal("Alice")))
                        .project(List.of(builder.variable("x", "id")), List.of("id"))
                        .build();

        GraphFieldTrimmer trimmer = new GraphFieldTrimmer(builder);
        RelNode after = trimmer.trim(project);
        Assert.assertEquals(
                "GraphLogicalProject(id=[x.id], isAppend=[false])\n"
                    + "  LogicalFilter(condition=[=(name, _UTF-8'Alice')])\n"
                    + "    GraphLogicalProject(x=[x], name=[x.name], isAppend=[false])\n"
                    + "      GraphLogicalSingleMatch(input=[null],"
                    + " sentence=[GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}],"
                    + " alias=[y], opt=[OUT])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[x], opt=[VERTEX])\n"
                    + "], matchOpt=[INNER])",
                after.explain().trim());

        Assert.assertEquals(
                "RecordType(BIGINT id)\n"
                    + "RecordType(Graph_Schema_Type(labels=[VertexLabel(person)],"
                    + " properties=[BIGINT id]) x, CHAR(1) name)\n"
                    + "RecordType(Graph_Schema_Type(labels=[VertexLabel(person)],"
                    + " properties=[BIGINT id]) x, CHAR(1) name)\n"
                    + "RecordType(Graph_Schema_Type(labels=[VertexLabel(person)],"
                    + " properties=[BIGINT id, CHAR(1) name]) x,"
                    + " Graph_Schema_Type(labels=[EdgeLabel(knows, person, person)], properties=[])"
                    + " y)\n",
                rowTypeString(after));
    }
}

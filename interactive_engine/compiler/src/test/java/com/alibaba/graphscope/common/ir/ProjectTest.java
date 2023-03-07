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

package com.alibaba.graphscope.common.ir;

import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphStdOperatorTable;
import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;
import com.alibaba.graphscope.common.ir.tools.config.LabelConfig;
import com.alibaba.graphscope.common.ir.tools.config.SourceConfig;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexNode;
import org.junit.Assert;
import org.junit.Test;

public class ProjectTest {
    // project("a") -> expr: "a", alias: "a"
    @Test
    public void project_1_test() {
        GraphBuilder builder = IrUtils.mockGraphBuilder();
        RexNode variable =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person"),
                                        "a"))
                        .variable("a");
        Project project = (Project) builder.project(ImmutableList.of(variable)).build();
        Assert.assertEquals("[a]", project.getProjects().toString());
        Assert.assertEquals(
                "RecordType(Graph_Schema_Type(BIGINT id, CHAR(1) name, INTEGER age) a)",
                project.getRowType().toString());
    }

    // project("a.age") -> expr: "a.age", alias: "age"
    @Test
    public void project_2_test() {
        GraphBuilder builder = IrUtils.mockGraphBuilder();
        RexNode variable =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person"),
                                        "a"))
                        .variable("a", "age");
        Project project = (Project) builder.project(ImmutableList.of(variable)).build();
        Assert.assertEquals("[a.age]", project.getProjects().toString());
        Assert.assertEquals("RecordType(INTEGER age)", project.getRowType().toString());
    }

    // project("a.age+1") -> expr: "a.age+1", alias: "$f0"
    @Test
    public void project_3_test() {
        GraphBuilder builder = IrUtils.mockGraphBuilder();
        RexNode plus =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person"),
                                        "a"))
                        .call(
                                GraphStdOperatorTable.PLUS,
                                builder.variable("a", "age"),
                                builder.literal(1));
        Project project = (Project) builder.project(ImmutableList.of(plus)).build();
        Assert.assertEquals("[+(a.age, 1)]", project.getProjects().toString());
        Assert.assertEquals("RecordType(INTEGER $f0)", project.getRowType().toString());
    }

    // project("a.age+1", "b") -> expr: "a.age+1", alias: "b"
    @Test
    public void project_4_test() {
        GraphBuilder builder = IrUtils.mockGraphBuilder();
        RexNode plus =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person"),
                                        "a"))
                        .call(
                                GraphStdOperatorTable.PLUS,
                                builder.variable("a", "age"),
                                builder.literal(1));
        Project project =
                (Project)
                        builder.project(ImmutableList.of(plus), ImmutableList.of("b"), false)
                                .build();
        Assert.assertEquals("[+(a.age, 1)]", project.getProjects().toString());
        Assert.assertEquals("RecordType(INTEGER b)", project.getRowType().toString());
    }

    // project is true
    @Test
    public void project_5_test() {
        GraphBuilder builder = IrUtils.mockGraphBuilder();
        RexNode variable =
                builder.source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person"),
                                        "a"))
                        .project(
                                ImmutableList.of(
                                        builder.call(
                                                GraphStdOperatorTable.PLUS,
                                                builder.variable("a", "age"),
                                                builder.literal(1))),
                                ImmutableList.of("b"),
                                true)
                        .variable(
                                "a",
                                "name"); // can refer to the alias before the project if append is
        // true
        Assert.assertEquals("a.name", variable.toString());
        // only contain the new appended columns in project data type
        Assert.assertEquals("RecordType(INTEGER b)", builder.build().getRowType().toString());
    }
}

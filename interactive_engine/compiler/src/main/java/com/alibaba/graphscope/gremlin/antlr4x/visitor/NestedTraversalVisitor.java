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

package com.alibaba.graphscope.gremlin.antlr4x.visitor;

import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.grammar.GremlinGSBaseVisitor;
import com.alibaba.graphscope.grammar.GremlinGSParser;
import com.alibaba.graphscope.gremlin.exception.UnsupportedEvalException;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

public class NestedTraversalVisitor extends GremlinGSBaseVisitor<RexNode> {
    private final GraphBuilder parentBuilder;
    private final GraphBuilder nestedBuilder;
    private final @Nullable String tag;

    public NestedTraversalVisitor(GraphBuilder parentBuilder, @Nullable String tag) {
        this.parentBuilder = parentBuilder;
        this.nestedBuilder =
                GraphBuilder.create(
                        this.parentBuilder.getContext(),
                        (GraphOptCluster) this.parentBuilder.getCluster(),
                        this.parentBuilder.getRelOptSchema());
        this.tag = tag;
    }

    @Override
    public RexNode visitNestedTraversal(GremlinGSParser.NestedTraversalContext ctx) {
        Preconditions.checkArgument(parentBuilder.size() > 0, "parent builder should not be empty");
        RelNode commonRel = parentBuilder.peek();
        nestedBuilder.push(commonRel);
        if (tag != null) {
            nestedBuilder.project(
                    ImmutableList.of(nestedBuilder.variable(tag)), ImmutableList.of(), true);
        }
        GraphBuilderVisitor visitor = new GraphBuilderVisitor(nestedBuilder);
        RelNode subRel = visitor.visitNestedTraversal(ctx).build();
        if (new SubQueryChecker(commonRel).test(subRel)) {
            // todo: return sub query in RexSubQuery
            throw new UnsupportedEvalException(
                    GremlinGSParser.NestedTraversalContext.class, "sub query is unsupported yet");
        }
        // convert subRel to expression
        Project project = (Project) subRel;
        Preconditions.checkArgument(
                project.getProjects().size() == 1,
                "value returned from sub query should be single value");
        RexNode expr = project.getProjects().get(0);
        String alias = project.getRowType().getFieldList().get(0).getName();
        return nestedBuilder.alias(expr, alias);
    }
}

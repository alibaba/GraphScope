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

import com.alibaba.graphscope.common.ir.meta.schema.CommonOptTable;
import com.alibaba.graphscope.common.ir.rel.CommonTableScan;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.grammar.GremlinGSBaseVisitor;
import com.alibaba.graphscope.grammar.GremlinGSParser;
import com.alibaba.graphscope.gremlin.antlr4.GenericLiteralVisitor;
import com.alibaba.graphscope.gremlin.exception.UnsupportedEvalException;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.antlr.v4.runtime.ParserRuleContext;
import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

public class NestedTraversalRexVisitor extends GremlinGSBaseVisitor<RexNode> {
    private final GraphBuilder parentBuilder;
    private final GraphBuilder nestedBuilder;
    private final @Nullable String tag;
    private final ParserRuleContext parentCtx;

    public NestedTraversalRexVisitor(
            GraphBuilder parentBuilder, @Nullable String tag, ParserRuleContext parentCtx) {
        this.parentBuilder = parentBuilder;
        this.nestedBuilder =
                GraphBuilder.create(
                        this.parentBuilder.getContext(),
                        (GraphOptCluster) this.parentBuilder.getCluster(),
                        this.parentBuilder.getRelOptSchema());
        this.tag = tag;
        this.parentCtx = parentCtx;
    }

    @Override
    public RexNode visitNestedTraversal(GremlinGSParser.NestedTraversalContext ctx) {
        RelNode commonRel =
                Objects.requireNonNull(
                        parentBuilder.peek(), "parentCtx builder should not be empty");
        commonRel =
                new CommonTableScan(
                        commonRel.getCluster(),
                        commonRel.getTraitSet(),
                        new CommonOptTable(commonRel));
        nestedBuilder.push(commonRel);
        if (tag != null) {
            nestedBuilder.project(
                    ImmutableList.of(nestedBuilder.variable(tag)), ImmutableList.of(), true);
        }
        GraphBuilderVisitor visitor = new GraphBuilderVisitor(nestedBuilder);
        RelNode subRel = visitor.visitNestedTraversal(ctx).build();
        String alias = null;
        // set query given alias
        TraversalMethodIterator iterator = new TraversalMethodIterator(ctx);
        while (iterator.hasNext()) {
            GremlinGSParser.TraversalMethodContext cur = iterator.next();
            if (cur.traversalMethod_as() != null) {
                alias =
                        GenericLiteralVisitor.getStringLiteral(
                                cur.traversalMethod_as().stringLiteral());
            }
        }
        if (alias != null) {
            subRel = nestedBuilder.push(subRel).as(null).build();
        }
        RexNode expr;
        if (new SubQueryChecker(commonRel).test(subRel)) {
            // todo: return sub query in RexSubQuery
            throw new UnsupportedEvalException(
                    GremlinGSParser.NestedTraversalContext.class, "sub query is unsupported yet");
        } else {
            // convert subRel to expression
            Project project = (Project) subRel;
            Preconditions.checkArgument(
                    project.getProjects().size() == 1,
                    "value returned from sub query should be single value");
            expr = project.getProjects().get(0);
            if (parentCtx instanceof GremlinGSParser.TraversalMethod_whereContext) {
                // convert to IS_NOT_NULL
                expr = nestedBuilder.isNotNull(expr);
            } else if (parentCtx instanceof GremlinGSParser.TraversalMethod_notContext) {
                // convert to IS_NULL
                expr = nestedBuilder.isNull(expr);
            }
        }
        return nestedBuilder.alias(expr, alias);
    }
}

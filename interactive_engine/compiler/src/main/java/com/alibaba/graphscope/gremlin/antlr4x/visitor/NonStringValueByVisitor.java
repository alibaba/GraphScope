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

import com.alibaba.graphscope.common.ir.rel.GraphLogicalDedupBy;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.grammar.GremlinGSBaseVisitor;
import com.alibaba.graphscope.grammar.GremlinGSParser;
import com.alibaba.graphscope.gremlin.exception.UnsupportedEvalException;
import com.google.common.base.Preconditions;

import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;

public class NonStringValueByVisitor extends GremlinGSBaseVisitor<RelBuilder.AggCall> {
    private final GraphBuilder builder;

    public NonStringValueByVisitor(GraphBuilder builder) {
        this.builder = builder;
    }

    @Override
    public RelBuilder.AggCall visitNonStringValueBy(GremlinGSParser.NonStringValueByContext byCtx) {
        String alias =
                (byCtx.traversalMethod_as() != null)
                        ? (String)
                                LiteralVisitor.INSTANCE.visit(
                                        byCtx.traversalMethod_as().StringLiteral())
                        : null;
        if (byCtx.traversalMethod_dedup() != null) {
            GraphBuilder nestedBuilder =
                    GraphBuilder.create(
                            builder.getContext(),
                            (GraphOptCluster) builder.getCluster(),
                            builder.getRelOptSchema());
            Preconditions.checkArgument(builder.size() > 0, "parent builder should not be empty");
            GraphBuilderVisitor nestedVisitor =
                    new GraphBuilderVisitor(nestedBuilder.push(this.builder.peek()));
            GraphLogicalDedupBy dedupBy =
                    (GraphLogicalDedupBy)
                            nestedVisitor
                                    .visitTraversalMethod_dedup(byCtx.traversalMethod_dedup())
                                    .build();
            if (byCtx.oC_AggregateFunctionInvocation() != null) {
                String functionName = byCtx.oC_AggregateFunctionInvocation().getChild(0).getText();
                if (functionName.equals("count")) {
                    return builder.count(true, alias, dedupBy.getDedupByKeys());
                } else if (functionName.equals("fold")) {
                    return builder.collect(true, alias, dedupBy.getDedupByKeys());
                }
            }
            throw new UnsupportedEvalException(
                    byCtx.getClass(), byCtx.getText() + " is unsupported yet in group value by");
        } else if (byCtx.oC_AggregateFunctionInvocation() != null) {
            RexNode expr;
            if (byCtx.traversalMethod_select() != null || byCtx.traversalMethod_values() != null) {
                GraphBuilder nestedBuilder =
                        GraphBuilder.create(
                                builder.getContext(),
                                (GraphOptCluster) builder.getCluster(),
                                builder.getRelOptSchema());
                Preconditions.checkArgument(
                        builder.size() > 0, "parent builder should not be empty");
                GraphBuilderVisitor nestedVisitor =
                        new GraphBuilderVisitor(nestedBuilder.push(this.builder.peek()));
                if (byCtx.traversalMethod_select() != null) {
                    nestedVisitor.visitTraversalMethod_select(byCtx.traversalMethod_select());
                }
                if (byCtx.traversalMethod_values() != null) {
                    nestedVisitor.visitTraversalMethod_values(byCtx.traversalMethod_values());
                }
                RelNode subRel = nestedBuilder.build();
                if (new SubQueryChecker(builder.peek()).test(subRel)) {
                    throw new UnsupportedEvalException(
                            byCtx.getClass(),
                            "sub query " + byCtx.getText() + "is unsupported yet");
                }
                // convert subRel to expression
                Project project = (Project) subRel;
                Preconditions.checkArgument(
                        project.getInputs().size() == 1,
                        "value returned from sub query should be single value");
                expr = project.getProjects().get(0);
            } else {
                expr = builder.variable((String) null);
            }
            String functionName = byCtx.oC_AggregateFunctionInvocation().getChild(0).getText();
            switch (functionName) {
                case "count":
                    return builder.count(false, alias, expr);
                case "sum":
                    return builder.sum(false, alias, expr);
                case "min":
                    return builder.min(alias, expr);
                case "max":
                    return builder.max(alias, expr);
                case "mean":
                    return builder.avg(false, alias, expr);
                case "fold":
                    return builder.collect(false, alias, expr);
                default:
                    throw new IllegalArgumentException(
                            "invalid aggregate function "
                                    + byCtx.oC_AggregateFunctionInvocation().getText());
            }
        } else {
            throw new UnsupportedEvalException(
                    byCtx.getClass(),
                    "group value by context " + byCtx.getText() + " is unsupported yet");
        }
    }
}

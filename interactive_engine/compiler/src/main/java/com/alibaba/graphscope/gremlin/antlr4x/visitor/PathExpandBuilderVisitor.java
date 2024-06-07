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

import com.alibaba.graphscope.common.antlr4.ExprVisitorResult;
import com.alibaba.graphscope.common.ir.tools.config.*;
import com.alibaba.graphscope.grammar.GremlinGSBaseVisitor;
import com.alibaba.graphscope.grammar.GremlinGSParser;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.List;

public class PathExpandBuilderVisitor extends GremlinGSBaseVisitor<PathExpandConfig.Builder> {
    private final PathExpandConfig.Builder builder;
    private final GraphBuilderVisitor parent;

    public PathExpandBuilderVisitor(GraphBuilderVisitor parent) {
        // PATH_OPT = ARBITRARY and RESULT_OPT = END_V are set by default
        this.builder = PathExpandConfig.newBuilder(parent.getGraphBuilder());
        this.parent = parent;
    }

    @Override
    public PathExpandConfig.Builder visitTraversalMethod_out(
            GremlinGSParser.TraversalMethod_outContext ctx) {
        List<String> labels =
                new LiteralList(ctx.oC_ListLiteral(), ctx.oC_Expression()).toList(String.class);
        Preconditions.checkArgument(!labels.isEmpty(), "hop range can not be empty in path expand");
        String[] ranges = labels.get(0).split("\\.\\.");
        int lower = Integer.valueOf(ranges[0]);
        int upper = Integer.valueOf(ranges[1]);
        // set path_opt and result_opt
        List<GremlinGSParser.TraversalMethod_withContext> nextWith =
                getNextWith((GremlinGSParser.TraversalMethodContext) ctx.getParent());
        nextWith.forEach(k -> visitTraversalMethod_with(k));
        // set expand config, getV config, range
        return builder.expand(new ExpandConfig(GraphOpt.Expand.OUT, getExpandLabelConfig(labels)))
                .getV(new GetVConfig(GraphOpt.GetV.END))
                .range(lower, upper - lower);
    }

    @Override
    public PathExpandConfig.Builder visitTraversalMethod_in(
            GremlinGSParser.TraversalMethod_inContext ctx) {
        List<String> labels =
                new LiteralList(ctx.oC_ListLiteral(), ctx.oC_Expression()).toList(String.class);
        Preconditions.checkArgument(!labels.isEmpty(), "hop range can not be empty in path expand");
        String[] ranges = labels.get(0).split("\\.\\.");
        int lower = Integer.valueOf(ranges[0]);
        int upper = Integer.valueOf(ranges[1]);
        // set path_opt and result_opt
        List<GremlinGSParser.TraversalMethod_withContext> nextWith =
                getNextWith((GremlinGSParser.TraversalMethodContext) ctx.getParent());
        nextWith.forEach(k -> visitTraversalMethod_with(k));
        // set expand config, getV config, range
        return builder.expand(new ExpandConfig(GraphOpt.Expand.IN, getExpandLabelConfig(labels)))
                .getV(new GetVConfig(GraphOpt.GetV.START))
                .range(lower, upper - lower);
    }

    @Override
    public PathExpandConfig.Builder visitTraversalMethod_both(
            GremlinGSParser.TraversalMethod_bothContext ctx) {
        List<String> labels =
                new LiteralList(ctx.oC_ListLiteral(), ctx.oC_Expression()).toList(String.class);
        Preconditions.checkArgument(!labels.isEmpty(), "hop range can not be empty in path expand");
        String[] ranges = labels.get(0).split("\\.\\.");
        int lower = Integer.valueOf(ranges[0]);
        int upper = Integer.valueOf(ranges[1]);
        // set path_opt and result_opt
        List<GremlinGSParser.TraversalMethod_withContext> nextWith =
                getNextWith((GremlinGSParser.TraversalMethodContext) ctx.getParent());
        nextWith.forEach(k -> visitTraversalMethod_with(k));
        // set expand config, getV config, range
        return builder.expand(new ExpandConfig(GraphOpt.Expand.BOTH, getExpandLabelConfig(labels)))
                .getV(new GetVConfig(GraphOpt.GetV.OTHER))
                .range(lower, upper - lower);
    }

    @Override
    public PathExpandConfig.Builder visitTraversalMethod_with(
            GremlinGSParser.TraversalMethod_withContext ctx) {
        String optKey = (String) LiteralVisitor.INSTANCE.visit(ctx.StringLiteral());
        switch (optKey.toUpperCase()) {
            case "PATH_OPT":
                Object pathValue = LiteralVisitor.INSTANCE.visit(ctx.oC_Literal());
                return builder.pathOpt(
                        GraphOpt.PathExpandPath.valueOf(String.valueOf(pathValue).toUpperCase()));
            case "RESULT_OPT":
                Object resultValue = LiteralVisitor.INSTANCE.visit(ctx.oC_Literal());
                return builder.resultOpt(
                        GraphOpt.PathExpandResult.valueOf(
                                String.valueOf(resultValue).toUpperCase()));
            case "UNTIL":
                ExprVisitorResult exprRes =
                        new ExtExpressionVisitor(builder, parent.getAliasInfer())
                                .visitTraversalMethod_expr(ctx.traversalMethod_expr());
                return builder.untilCondition(exprRes.getExpr());
            default:
                return builder;
        }
    }

    private List<GremlinGSParser.TraversalMethod_withContext> getNextWith(
            GremlinGSParser.TraversalMethodContext curCtx) {
        List<GremlinGSParser.TraversalMethod_withContext> nextWith = Lists.newArrayList();
        TraversalMethodIterator methodIterator = new TraversalMethodIterator(curCtx);
        while (methodIterator.hasNext()) {
            GremlinGSParser.TraversalMethodContext next = methodIterator.next();
            if (next.traversalMethod_with() != null) {
                nextWith.add(next.traversalMethod_with());
            } else {
                break;
            }
        }
        return nextWith;
    }

    private LabelConfig getExpandLabelConfig(List<String> parameters) {
        if (parameters.size() <= 1) {
            return new LabelConfig(true);
        } else {
            LabelConfig expandLabels = new LabelConfig(false);
            parameters.subList(1, parameters.size()).forEach(expandLabels::addLabel);
            return expandLabels;
        }
    }
}

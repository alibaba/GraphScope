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

package com.alibaba.graphscope.cypher.antlr4.visitor;

import com.alibaba.graphscope.common.ir.rel.type.group.GraphAggCall;
import com.alibaba.graphscope.common.ir.rex.RexTmpVariableConverter;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;
import com.alibaba.graphscope.cypher.antlr4.visitor.type.ExprVisitorResult;
import com.alibaba.graphscope.grammar.CypherGSBaseVisitor;
import com.alibaba.graphscope.grammar.CypherGSParser;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.tools.RelBuilder;

import java.util.*;
import java.util.stream.Collectors;

public class GraphBuilderVisitor extends CypherGSBaseVisitor<GraphBuilder> {
    private final GraphBuilder builder;
    private final Set<String> uniqueNameList;
    private final ExpressionVisitor expressionVisitor;

    public GraphBuilderVisitor(GraphBuilder builder) {
        this.builder = Objects.requireNonNull(builder);
        this.expressionVisitor = new ExpressionVisitor(this);
        this.uniqueNameList = new HashSet<>();
    }

    @Override
    public GraphBuilder visitOC_Cypher(CypherGSParser.OC_CypherContext ctx) {
        return visitOC_Statement(ctx.oC_Statement());
    }

    @Override
    public GraphBuilder visitOC_Match(CypherGSParser.OC_MatchContext ctx) {
        int childCnt = ctx.oC_Pattern().getChildCount();
        List<RelNode> sentences = new ArrayList<>();
        for (int i = 0; i < childCnt; ++i) {
            CypherGSParser.OC_PatternPartContext partCtx = ctx.oC_Pattern().oC_PatternPart(i);
            if (partCtx == null) continue;
            sentences.add(visitOC_PatternPart(partCtx).build());
        }
        if (sentences.size() == 1) {
            builder.match(sentences.get(0), GraphOpt.Match.INNER);
        } else if (sentences.size() > 1) {
            builder.match(sentences.get(0), sentences.subList(1, sentences.size()));
        } else {
            throw new IllegalArgumentException("sentences in match should not be empty");
        }
        return (ctx.oC_Where() != null) ? visitOC_Where(ctx.oC_Where()) : builder;
    }

    @Override
    public GraphBuilder visitOC_PatternElementChain(
            CypherGSParser.OC_PatternElementChainContext ctx) {
        CypherGSParser.OC_RelationshipPatternContext relationCtx = ctx.oC_RelationshipPattern();
        CypherGSParser.OC_RangeLiteralContext literalCtx =
                relationCtx.oC_RelationshipDetail().oC_RangeLiteral();
        // path_expand
        if (literalCtx != null && literalCtx.oC_IntegerLiteral().size() > 1) {
            builder.pathExpand(
                    new PathExpandBuilderVisitor(this).visitOC_PatternElementChain(ctx).build());
            // extract the end vertex from path_expand results
            if (ctx.oC_NodePattern() != null) {
                builder.getV(Utils.getVConfig(ctx.oC_NodePattern()));
                // fuse filter with endV
                return visitOC_Properties(ctx.oC_NodePattern().oC_Properties());
            } else {
                return builder;
            }
        } else { // expand + getV
            return super.visitOC_PatternElementChain(ctx);
        }
    }

    @Override
    public GraphBuilder visitOC_NodePattern(CypherGSParser.OC_NodePatternContext ctx) {
        // source
        if (ctx.parent instanceof CypherGSParser.OC_PatternElementContext) {
            builder.source(Utils.sourceConfig(ctx));
        } else { // getV
            builder.getV(Utils.getVConfig(ctx));
        }
        return visitOC_Properties(ctx.oC_Properties());
    }

    @Override
    public GraphBuilder visitOC_RelationshipPattern(
            CypherGSParser.OC_RelationshipPatternContext ctx) {
        builder.expand(Utils.expandConfig(ctx));
        return visitOC_Properties(ctx.oC_RelationshipDetail().oC_Properties());
    }

    @Override
    public GraphBuilder visitOC_Properties(CypherGSParser.OC_PropertiesContext ctx) {
        return (ctx == null)
                ? builder
                : builder.filter(Utils.propertyFilters(this.builder, this.expressionVisitor, ctx));
    }

    @Override
    public GraphBuilder visitOC_Where(CypherGSParser.OC_WhereContext ctx) {
        ExprVisitorResult res = expressionVisitor.visitOC_Expression(ctx.oC_Expression());
        if (!res.getAggCalls().isEmpty()) {
            throw new IllegalArgumentException(
                    "aggregate functions should not exist in filter expression");
        }
        return builder.filter(res.getExpr());
    }

    @Override
    public GraphBuilder visitOC_With(CypherGSParser.OC_WithContext ctx) {
        visitOC_ProjectionBody(ctx.oC_ProjectionBody());
        return (ctx.oC_Where() != null) ? visitOC_Where(ctx.oC_Where()) : builder;
    }

    @Override
    public GraphBuilder visitOC_ProjectionBody(CypherGSParser.OC_ProjectionBodyContext ctx) {
        boolean isDistinct = (ctx.DISTINCT() != null);
        List<RexNode> keyExprs = new ArrayList<>();
        List<String> keyAliases = new ArrayList<>();
        List<RelBuilder.AggCall> aggCalls = new ArrayList<>();
        List<RexNode> extraExprs = new ArrayList<>();
        List<String> extraAliases = new ArrayList<>();
        if (isGroupPattern(ctx, keyExprs, keyAliases, aggCalls, extraExprs, extraAliases)) {
            RelBuilder.GroupKey groupKey;
            List<String> newAliases = new ArrayList<>();
            if (keyExprs.isEmpty()) {
                groupKey = builder.groupKey();
            } else {
                if (!extraExprs.isEmpty()) {
                    for (int i = 0; i < keyExprs.size(); ++i) {
                        newAliases.add(inferAlias());
                    }
                    groupKey = builder.groupKey(keyExprs, newAliases);
                } else {
                    groupKey = builder.groupKey(keyExprs, keyAliases);
                }
            }
            builder.aggregate(groupKey, aggCalls);
            if (!extraExprs.isEmpty()) {
                RexTmpVariableConverter converter = new RexTmpVariableConverter(true, builder);
                extraExprs =
                        extraExprs.stream()
                                .map(k -> k.accept(converter))
                                .collect(Collectors.toList());
                for (int i = 0; i < newAliases.size(); ++i) {
                    extraExprs.add(i, builder.variable(newAliases.get(i)));
                    extraAliases.add(i, (i < keyAliases.size()) ? keyAliases.get(i) : null);
                }
                builder.project(extraExprs, extraAliases, false);
            }
        } else if (isDistinct) {
            builder.aggregate(builder.groupKey(keyExprs, keyAliases));
        } else {
            builder.project(keyExprs, keyAliases, false);
        }
        if (ctx.oC_Order() != null) {
            visitOC_Order(ctx.oC_Order());
        }
        if (ctx.oC_Limit() != null) {
            visitOC_Limit(ctx.oC_Limit());
        }
        return builder;
    }

    private boolean isGroupPattern(
            CypherGSParser.OC_ProjectionBodyContext ctx,
            List<RexNode> keyExprs,
            List<String> keyAliases,
            List<RelBuilder.AggCall> aggCalls,
            List<RexNode> extraExprs,
            List<String> extraAliases) {
        for (CypherGSParser.OC_ProjectionItemContext itemCtx :
                ctx.oC_ProjectionItems().oC_ProjectionItem()) {
            ExprVisitorResult item = expressionVisitor.visitOC_Expression(itemCtx.oC_Expression());
            String alias = (itemCtx.AS() == null) ? null : itemCtx.oC_Variable().getText();
            if (item.getAggCalls().isEmpty()) {
                keyExprs.add(item.getExpr());
                keyAliases.add(alias);
            } else {
                if (item.getExpr() instanceof RexCall) {
                    extraExprs.add(item.getExpr());
                    extraAliases.add(alias);
                    aggCalls.addAll(item.getAggCalls());
                } else if (item.getAggCalls().size() == 1) { // count(a.name)
                    GraphAggCall original = (GraphAggCall) item.getAggCalls().get(0);
                    aggCalls.add(
                            new GraphAggCall(
                                            original.getCluster(),
                                            original.getAggFunction(),
                                            original.getOperands())
                                    .as(alias)
                                    .distinct(original.isDistinct()));
                } else {
                    throw new IllegalArgumentException("invalid expr visit result");
                }
            }
        }
        return !aggCalls.isEmpty();
    }

    @Override
    public GraphBuilder visitOC_Order(CypherGSParser.OC_OrderContext ctx) {
        List<RexNode> orderBy = new ArrayList<>();
        for (CypherGSParser.OC_SortItemContext itemCtx : ctx.oC_SortItem()) {
            RexNode expr = expressionVisitor.visitOC_Expression(itemCtx.oC_Expression()).getExpr();
            if (itemCtx.DESC() != null || itemCtx.DESCENDING() != null) {
                expr = builder.desc(expr);
            }
            orderBy.add(expr);
        }
        return (GraphBuilder) builder.sort(orderBy);
    }

    @Override
    public GraphBuilder visitOC_Limit(CypherGSParser.OC_LimitContext ctx) {
        return (GraphBuilder)
                builder.limit(
                        0,
                        (Integer)
                                LiteralVisitor.INSTANCE.visitOC_IntegerLiteral(
                                        ctx.oC_IntegerLiteral()));
    }

    public GraphBuilder getGraphBuilder() {
        return builder;
    }

    public ExpressionVisitor getExpressionVisitor() {
        return expressionVisitor;
    }

    public String inferAlias() {
        String name;
        int j = 0;
        do {
            name = SqlValidatorUtil.EXPR_SUGGESTER.apply(null, j++, 0);
        } while (uniqueNameList.contains(name));
        uniqueNameList.add(name);
        return name;
    }
}

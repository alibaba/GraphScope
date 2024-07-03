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

import com.alibaba.graphscope.common.antlr4.ExprUniqueAliasInfer;
import com.alibaba.graphscope.common.antlr4.ExprVisitorResult;
import com.alibaba.graphscope.common.ir.rel.GraphLogicalProject;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalExpand;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalPathExpand;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalSource;
import com.alibaba.graphscope.common.ir.rel.type.group.GraphAggCall;
import com.alibaba.graphscope.common.ir.rex.RexTmpVariableConverter;
import com.alibaba.graphscope.common.ir.tools.AliasInference;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphStdOperatorTable;
import com.alibaba.graphscope.common.ir.tools.config.*;
import com.alibaba.graphscope.common.ir.type.GraphProperty;
import com.alibaba.graphscope.grammar.GremlinGSBaseVisitor;
import com.alibaba.graphscope.grammar.GremlinGSParser;
import com.alibaba.graphscope.gremlin.antlr4.TraversalEnumParser;
import com.alibaba.graphscope.gremlin.exception.InvalidGremlinScriptException;
import com.alibaba.graphscope.gremlin.exception.UnsupportedEvalException;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.RuleNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.T;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.javatuples.Pair;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class GraphBuilderVisitor extends GremlinGSBaseVisitor<GraphBuilder> {
    private final GraphBuilder builder;
    private final ExprUniqueAliasInfer aliasInfer;
    private final Predicate<ParseTree> trimAlias;

    public GraphBuilderVisitor(GraphBuilder builder) {
        this(builder, new ExprUniqueAliasInfer());
    }

    public GraphBuilderVisitor(GraphBuilder builder, ExprUniqueAliasInfer aliasInfer) {
        this(builder, aliasInfer, t -> false);
    }

    public GraphBuilderVisitor(GraphBuilder builder, Predicate<ParseTree> trimAlias) {
        this(builder, new ExprUniqueAliasInfer(), trimAlias);
    }

    public GraphBuilderVisitor(
            GraphBuilder builder, ExprUniqueAliasInfer aliasInfer, Predicate<ParseTree> trimAlias) {
        this.builder = Objects.requireNonNull(builder);
        this.aliasInfer = Objects.requireNonNull(aliasInfer);
        this.trimAlias = Objects.requireNonNull(trimAlias);
    }

    // re-implement the function in that we need to customize the behavior to visit each parse tree
    // node
    @Override
    public GraphBuilder visitChildren(RuleNode node) {
        GraphBuilder result = this.defaultResult();
        int n = node.getChildCount();

        for (int i = 0; i < n && this.shouldVisitNextChild(node, result); ++i) {
            ParseTree c = node.getChild(i);
            GraphBuilder childResult = (!trimAlias.test(c)) ? c.accept(this) : this.builder;
            result = this.aggregateResult(result, childResult);
        }

        return result;
    }

    @Override
    public GraphBuilder visitQuery(GremlinGSParser.QueryContext ctx) {
        super.visitQuery(ctx);
        // append tail project to indicate the columns to be output
        return appendTailProject();
    }

    @Override
    public GraphBuilder visitTraversalSourceSpawnMethod_V(
            GremlinGSParser.TraversalSourceSpawnMethod_VContext ctx) {
        builder.source(new SourceConfig(GraphOpt.Source.VERTEX, new LabelConfig(true)));
        List<Number> ids =
                new LiteralList(ctx.oC_ListLiteral(), ctx.oC_Expression()).toList(Number.class);
        if (ids.size() == 1) {
            return builder.filter(
                    builder.call(
                            GraphStdOperatorTable.EQUALS,
                            builder.variable(null, GraphProperty.ID_KEY),
                            builder.literal(ids.get(0))));
        } else if (ids.size() > 1) {
            return builder.filter(
                    builder.getRexBuilder()
                            .makeIn(
                                    builder.variable(null, GraphProperty.ID_KEY),
                                    ids.stream()
                                            .map(k -> builder.literal(k))
                                            .collect(Collectors.toList())));
        }
        return builder;
    }

    @Override
    public GraphBuilder visitTraversalSourceSpawnMethod_E(
            GremlinGSParser.TraversalSourceSpawnMethod_EContext ctx) {
        builder.source(new SourceConfig(GraphOpt.Source.EDGE, new LabelConfig(true)));
        List<Number> ids =
                new LiteralList(ctx.oC_ListLiteral(), ctx.oC_Expression()).toList(Number.class);
        if (ids.size() == 1) {
            return builder.filter(
                    builder.call(
                            GraphStdOperatorTable.EQUALS,
                            builder.variable(null, GraphProperty.ID_KEY),
                            builder.literal(ids.get(0))));
        } else if (ids.size() > 1) {
            return builder.filter(
                    builder.getRexBuilder()
                            .makeIn(
                                    builder.variable(null, GraphProperty.ID_KEY),
                                    ids.stream()
                                            .map(k -> builder.literal(k))
                                            .collect(Collectors.toList())));
        }
        return builder;
    }

    @Override
    public GraphBuilder visitTraversalMethod_hasLabel(
            GremlinGSParser.TraversalMethod_hasLabelContext ctx) {
        List<String> labels =
                new LiteralList(ctx.oC_ListLiteral(), ctx.oC_Expression()).toList(String.class);
        Preconditions.checkArgument(
                !labels.isEmpty(), "there should be at least one label parameter in `hasLabel`");
        if (labels.size() == 1) {
            return builder.filter(
                    builder.call(
                            GraphStdOperatorTable.EQUALS,
                            builder.variable(null, GraphProperty.LABEL_KEY),
                            builder.literal(labels.get(0))));
        } else {
            RexNode labelFilters =
                    builder.getRexBuilder()
                            .makeIn(
                                    builder.variable(null, GraphProperty.LABEL_KEY),
                                    labels.stream()
                                            .map(k -> builder.literal(k))
                                            .collect(Collectors.toList()));
            return builder.filter(labelFilters);
        }
    }

    @Override
    public GraphBuilder visitTraversalMethod_hasId(
            GremlinGSParser.TraversalMethod_hasIdContext ctx) {
        List<Number> ids =
                new LiteralList(ctx.oC_ListLiteral(), ctx.oC_Expression()).toList(Number.class);
        Preconditions.checkArgument(
                !ids.isEmpty(), "there should be at least one id parameter in `hasId`");
        if (ids.size() == 1) {
            return builder.filter(
                    builder.call(
                            GraphStdOperatorTable.EQUALS,
                            builder.variable(null, GraphProperty.ID_KEY),
                            builder.literal(ids.get(0))));
        } else {
            RexNode idFilters =
                    builder.getRexBuilder()
                            .makeIn(
                                    builder.variable(null, GraphProperty.ID_KEY),
                                    ids.stream()
                                            .map(k -> builder.literal(k))
                                            .collect(Collectors.toList()));
            return builder.filter(idFilters);
        }
    }

    @Override
    public GraphBuilder visitTraversalMethod_has(GremlinGSParser.TraversalMethod_hasContext ctx) {
        String notice =
                "supported pattern is [has('key', 'value')] or [has('key', P)] or [has('label',"
                        + " 'key', 'value')] or [has('label', 'key', P)]";
        int childCount = ctx.getChildCount();
        if (childCount == 6 && ctx.oC_Literal() != null) { // g.V().has("name", "marko")
            String propertyKey = (String) LiteralVisitor.INSTANCE.visit(ctx.StringLiteral(0));
            Object propertyValue = LiteralVisitor.INSTANCE.visit(ctx.oC_Literal());
            return builder.filter(
                    builder.call(
                            GraphStdOperatorTable.EQUALS,
                            builder.variable(null, propertyKey),
                            builder.literal(propertyValue)));
        } else if (childCount == 6
                && ctx.traversalPredicate() != null) { // g.V().has("name", P.eq("marko"))
            String propertyKey = (String) LiteralVisitor.INSTANCE.visit(ctx.StringLiteral(0));
            ExpressionVisitor exprVisitor =
                    new ExpressionVisitor(this.builder, builder.variable(null, propertyKey));
            return builder.filter(exprVisitor.visitTraversalPredicate(ctx.traversalPredicate()));
        } else if (childCount == 8
                && ctx.oC_Literal() != null) { // g.V().has("person", "name", "marko")
            String labelValue = (String) LiteralVisitor.INSTANCE.visit(ctx.StringLiteral(0));
            String propertyKey = (String) LiteralVisitor.INSTANCE.visit(ctx.StringLiteral(1));
            Object propertyValue = LiteralVisitor.INSTANCE.visit(ctx.oC_Literal());
            return builder.filter(
                    builder.call(
                            GraphStdOperatorTable.AND,
                            builder.call(
                                    GraphStdOperatorTable.EQUALS,
                                    builder.variable(null, GraphProperty.LABEL_KEY),
                                    builder.literal(labelValue)),
                            builder.call(
                                    GraphStdOperatorTable.EQUALS,
                                    builder.variable(null, propertyKey),
                                    builder.literal(propertyValue))));
        } else if (childCount == 8
                && ctx.traversalPredicate() != null) { // g.V().has("person", "name", P.eq("marko"))
            String labelValue = (String) LiteralVisitor.INSTANCE.visit(ctx.StringLiteral(0));
            String propertyKey = (String) LiteralVisitor.INSTANCE.visit(ctx.StringLiteral(1));
            ExpressionVisitor exprVisitor =
                    new ExpressionVisitor(this.builder, builder.variable(null, propertyKey));
            return builder.filter(
                    builder.call(
                            GraphStdOperatorTable.AND,
                            builder.call(
                                    GraphStdOperatorTable.EQUALS,
                                    builder.variable(null, GraphProperty.LABEL_KEY),
                                    builder.literal(labelValue)),
                            exprVisitor.visitTraversalPredicate(ctx.traversalPredicate())));
        } else if (childCount == 4 && ctx.StringLiteral() != null) { // g.V().has("name")
            String propertyKey = LiteralVisitor.INSTANCE.visit(ctx.StringLiteral(0)).toString();
            return builder.filter(
                    builder.call(
                            GraphStdOperatorTable.IS_NOT_NULL,
                            builder.variable(null, propertyKey)));
        } else {
            throw new UnsupportedEvalException(ctx.getClass(), notice);
        }
    }

    @Override
    public GraphBuilder visitTraversalMethod_hasNot(
            GremlinGSParser.TraversalMethod_hasNotContext ctx) {
        // g.V().hasNot("name")
        String propertyKey = (String) LiteralVisitor.INSTANCE.visit(ctx.StringLiteral());
        return builder.filter(
                builder.call(GraphStdOperatorTable.IS_NULL, builder.variable(null, propertyKey)));
    }

    @Override
    public GraphBuilder visitTraversalMethod_outE(GremlinGSParser.TraversalMethod_outEContext ctx) {
        builder.expand(
                new ExpandConfig(
                        GraphOpt.Expand.OUT,
                        getLabelConfig(ctx.oC_ListLiteral(), ctx.oC_Expression())));
        if (ctx.traversalMethod_inV() != null) {
            visitTraversalMethod_inV(ctx.traversalMethod_inV());
        }
        return builder;
    }

    @Override
    public GraphBuilder visitTraversalMethod_inE(GremlinGSParser.TraversalMethod_inEContext ctx) {
        builder.expand(
                new ExpandConfig(
                        GraphOpt.Expand.IN,
                        getLabelConfig(ctx.oC_ListLiteral(), ctx.oC_Expression())));
        if (ctx.traversalMethod_outV() != null) {
            visitTraversalMethod_outV(ctx.traversalMethod_outV());
        }
        return builder;
    }

    @Override
    public GraphBuilder visitTraversalMethod_bothE(
            GremlinGSParser.TraversalMethod_bothEContext ctx) {
        builder.expand(
                new ExpandConfig(
                        GraphOpt.Expand.BOTH,
                        getLabelConfig(ctx.oC_ListLiteral(), ctx.oC_Expression())));
        if (ctx.traversalMethod_otherV() != null) {
            visitTraversalMethod_otherV(ctx.traversalMethod_otherV());
        }
        return builder;
    }

    @Override
    public GraphBuilder visitTraversalMethod_outV(GremlinGSParser.TraversalMethod_outVContext ctx) {
        return builder.getV(new GetVConfig(GraphOpt.GetV.START, new LabelConfig(true)));
    }

    @Override
    public GraphBuilder visitTraversalMethod_inV(GremlinGSParser.TraversalMethod_inVContext ctx) {
        return builder.getV(new GetVConfig(GraphOpt.GetV.END, new LabelConfig(true)));
    }

    @Override
    public GraphBuilder visitTraversalMethod_otherV(
            GremlinGSParser.TraversalMethod_otherVContext ctx) {
        return builder.getV(new GetVConfig(GraphOpt.GetV.OTHER, new LabelConfig(true)));
    }

    @Override
    public GraphBuilder visitTraversalMethod_endV(GremlinGSParser.TraversalMethod_endVContext ctx) {
        RelNode peek = builder.peek();
        if (peek instanceof GraphLogicalPathExpand) {
            GraphLogicalPathExpand pathExpand = (GraphLogicalPathExpand) peek;
            GraphLogicalExpand expand = (GraphLogicalExpand) pathExpand.getExpand();
            switch (expand.getOpt()) {
                case OUT:
                    return builder.getV(new GetVConfig(GraphOpt.GetV.END, new LabelConfig(true)));
                case IN:
                    return builder.getV(new GetVConfig(GraphOpt.GetV.START, new LabelConfig(true)));
                case BOTH:
                default:
                    return builder.getV(new GetVConfig(GraphOpt.GetV.OTHER, new LabelConfig(true)));
            }
        }
        throw new InvalidGremlinScriptException("endV should follow with path expand");
    }

    @Override
    public GraphBuilder visitTraversalMethod_out(GremlinGSParser.TraversalMethod_outContext ctx) {
        if (pathExpandPattern(ctx.oC_ListLiteral(), ctx.oC_Expression())) {
            return builder.pathExpand(
                    new PathExpandBuilderVisitor(this).visitTraversalMethod_out(ctx).buildConfig());
        } else {
            return builder.expand(
                            new ExpandConfig(
                                    GraphOpt.Expand.OUT,
                                    getLabelConfig(ctx.oC_ListLiteral(), ctx.oC_Expression())))
                    .getV(new GetVConfig(GraphOpt.GetV.END, new LabelConfig(true)));
        }
    }

    @Override
    public GraphBuilder visitTraversalMethod_in(GremlinGSParser.TraversalMethod_inContext ctx) {
        if (pathExpandPattern(ctx.oC_ListLiteral(), ctx.oC_Expression())) {
            return builder.pathExpand(
                    new PathExpandBuilderVisitor(this).visitTraversalMethod_in(ctx).buildConfig());
        } else {
            return builder.expand(
                            new ExpandConfig(
                                    GraphOpt.Expand.IN,
                                    getLabelConfig(ctx.oC_ListLiteral(), ctx.oC_Expression())))
                    .getV(new GetVConfig(GraphOpt.GetV.START, new LabelConfig(true)));
        }
    }

    @Override
    public GraphBuilder visitTraversalMethod_both(GremlinGSParser.TraversalMethod_bothContext ctx) {
        if (pathExpandPattern(ctx.oC_ListLiteral(), ctx.oC_Expression())) {
            return builder.pathExpand(
                    new PathExpandBuilderVisitor(this)
                            .visitTraversalMethod_both(ctx)
                            .buildConfig());
        } else {
            return builder.expand(
                            new ExpandConfig(
                                    GraphOpt.Expand.BOTH,
                                    getLabelConfig(ctx.oC_ListLiteral(), ctx.oC_Expression())))
                    .getV(new GetVConfig(GraphOpt.GetV.OTHER, new LabelConfig(true)));
        }
    }

    @Override
    public GraphBuilder visitTraversalMethod_with(GremlinGSParser.TraversalMethod_withContext ctx) {
        return builder;
    }

    @Override
    public GraphBuilder visitTraversalMethod_as(GremlinGSParser.TraversalMethod_asContext ctx) {
        return builder.as((String) LiteralVisitor.INSTANCE.visit(ctx.StringLiteral()));
    }

    @Override
    public GraphBuilder visitTraversalMethod_valueMap(
            GremlinGSParser.TraversalMethod_valueMapContext ctx) {
        RexNode expr =
                new ExpressionVisitor(builder, builder.variable((String) null))
                        .visitTraversalMethod_valueMap(ctx);
        return builder.project(ImmutableList.of(expr), ImmutableList.of(), true);
    }

    @Override
    public GraphBuilder visitTraversalMethod_values(
            GremlinGSParser.TraversalMethod_valuesContext ctx) {
        if (ctx.getChildCount() == 4 && ctx.StringLiteral() != null) {
            RexNode expr =
                    new ExpressionVisitor(builder, builder.variable((String) null))
                            .visitTraversalMethod_values(ctx);
            return builder.project(ImmutableList.of(expr), ImmutableList.of(), true);
        }
        throw new UnsupportedEvalException(ctx.getClass(), "supported pattern is [values('..')]");
    }

    @Override
    public GraphBuilder visitTraversalMethod_elementMap(
            GremlinGSParser.TraversalMethod_elementMapContext ctx) {
        RexNode expr =
                new ExpressionVisitor(builder, builder.variable((String) null))
                        .visitTraversalMethod_elementMap(ctx);
        return builder.project(ImmutableList.of(expr), ImmutableList.of(), true);
    }

    @Override
    public GraphBuilder visitTraversalMethod_select(
            GremlinGSParser.TraversalMethod_selectContext ctx) {
        RexNode expr;
        LiteralList literalList = new LiteralList(ctx.oC_ListLiteral(), ctx.oC_Expression());
        if (!literalList.isEmpty()) {
            List<String> selectTags = literalList.toList(String.class);
            GremlinGSParser.TraversalMethod_selectby_listContext listCtx =
                    ctx.traversalMethod_selectby_list();
            List<GremlinGSParser.TraversalMethod_selectbyContext> byCtxs =
                    listCtx == null
                            ? ImmutableList.of()
                            : listCtx.getRuleContexts(
                                    GremlinGSParser.TraversalMethod_selectbyContext.class);
            Map<String, RexNode> keyValueMap = Maps.newLinkedHashMap();
            for (int i = 0; i < selectTags.size(); ++i) {
                String selectTag = selectTags.get(i);
                keyValueMap.put(selectTag, convertSelectByCtx(byCtxs, i, selectTag));
            }
            Preconditions.checkArgument(
                    !keyValueMap.isEmpty(), "keyValue should not be empty in select");

            if (keyValueMap.size() == 1) {
                expr = keyValueMap.entrySet().iterator().next().getValue();
            } else {
                List<RexNode> mapParameters = Lists.newArrayList();
                keyValueMap.forEach(
                        (k, v) -> {
                            mapParameters.add(builder.literal(k));
                            mapParameters.add(v);
                        });
                expr = builder.call(GraphStdOperatorTable.MAP_VALUE_CONSTRUCTOR, mapParameters);
            }
        } else if (ctx.traversalColumn() != null) {
            Column column =
                    TraversalEnumParser.parseTraversalEnumFromContext(
                            Column.class, ctx.traversalColumn());
            expr = builder.variable(column.name());
        } else if (ctx.traversalMethod_expr() != null) {
            ExprVisitorResult exprRes =
                    new ExtExpressionVisitor(builder, aliasInfer).visit(ctx.traversalMethod_expr());
            // should aggregate first
            if (!exprRes.getAggCalls().isEmpty()) {
                Preconditions.checkArgument(
                        exprRes.getAggCalls().size() == 1,
                        "only one agg call in expr is supported");
                RelBuilder.AggCall aggCall = exprRes.getAggCalls().get(0);
                // the expr is actually an aggregate call, no need to project further
                if (!(exprRes.getExpr() instanceof RexCall)) {
                    return builder.aggregate(
                            builder.groupKey(), ImmutableList.of(aggCall.as(null)));
                }
                // composite expr, i.e. count(n) + 1
                builder.aggregate(builder.groupKey(), ImmutableList.of(aggCall));
            }
            expr = exprRes.getExpr().accept(new RexTmpVariableConverter(true, builder));
        } else {
            throw new InvalidGremlinScriptException(
                    ctx.getText()
                            + " is invalid, supported pattern is [select('key')] or [select('key1',"
                            + " 'key2', ...)] or [select(Column.keys)] or [select(expr)]");
        }
        return builder.project(ImmutableList.of(expr), ImmutableList.of(), true);
    }

    @Override
    public GraphBuilder visitTraversalMethod_order(
            GremlinGSParser.TraversalMethod_orderContext ctx) {
        GremlinGSParser.TraversalMethod_orderby_listContext listCtx =
                ctx.traversalMethod_orderby_list();
        List<GremlinGSParser.TraversalMethod_orderbyContext> byCtxs =
                listCtx == null
                        ? ImmutableList.of()
                        : listCtx.getRuleContexts(
                                GremlinGSParser.TraversalMethod_orderbyContext.class);
        List<RexNode> exprs = Lists.newArrayList();
        if (byCtxs.isEmpty()) {
            exprs.add(builder.variable((String) null));
        } else {
            for (GremlinGSParser.TraversalMethod_orderbyContext byCtx : byCtxs) {
                List<RexNode> byExprs = convertOrderByCtx(byCtx);
                Order orderOpt = Order.asc;
                if (byCtx.traversalOrder() != null) {
                    orderOpt =
                            TraversalEnumParser.parseTraversalEnumFromContext(
                                    Order.class, byCtx.traversalOrder());
                }
                for (RexNode expr : byExprs) {
                    if (orderOpt == Order.desc) {
                        exprs.add(builder.desc(expr));
                    } else if (orderOpt == Order.asc) {
                        exprs.add(expr);
                    }
                }
            }
        }
        return builder.sortLimit(null, null, exprs);
    }

    @Override
    public GraphBuilder visitTraversalMethod_limit(
            GremlinGSParser.TraversalMethod_limitContext ctx) {
        Number limit = (Number) LiteralVisitor.INSTANCE.visit(ctx.oC_IntegerLiteral());
        return (GraphBuilder) builder.limit(0, limit.intValue());
    }

    @Override
    public GraphBuilder visitTraversalMethod_group(
            GremlinGSParser.TraversalMethod_groupContext ctx) {
        return builder.aggregate(
                convertGroupKeyBy(ctx.traversalMethod_group_keyby()),
                convertGroupValueBy(ctx.traversalMethod_group_valueby()));
    }

    @Override
    public GraphBuilder visitTraversalMethod_groupCount(
            GremlinGSParser.TraversalMethod_groupCountContext ctx) {
        return (GraphBuilder)
                builder.aggregate(
                        convertGroupKeyBy(ctx.traversalMethod_group_keyby()),
                        builder.count(
                                false, Column.values.name(), builder.variable((String) null)));
    }

    @Override
    public GraphBuilder visitOC_AggregateFunctionInvocation(
            GremlinGSParser.OC_AggregateFunctionInvocationContext ctx) {
        String functionName = ctx.getChild(0).getText();
        switch (functionName) {
            case "count":
                return (GraphBuilder)
                        builder.aggregate(
                                builder.groupKey(), builder.count(builder.variable((String) null)));
            case "sum":
                return (GraphBuilder)
                        builder.aggregate(
                                builder.groupKey(),
                                builder.sum(false, null, builder.variable((String) null)));
            case "min":
                return (GraphBuilder)
                        builder.aggregate(
                                builder.groupKey(),
                                builder.min(null, builder.variable((String) null)));
            case "max":
                return (GraphBuilder)
                        builder.aggregate(
                                builder.groupKey(),
                                builder.max(null, builder.variable((String) null)));
            case "mean":
                return (GraphBuilder)
                        builder.aggregate(
                                builder.groupKey(),
                                builder.avg(false, null, builder.variable((String) null)));
            case "fold":
                return (GraphBuilder)
                        builder.aggregate(builder.groupKey(), builder.collect(false, null));
            default:
                throw new UnsupportedEvalException(
                        ctx.getClass(),
                        "supported aggregation functions are count/sum/min/max/mean/fold");
        }
    }

    @Override
    public GraphBuilder visitTraversalMethod_dedup(
            GremlinGSParser.TraversalMethod_dedupContext ctx) {
        List<String> dedupTags =
                new LiteralList(ctx.oC_ListLiteral(), ctx.oC_Expression()).toList(String.class);
        if (dedupTags.isEmpty()) {
            dedupTags.add(null);
        }
        List<RexNode> dedupByKeys =
                dedupTags.stream()
                        .map(k -> convertDedupByCtx(ctx.traversalMethod_dedupby(), k))
                        .collect(Collectors.toList());
        return builder.dedupBy(dedupByKeys);
    }

    @Override
    public GraphBuilder visitTraversalMethod_where(
            GremlinGSParser.TraversalMethod_whereContext ctx) {
        WherePredicateVisitor.Ring whereByRing =
                new WherePredicateVisitor.Ring(ctx.traversalMethod_whereby_list());
        if (ctx.StringLiteral() != null && ctx.traversalPredicate() != null) {
            return builder.filter(
                    new WherePredicateVisitor(
                                    builder,
                                    (String) LiteralVisitor.INSTANCE.visit(ctx.StringLiteral()),
                                    whereByRing)
                            .visitTraversalPredicate(ctx.traversalPredicate()));
        } else if (ctx.traversalPredicate() != null) {
            return builder.filter(
                    new WherePredicateVisitor(builder, null, whereByRing)
                            .visitTraversalPredicate(ctx.traversalPredicate()));
        } else if (ctx.traversalMethod_expr() != null) {
            ExprVisitorResult exprRes =
                    new ExtExpressionVisitor(builder, aliasInfer).visit(ctx.traversalMethod_expr());
            if (!exprRes.getAggCalls().isEmpty()) {
                throw new IllegalArgumentException(
                        "aggregate functions should not exist in filter expression");
            }
            return builder.filter(exprRes.getExpr());
        } else if (ctx.nestedTraversal() != null) {
            TraversalMethodIterator methodIterator =
                    new TraversalMethodIterator(ctx.nestedTraversal());
            String alias = null;
            if (methodIterator.hasNext()) {
                GremlinGSParser.TraversalMethodContext methodCtx = methodIterator.next();
                if (methodCtx.traversalMethod_as() != null) {
                    alias =
                            (String)
                                    LiteralVisitor.INSTANCE.visit(
                                            methodCtx.traversalMethod_as().StringLiteral());
                }
            }
            RexNode subQuery =
                    (new NestedTraversalRexVisitor(builder, alias, ctx))
                            .visitNestedTraversal(ctx.nestedTraversal());
            return builder.filter(Utils.convertExprToPair(subQuery).getValue0());
        } else if (ctx.traversalMethod_not() != null) {
            return visitTraversalMethod_not(ctx.traversalMethod_not());
        }
        throw new UnsupportedEvalException(ctx.getClass(), ctx.getText() + " is unsupported");
    }

    @Override
    public GraphBuilder visitTraversalMethod_not(GremlinGSParser.TraversalMethod_notContext ctx) {
        RexNode subQuery =
                (new NestedTraversalRexVisitor(builder, null, ctx))
                        .visitNestedTraversal(ctx.nestedTraversal());
        return builder.filter(Utils.convertExprToPair(subQuery).getValue0());
    }

    @Override
    public GraphBuilder visitTraversalMethod_is(GremlinGSParser.TraversalMethod_isContext ctx) {
        if (ctx.oC_Literal() != null) {
            return builder.filter(
                    builder.equals(
                            builder.variable((String) null),
                            builder.literal(LiteralVisitor.INSTANCE.visit(ctx.oC_Literal()))));
        } else if (ctx.traversalPredicate() != null) {
            return builder.filter(
                    new ExpressionVisitor(builder, builder.variable((String) null))
                            .visitTraversalPredicate(ctx.traversalPredicate()));
        }
        throw new UnsupportedEvalException(ctx.getClass(), ctx.getText() + " is unsupported");
    }

    @Override
    public GraphBuilder visitTraversalMethod_label(
            GremlinGSParser.TraversalMethod_labelContext ctx) {
        return builder.project(
                ImmutableList.of(builder.variable(null, T.label.getAccessor())),
                ImmutableList.of(),
                true);
    }

    @Override
    public GraphBuilder visitTraversalMethod_id(GremlinGSParser.TraversalMethod_idContext ctx) {
        return builder.project(
                ImmutableList.of(builder.variable(null, T.id.getAccessor())),
                ImmutableList.of(),
                true);
    }

    @Override
    public GraphBuilder visitTraversalMethod_match(
            GremlinGSParser.TraversalMethod_matchContext ctx) {
        Preconditions.checkArgument(
                builder.peek() instanceof GraphLogicalSource,
                "match should start from global source vertices");
        GremlinGSParser.NestedTraversalExprContext exprCtx = ctx.nestedTraversalExpr();
        NestedTraversalRelVisitor visitor = new NestedTraversalRelVisitor(builder);
        List<RelNode> innerSentences = Lists.newArrayList();
        List<RelNode> antiSentences = Lists.newArrayList();
        for (int i = 0; i < exprCtx.getChildCount(); ++i) {
            if (!(exprCtx.getChild(i) instanceof GremlinGSParser.NestedTraversalContext)) continue;
            GremlinGSParser.NestedTraversalContext nestedCtx =
                    (GremlinGSParser.NestedTraversalContext) exprCtx.getChild(i);
            GremlinGSParser.NestedTraversalContext antiCtx = getAntiContext(nestedCtx);
            if (antiCtx != null) {
                antiSentences.add(visitor.visitNestedTraversal(antiCtx));
            } else {
                innerSentences.add(visitor.visitNestedTraversal(nestedCtx));
            }
        }
        Preconditions.checkArgument(
                innerSentences.size() > 0, "match should have at least one inner sentence");
        builder.build();
        // add inner sentences
        if (innerSentences.size() == 1) {
            builder.match(innerSentences.get(0), GraphOpt.Match.INNER);
        } else {
            builder.match(innerSentences.get(0), innerSentences.subList(1, innerSentences.size()));
        }
        // add anti sentences
        for (RelNode anti : antiSentences) {
            builder.match(anti, GraphOpt.Match.ANTI);
        }
        return builder;
    }

    private GremlinGSParser.NestedTraversalContext getAntiContext(
            GremlinGSParser.NestedTraversalContext ctx) {
        GremlinGSParser.ChainedTraversalContext chainedCtx = ctx.chainedTraversal();
        if (chainedCtx != null && chainedCtx.getChildCount() == 1) {
            GremlinGSParser.TraversalMethodContext methodCtx = chainedCtx.traversalMethod();
            if (methodCtx.traversalMethod_not() != null) {
                return methodCtx.traversalMethod_not().nestedTraversal();
            }
        }
        return null;
    }

    @Override
    public GraphBuilder visitTraversalMethod_union(
            GremlinGSParser.TraversalMethod_unionContext ctx) {
        GremlinGSParser.NestedTraversalExprContext exprCtx = ctx.nestedTraversalExpr();
        NestedTraversalRelVisitor visitor = new NestedTraversalRelVisitor(builder);
        List<RelNode> branches = Lists.newArrayList();
        for (int i = 0; i < exprCtx.getChildCount(); ++i) {
            if (!(exprCtx.getChild(i) instanceof GremlinGSParser.NestedTraversalContext)) continue;
            GremlinGSParser.NestedTraversalContext nestedCtx =
                    (GremlinGSParser.NestedTraversalContext) exprCtx.getChild(i);
            branches.add(visitor.visitNestedTraversal(nestedCtx));
        }
        Preconditions.checkArgument(branches.size() > 0, "union should have at least one branch");
        builder.build();
        for (RelNode branch : branches) {
            builder.push(branch);
        }
        return (GraphBuilder) builder.union(true, branches.size());
    }

    @Override
    public GraphBuilder visitTraversalMethod_identity(
            GremlinGSParser.TraversalMethod_identityContext ctx) {
        return builder;
    }

    public GraphBuilder getGraphBuilder() {
        return this.builder;
    }

    public ExprUniqueAliasInfer getAliasInfer() {
        return this.aliasInfer;
    }

    private RelBuilder.GroupKey convertGroupKeyBy(
            GremlinGSParser.TraversalMethod_group_keybyContext keyCtx) {
        String defaultAlias = Column.keys.name();
        if (keyCtx != null) {
            if (keyCtx.StringLiteral() != null) {
                return builder.groupKey(
                        ImmutableList.of(
                                builder.variable(
                                        null,
                                        (String)
                                                LiteralVisitor.INSTANCE.visit(
                                                        keyCtx.StringLiteral()))),
                        ImmutableList.of(defaultAlias));
            } else if (keyCtx.nonStringKeyByList() != null) {
                List<RexNode> exprs = Lists.newArrayList();
                List<@Nullable String> aliases = Lists.newArrayList();
                for (int i = 0; i < keyCtx.nonStringKeyByList().getChildCount(); ++i) {
                    GremlinGSParser.NonStringKeyByContext byCtx =
                            keyCtx.nonStringKeyByList().nonStringKeyBy(i);
                    if (byCtx == null) continue;
                    Pair<RexNode, @Nullable String> exprWithAlias =
                            Utils.convertExprToPair(
                                    new NestedTraversalRexVisitor(this.builder, null, keyCtx)
                                            .visitNestedTraversal(byCtx.nestedTraversal()));
                    exprs.add(exprWithAlias.getValue0());
                    String alias = exprWithAlias.getValue1();
                    aliases.add(alias == AliasInference.DEFAULT_NAME ? null : alias);
                }
                if (exprs.size() == 1) {
                    if (aliases.isEmpty()) {
                        aliases.add(defaultAlias);
                    } else if (aliases.get(0) == null) {
                        aliases.set(0, defaultAlias);
                    }
                }
                return builder.groupKey(exprs, aliases);
            }
        }
        return builder.groupKey(
                ImmutableList.of(builder.variable((String) null)), ImmutableList.of(defaultAlias));
    }

    private List<RelBuilder.AggCall> convertGroupValueBy(
            GremlinGSParser.TraversalMethod_group_valuebyContext valueCtx) {
        String defaultAlias = Column.values.name();
        if (valueCtx != null) {
            if (valueCtx.StringLiteral() != null) {
                return ImmutableList.of(
                        builder.collect(
                                false,
                                defaultAlias,
                                builder.variable(
                                        null,
                                        (String)
                                                LiteralVisitor.INSTANCE.visit(
                                                        valueCtx.StringLiteral()))));
            } else if (valueCtx.nonStringValueByList() != null) {
                List<RelBuilder.AggCall> aggCalls = Lists.newArrayList();
                for (int i = 0; i < valueCtx.nonStringValueByList().getChildCount(); ++i) {
                    GremlinGSParser.NonStringValueByContext byCtx =
                            valueCtx.nonStringValueByList().nonStringValueBy(i);
                    if (byCtx == null) continue;
                    aggCalls.add(
                            new NonStringValueByVisitor(this.builder).visitNonStringValueBy(byCtx));
                }
                if (aggCalls.size() == 1 && ((GraphAggCall) aggCalls.get(0)).getAlias() == null) {
                    aggCalls.set(0, ((GraphAggCall) aggCalls.get(0)).as(defaultAlias));
                }
                return aggCalls;
            }
        }
        return ImmutableList.of(
                builder.collect(false, defaultAlias, builder.variable((String) null)));
    }

    private RexNode convertDedupByCtx(
            GremlinGSParser.TraversalMethod_dedupbyContext byCtx, @Nullable String tag) {
        if (byCtx == null) {
            return builder.variable(tag);
        } else if (byCtx.StringLiteral() != null) {
            return builder.variable(
                    tag, (String) LiteralVisitor.INSTANCE.visit(byCtx.StringLiteral()));
        } else if (byCtx.traversalToken() != null) {
            T token =
                    TraversalEnumParser.parseTraversalEnumFromContext(
                            T.class, byCtx.traversalToken());
            return builder.variable(tag, token.getAccessor());
        } else if (byCtx.nestedTraversal() != null) {
            return Utils.convertExprToPair(
                            new NestedTraversalRexVisitor(this.builder, tag, byCtx)
                                    .visitNestedTraversal(byCtx.nestedTraversal()))
                    .getValue0();
        } else {
            throw new UnsupportedEvalException(
                    byCtx.getClass(), byCtx.getText() + " is unsupported yet");
        }
    }

    private List<RexNode> convertOrderByCtx(GremlinGSParser.TraversalMethod_orderbyContext byCtx) {
        List<RexNode> exprs = Lists.newArrayList();
        if (byCtx.StringLiteral() != null) {
            exprs.add(
                    builder.variable(
                            null, (String) LiteralVisitor.INSTANCE.visit(byCtx.StringLiteral())));
        } else if (byCtx.traversalMethod_values() != null
                || byCtx.traversalMethod_select() != null) {
            RelNode project =
                    (byCtx.traversalMethod_values() != null)
                            ? visitTraversalMethod_values(byCtx.traversalMethod_values()).build()
                            : visitTraversalMethod_select(byCtx.traversalMethod_select()).build();
            Preconditions.checkArgument(
                    project instanceof Project, "rel=%s has invalid class type", project);
            builder.push(((Project) project).getInput());
            exprs.addAll(((Project) project).getProjects());
        } else if (byCtx.nestedTraversal() != null) {
            RexNode rex =
                    Utils.convertExprToPair(
                                    new NestedTraversalRexVisitor(this.builder, null, byCtx)
                                            .visitNestedTraversal(byCtx.nestedTraversal()))
                            .getValue0();
            exprs.add(rex);
        } else {
            exprs.add(builder.variable((String) null));
        }
        return exprs;
    }

    private RexNode convertSelectByCtx(
            List<GremlinGSParser.TraversalMethod_selectbyContext> byCtxs, int i, String tag) {
        int ctxCnt = byCtxs.size();
        if (ctxCnt == 0) {
            return builder.variable(tag);
        }
        GremlinGSParser.TraversalMethod_selectbyContext byCtx = byCtxs.get(i % ctxCnt);
        return new ExpressionVisitor(builder, builder.variable(tag))
                .visitTraversalMethod_selectby(byCtx);
    }

    private boolean pathExpandPattern(
            GremlinGSParser.OC_ListLiteralContext literalCtx,
            List<GremlinGSParser.OC_ExpressionContext> exprCtxList) {
        List<String> labels = new LiteralList(literalCtx, exprCtxList).toList(String.class);
        return !labels.isEmpty() && rangeExpression(labels.get(0));
    }

    private boolean rangeExpression(String label) {
        return label.matches("^\\d+\\.\\.\\d+");
    }

    private LabelConfig getLabelConfig(
            GremlinGSParser.OC_ListLiteralContext literalCtx,
            List<GremlinGSParser.OC_ExpressionContext> exprCtxList) {
        List<String> labels = new LiteralList(literalCtx, exprCtxList).toList(String.class);
        if (labels.isEmpty()) {
            return new LabelConfig(true);
        } else {
            LabelConfig labelConfig = new LabelConfig(false);
            labels.forEach(labelConfig::addLabel);
            return labelConfig;
        }
    }

    private GraphBuilder appendTailProject() {
        Preconditions.checkArgument(builder.size() > 0, "builder should not be empty");
        RelNode top = builder.peek();
        if ((top instanceof Aggregate)
                || (top instanceof GraphLogicalProject)
                        && ((GraphLogicalProject) top).isAppend() == false) return builder;
        List<RexNode> exprs = Lists.newArrayList();
        List<String> aliases = Lists.newArrayList();
        for (RelDataTypeField field : top.getRowType().getFieldList()) {
            exprs.add(builder.variable(field.getName()));
            aliases.add(field.getName() == AliasInference.DEFAULT_NAME ? null : field.getName());
        }
        return builder.project(exprs, aliases);
    }
}

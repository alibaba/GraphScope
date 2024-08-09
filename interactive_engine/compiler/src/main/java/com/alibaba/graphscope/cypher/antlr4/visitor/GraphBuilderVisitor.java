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

import com.alibaba.graphscope.common.antlr4.ExprUniqueAliasInfer;
import com.alibaba.graphscope.common.antlr4.ExprVisitorResult;
import com.alibaba.graphscope.common.ir.meta.schema.LoadCSVTable;
import com.alibaba.graphscope.common.ir.rel.DataSourceTableScan;
import com.alibaba.graphscope.common.ir.rel.GraphLogicalAggregate;
import com.alibaba.graphscope.common.ir.rel.LoadCSVTableScan;
import com.alibaba.graphscope.common.ir.rel.ddl.GraphTableModify;
import com.alibaba.graphscope.common.ir.rel.type.DataFormat;
import com.alibaba.graphscope.common.ir.rel.type.DataSource;
import com.alibaba.graphscope.common.ir.rel.type.FieldMappings;
import com.alibaba.graphscope.common.ir.rel.type.TargetGraph;
import com.alibaba.graphscope.common.ir.rel.type.group.GraphAggCall;
import com.alibaba.graphscope.common.ir.rex.RexGraphDynamicParam;
import com.alibaba.graphscope.common.ir.rex.RexTmpVariableConverter;
import com.alibaba.graphscope.common.ir.rex.RexVariableAliasCollector;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphRexBuilder;
import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;
import com.alibaba.graphscope.grammar.CypherGSBaseVisitor;
import com.alibaba.graphscope.grammar.CypherGSParser;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class GraphBuilderVisitor extends CypherGSBaseVisitor<GraphBuilder> {
    private final GraphBuilder builder;
    private final ExpressionVisitor expressionVisitor;
    private final ExprUniqueAliasInfer aliasInfer;

    public GraphBuilderVisitor(GraphBuilder builder) {
        this(builder, new ExprUniqueAliasInfer());
    }

    public GraphBuilderVisitor(GraphBuilder builder, ExprUniqueAliasInfer aliasInfer) {
        this.builder = Objects.requireNonNull(builder);
        this.aliasInfer = Objects.requireNonNull(aliasInfer);
        this.expressionVisitor = new ExpressionVisitor(this);
    }

    @Override
    public GraphBuilder visitOC_Cypher(CypherGSParser.OC_CypherContext ctx) {
        return visitOC_Statement(ctx.oC_Statement());
    }

    @Override
    public GraphBuilder visitOC_Set(CypherGSParser.OC_SetContext ctx) {
        FieldMappings updateMappings = createFieldMappings(ctx);
        TableModify update =
                new GraphTableModify.Update(builder.getCluster(), builder.build(), updateMappings);
        return builder.push(update);
    }

    private FieldMappings createFieldMappings(CypherGSParser.OC_SetContext ctx) {
        List<FieldMappings.Entry> mappings =
                ctx.oC_SetItem().stream()
                        .map(
                                item -> {
                                    RexNode target = null, source = null;
                                    if (item.oC_PropertyOrLabelsExpression() != null) {
                                        target =
                                                expressionVisitor
                                                        .visitOC_PropertyOrLabelsExpression(
                                                                item
                                                                        .oC_PropertyOrLabelsExpression())
                                                        .getExpr();
                                    } else if (item.oC_Variable() != null) {
                                        target =
                                                expressionVisitor
                                                        .visitOC_Variable(item.oC_Variable())
                                                        .getExpr();
                                    }
                                    if (item.oC_Expression() != null) {
                                        source =
                                                expressionVisitor
                                                        .visitOC_Expression(item.oC_Expression())
                                                        .getExpr();
                                    }
                                    Preconditions.checkArgument(
                                            source != null && target != null,
                                            "source and target should not be null in set item");
                                    return new FieldMappings.Entry(source, target);
                                })
                        .collect(Collectors.toList());
        return new FieldMappings(mappings);
    }

    @Override
    public GraphBuilder visitOC_Delete(CypherGSParser.OC_DeleteContext ctx) {
        List<RexNode> deleteExprs =
                ctx.oC_Expression().stream()
                        .map(item -> expressionVisitor.visitOC_Expression(item).getExpr())
                        .collect(Collectors.toList());
        TableModify delete =
                new GraphTableModify.Delete(builder.getCluster(), builder.build(), deleteExprs);
        return builder.push(delete);
    }

    @Override
    public GraphBuilder visitOC_LoadCSV(CypherGSParser.OC_LoadCSVContext ctx) {
        RexNode location = expressionVisitor.visitOC_Expression(ctx.oC_Expression()).getExpr();
        if (location instanceof RexGraphDynamicParam
                && location.getType().getSqlTypeName() == SqlTypeName.UNKNOWN) {
            RexGraphDynamicParam param = (RexGraphDynamicParam) location;
            ;
            location =
                    ((GraphRexBuilder) builder.getRexBuilder())
                            .makeGraphDynamicParam(
                                    builder.getCluster()
                                            .getTypeFactory()
                                            .createSqlType(SqlTypeName.CHAR),
                                    param.getName(),
                                    param.getIndex());
        }
        DataSource source = new DataSource.Location(location);
        String aliasName = ctx.oC_Variable() != null ? ctx.oC_Variable().getText() : null;
        TableScan tableScan =
                new LoadCSVTableScan(
                        (GraphOptCluster) builder.getCluster(),
                        ImmutableList.of(),
                        new LoadCSVTable(source, createFormat(ctx), null),
                        aliasName);
        return builder.push(tableScan);
    }

    private DataFormat createFormat(CypherGSParser.OC_LoadCSVContext ctx) {
        DataFormat format = new DataFormat();
        if (ctx.oC_WithHeaders() != null) {
            format.withHeader(true);
        }
        if (ctx.oC_FieldTerminator() != null) {
            format.withDelimiter(
                    (String)
                            LiteralVisitor.INSTANCE.visit(
                                    ctx.oC_FieldTerminator().oC_Expression()));
        }
        return format;
    }

    @Override
    public GraphBuilder visitOC_Create(CypherGSParser.OC_CreateContext ctx) {
        TargetGraph target = new TargetGraphVisitor(this).visit(ctx.oC_Pattern());
        TableModify insert =
                new GraphTableModify.Insert(builder.getCluster(), builder.build(), target);
        return builder.push(insert);
    }

    @Override
    public GraphBuilder visitOC_Match(CypherGSParser.OC_MatchContext ctx) {
        // convert the match to DataSourceTableScan if its input is a LoadCSVTableScan
        if (builder.size() > 0 && builder.peek() instanceof LoadCSVTableScan) {
            TargetGraph target = new TargetGraphVisitor(this).visit(ctx.oC_Pattern());
            TableScan scan =
                    new DataSourceTableScan(
                            (GraphOptCluster) builder.getCluster(),
                            ImmutableList.of(),
                            builder.build(),
                            target);
            return builder.push(scan);
        }
        int childCnt = ctx.oC_Pattern().getChildCount();
        List<RelNode> sentences = new ArrayList<>();
        for (int i = 0; i < childCnt; ++i) {
            CypherGSParser.OC_PatternPartContext partCtx = ctx.oC_Pattern().oC_PatternPart(i);
            if (partCtx == null) continue;
            sentences.add(visitOC_PatternPart(partCtx).build());
        }
        if (sentences.size() == 1) {
            builder.match(
                    sentences.get(0),
                    (ctx.OPTIONAL() != null) ? GraphOpt.Match.OPTIONAL : GraphOpt.Match.INNER);
        } else if (sentences.size() > 1) {
            Preconditions.checkArgument(
                    ctx.OPTIONAL() == null, "multiple sentences in match should not be optional");
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
                    new PathExpandBuilderVisitor(this)
                            .visitOC_PatternElementChain(ctx)
                            .buildConfig());
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
        if (!(ctx.parent instanceof CypherGSParser.OC_PatternElementChainContext)) {
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
        // try to convert not exists sub query to anti join
        List<RexNode> conditions = RelOptUtil.conjunctions(res.getExpr());
        List<RexNode> conditionsToRemove = Lists.newArrayList();
        for (RexNode condition : conditions) {
            RelNode antiSentence = getSubQueryRel(condition);
            if (antiSentence != null) {
                builder.match(antiSentence, GraphOpt.Match.ANTI);
                conditionsToRemove.add(condition);
            }
        }
        conditions.removeAll(conditionsToRemove);
        if (!conditions.isEmpty()) {
            builder.filter(conditions);
        }
        return builder;
    }

    /**
     * Return {@code RelNode} nested in the {@code RelSubQuery} if the condition denotes a {@code NOT} {@code RelSubQuery(kind=EXISTS)}
     * @param condition
     * @return
     */
    private @Nullable RelNode getSubQueryRel(RexNode condition) {
        if (condition instanceof RexCall) {
            RexCall call = (RexCall) condition;
            if (call.getOperator().getKind() == SqlKind.NOT) {
                RexNode operand = ((RexCall) condition).operands.get(0);
                if (operand instanceof RexSubQuery
                        && ((RexSubQuery) operand).getOperator().getKind() == SqlKind.EXISTS) {
                    return ((RexSubQuery) operand).rel;
                }
            }
        }
        return null;
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
            if (keyExprs.isEmpty()) {
                groupKey = builder.groupKey();
            } else {
                groupKey = builder.groupKey(keyExprs, keyAliases);
            }
            builder.aggregate(groupKey, aggCalls);
            if (!extraExprs.isEmpty()) {
                RexTmpVariableConverter converter = new RexTmpVariableConverter(true, builder);
                extraExprs =
                        extraExprs.stream()
                                .map(k -> k.accept(converter))
                                .collect(Collectors.toList());
                List<RexNode> projectExprs = Lists.newArrayList();
                List<String> projectAliases = Lists.newArrayList();
                List<String> extraVarNames = Lists.newArrayList();
                RexVariableAliasCollector<String> varNameCollector =
                        new RexVariableAliasCollector<>(
                                true,
                                v -> {
                                    String[] splits = v.getName().split("\\.");
                                    return splits[0];
                                });
                extraExprs.forEach(k -> extraVarNames.addAll(k.accept(varNameCollector)));
                GraphLogicalAggregate aggregate = (GraphLogicalAggregate) builder.peek();
                aggregate
                        .getRowType()
                        .getFieldList()
                        .forEach(
                                field -> {
                                    if (!extraVarNames.contains(field.getName())) {
                                        projectExprs.add(builder.variable(field.getName()));
                                        projectAliases.add(field.getName());
                                    }
                                });
                for (int i = 0; i < extraExprs.size(); ++i) {
                    projectExprs.add(extraExprs.get(i));
                    projectAliases.add(extraAliases.get(i));
                }
                builder.project(projectExprs, projectAliases, false);
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

    public ExprUniqueAliasInfer getAliasInfer() {
        return aliasInfer;
    }
}

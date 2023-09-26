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

import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalExpand;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalPathExpand;
import com.alibaba.graphscope.common.ir.rex.RexGraphVariable;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphStdOperatorTable;
import com.alibaba.graphscope.common.ir.tools.config.*;
import com.alibaba.graphscope.common.ir.type.GraphProperty;
import com.alibaba.graphscope.common.ir.type.GraphSchemaType;
import com.alibaba.graphscope.grammar.GremlinGSBaseVisitor;
import com.alibaba.graphscope.grammar.GremlinGSParser;
import com.alibaba.graphscope.gremlin.antlr4.GenericLiteralVisitor;
import com.alibaba.graphscope.gremlin.antlr4.TraversalEnumParser;
import com.alibaba.graphscope.gremlin.exception.InvalidGremlinScriptException;
import com.alibaba.graphscope.gremlin.exception.UnsupportedEvalException;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.T;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class GraphBuilderVisitor extends GremlinGSBaseVisitor<GraphBuilder> {
    private final GraphBuilder builder;

    public GraphBuilderVisitor(GraphBuilder builder) {
        this.builder = builder;
    }

    @Override
    public GraphBuilder visitTraversalSourceSpawnMethod_V(
            GremlinGSParser.TraversalSourceSpawnMethod_VContext ctx) {
        return builder.source(
                new SourceConfig(
                        GraphOpt.Source.VERTEX,
                        new LabelConfig(true),
                        getNextTag(
                                new TraversalMethodIterator(
                                        (GremlinGSParser.TraversalSourceSpawnMethodContext)
                                                ctx.getParent()))));
    }

    @Override
    public GraphBuilder visitTraversalSourceSpawnMethod_E(
            GremlinGSParser.TraversalSourceSpawnMethod_EContext ctx) {
        return builder.source(
                new SourceConfig(
                        GraphOpt.Source.EDGE,
                        new LabelConfig(true),
                        getNextTag(
                                new TraversalMethodIterator(
                                        (GremlinGSParser.TraversalSourceSpawnMethodContext)
                                                ctx.getParent()))));
    }

    @Override
    public GraphBuilder visitTraversalMethod_hasLabel(
            GremlinGSParser.TraversalMethod_hasLabelContext ctx) {
        if (ctx.stringLiteral() != null) {
            String label = GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral());
            if (ObjectUtils.isEmpty(ctx.stringLiteralList())) {
                return builder.filter(
                        builder.call(
                                GraphStdOperatorTable.EQUALS,
                                builder.variable(null, GraphProperty.LABEL_KEY),
                                builder.literal(label)));
            } else {
                List<RexNode> labelNodes = Lists.newArrayList(builder.literal(label));
                String[] otherLabels =
                        GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList());
                for (String other : otherLabels) {
                    labelNodes.add(builder.literal(other));
                }
                RexNode labelFilters =
                        builder.getRexBuilder()
                                .makeIn(
                                        builder.variable(null, GraphProperty.LABEL_KEY),
                                        labelNodes);
                return builder.filter(labelFilters);
            }
        }
        throw new UnsupportedEvalException(
                ctx.getClass(), "supported pattern is [hasLabel('str')] or hasLabel('str1', ...)");
    }

    @Override
    public GraphBuilder visitTraversalMethod_hasId(
            GremlinGSParser.TraversalMethod_hasIdContext ctx) {
        Object[] ids =
                GenericLiteralVisitor.getIntegerLiteralExpr(
                        ctx.nonEmptyIntegerLiteralList().integerLiteralExpr());
        if (ids.length == 1) {
            return builder.filter(
                    builder.call(
                            GraphStdOperatorTable.EQUALS,
                            builder.variable(null, GraphProperty.ID_KEY),
                            builder.literal(ids[0])));
        } else if (ids.length > 1) {
            List<RexNode> idNodes =
                    Arrays.asList(ids).stream()
                            .map(k -> builder.literal(k))
                            .collect(Collectors.toList());
            RexNode idFilters =
                    builder.getRexBuilder()
                            .makeIn(builder.variable(null, GraphProperty.ID_KEY), idNodes);
            return builder.filter(idFilters);
        }
        throw new UnsupportedEvalException(
                ctx.getClass(), "supported pattern is [hasId(1)] or hasId(1, 2, ...)");
    }

    @Override
    public GraphBuilder visitTraversalMethod_has(GremlinGSParser.TraversalMethod_hasContext ctx) {
        String notice =
                "supported pattern is [has('key', 'value')] or [has('key', P)] or [has('label',"
                        + " 'key', 'value')] or [has('label', 'key', P)]";
        int childCount = ctx.getChildCount();
        if (childCount == 6 && ctx.genericLiteral() != null) { // g.V().has("name", "marko")
            String propertyKey = GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral(0));
            Object propertyValue =
                    GenericLiteralVisitor.getInstance().visitGenericLiteral(ctx.genericLiteral());
            return builder.filter(
                    builder.call(
                            GraphStdOperatorTable.EQUALS,
                            builder.variable(null, propertyKey),
                            builder.literal(propertyValue)));
        } else if (childCount == 6
                && ctx.traversalPredicate() != null) { // g.V().has("name", P.eq("marko"))
            String propertyKey = GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral(0));
            ExpressionVisitor exprVisitor =
                    new ExpressionVisitor(this.builder, builder.variable(null, propertyKey));
            return builder.filter(exprVisitor.visitTraversalPredicate(ctx.traversalPredicate()));
        } else if (childCount == 8
                && ctx.genericLiteral() != null) { // g.V().has("person", "name", "marko")
            String labelValue = GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral(0));
            String propertyKey = GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral(1));
            Object propertyValue =
                    GenericLiteralVisitor.getInstance().visitGenericLiteral(ctx.genericLiteral());
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
            String labelValue = GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral(0));
            String propertyKey = GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral(1));
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
        } else if (childCount == 4 && ctx.stringLiteral() != null) { // g.V().has("name")
            String propertyKey = GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral(0));
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
        String propertyKey = GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral());
        return builder.filter(
                builder.call(GraphStdOperatorTable.IS_NULL, builder.variable(null, propertyKey)));
    }

    @Override
    public GraphBuilder visitTraversalMethod_outE(GremlinGSParser.TraversalMethod_outEContext ctx) {
        return builder.expand(
                new ExpandConfig(
                        GraphOpt.Expand.OUT,
                        getLabelConfig(ctx.stringLiteralList()),
                        getNextTag(
                                new TraversalMethodIterator(
                                        (GremlinGSParser.TraversalMethodContext)
                                                ctx.getParent()))));
    }

    @Override
    public GraphBuilder visitTraversalMethod_inE(GremlinGSParser.TraversalMethod_inEContext ctx) {
        return builder.expand(
                new ExpandConfig(
                        GraphOpt.Expand.IN,
                        getLabelConfig(ctx.stringLiteralList()),
                        getNextTag(
                                new TraversalMethodIterator(
                                        (GremlinGSParser.TraversalMethodContext)
                                                ctx.getParent()))));
    }

    @Override
    public GraphBuilder visitTraversalMethod_bothE(
            GremlinGSParser.TraversalMethod_bothEContext ctx) {
        return builder.expand(
                new ExpandConfig(
                        GraphOpt.Expand.BOTH,
                        getLabelConfig(ctx.stringLiteralList()),
                        getNextTag(
                                new TraversalMethodIterator(
                                        (GremlinGSParser.TraversalMethodContext)
                                                ctx.getParent()))));
    }

    @Override
    public GraphBuilder visitTraversalMethod_outV(GremlinGSParser.TraversalMethod_outVContext ctx) {
        return builder.getV(
                new GetVConfig(
                        GraphOpt.GetV.START,
                        new LabelConfig(true),
                        getNextTag(
                                new TraversalMethodIterator(
                                        (GremlinGSParser.TraversalMethodContext)
                                                ctx.getParent()))));
    }

    @Override
    public GraphBuilder visitTraversalMethod_inV(GremlinGSParser.TraversalMethod_inVContext ctx) {
        return builder.getV(
                new GetVConfig(
                        GraphOpt.GetV.END,
                        new LabelConfig(true),
                        getNextTag(
                                new TraversalMethodIterator(
                                        (GremlinGSParser.TraversalMethodContext)
                                                ctx.getParent()))));
    }

    @Override
    public GraphBuilder visitTraversalMethod_otherV(
            GremlinGSParser.TraversalMethod_otherVContext ctx) {
        return builder.getV(
                new GetVConfig(
                        GraphOpt.GetV.OTHER,
                        new LabelConfig(true),
                        getNextTag(
                                new TraversalMethodIterator(
                                        (GremlinGSParser.TraversalMethodContext)
                                                ctx.getParent()))));
    }

    @Override
    public GraphBuilder visitTraversalMethod_endV(GremlinGSParser.TraversalMethod_endVContext ctx) {
        RelNode peek = builder.peek();
        if (peek instanceof GraphLogicalPathExpand) {
            GraphLogicalPathExpand pathExpand = (GraphLogicalPathExpand) peek;
            GraphLogicalExpand expand = (GraphLogicalExpand) pathExpand.getExpand();
            String tag =
                    getNextTag(
                            new TraversalMethodIterator(
                                    (GremlinGSParser.TraversalMethodContext) ctx.getParent()));
            switch (expand.getOpt()) {
                case OUT:
                    return builder.getV(
                            new GetVConfig(GraphOpt.GetV.END, new LabelConfig(true), tag));
                case IN:
                    return builder.getV(
                            new GetVConfig(GraphOpt.GetV.START, new LabelConfig(true), tag));
                case BOTH:
                default:
                    return builder.getV(
                            new GetVConfig(GraphOpt.GetV.OTHER, new LabelConfig(true), tag));
            }
        }
        throw new InvalidGremlinScriptException("endV should follow with path expand");
    }

    @Override
    public GraphBuilder visitTraversalMethod_out(GremlinGSParser.TraversalMethod_outContext ctx) {
        if (pathExpandPattern(ctx.stringLiteralList())) {
            return builder.pathExpand(
                    new PathExpandBuilderVisitor(this).visitTraversalMethod_out(ctx).build());
        } else {
            return builder.expand(
                            new ExpandConfig(
                                    GraphOpt.Expand.OUT, getLabelConfig(ctx.stringLiteralList())))
                    .getV(
                            new GetVConfig(
                                    GraphOpt.GetV.END,
                                    new LabelConfig(true),
                                    getNextTag(
                                            new TraversalMethodIterator(
                                                    (GremlinGSParser.TraversalMethodContext)
                                                            ctx.getParent()))));
        }
    }

    @Override
    public GraphBuilder visitTraversalMethod_in(GremlinGSParser.TraversalMethod_inContext ctx) {
        if (pathExpandPattern(ctx.stringLiteralList())) {
            return builder.pathExpand(
                    new PathExpandBuilderVisitor(this).visitTraversalMethod_in(ctx).build());
        } else {
            return builder.expand(
                            new ExpandConfig(
                                    GraphOpt.Expand.IN, getLabelConfig(ctx.stringLiteralList())))
                    .getV(
                            new GetVConfig(
                                    GraphOpt.GetV.START,
                                    new LabelConfig(true),
                                    getNextTag(
                                            new TraversalMethodIterator(
                                                    (GremlinGSParser.TraversalMethodContext)
                                                            ctx.getParent()))));
        }
    }

    @Override
    public GraphBuilder visitTraversalMethod_both(GremlinGSParser.TraversalMethod_bothContext ctx) {
        if (pathExpandPattern(ctx.stringLiteralList())) {
            return builder.pathExpand(
                    new PathExpandBuilderVisitor(this).visitTraversalMethod_both(ctx).build());
        } else {
            return builder.expand(
                            new ExpandConfig(
                                    GraphOpt.Expand.BOTH, getLabelConfig(ctx.stringLiteralList())))
                    .getV(
                            new GetVConfig(
                                    GraphOpt.GetV.OTHER,
                                    new LabelConfig(true),
                                    getNextTag(
                                            new TraversalMethodIterator(
                                                    (GremlinGSParser.TraversalMethodContext)
                                                            ctx.getParent()))));
        }
    }

    @Override
    public GraphBuilder visitTraversalMethod_with(GremlinGSParser.TraversalMethod_withContext ctx) {
        return builder;
    }

    @Override
    public GraphBuilder visitTraversalMethod_as(GremlinGSParser.TraversalMethod_asContext ctx) {
        return builder;
    }

    @Override
    public GraphBuilder visitTraversalMethod_valueMap(
            GremlinGSParser.TraversalMethod_valueMapContext ctx) {
        RexNode expr =
                builder.call(
                        GraphStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
                        getProperties(ctx, null).stream()
                                .flatMap(
                                        k ->
                                                Stream.of(
                                                        builder.literal(k),
                                                        builder.variable(null, k)))
                                .collect(Collectors.toList()));
        String tag = getNextTag(
                new TraversalMethodIterator(
                        (GremlinGSParser.TraversalMethodContext) ctx.getParent()));
        return builder.project(
                ImmutableList.of(expr), tag == null ? ImmutableList.of() : ImmutableList.of(tag), true);
    }

    @Override
    public GraphBuilder visitTraversalMethod_values(
            GremlinGSParser.TraversalMethod_valuesContext ctx) {
        if (ctx.getChildCount() == 4 && ctx.stringLiteral() != null) {
            RexNode expr =
                    builder.variable(
                            null, GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()));
            String tag = getNextTag(
                    new TraversalMethodIterator(
                            (GremlinGSParser.TraversalMethodContext) ctx.getParent()));
            return builder.project(
                    ImmutableList.of(expr), tag == null ? ImmutableList.of() : ImmutableList.of(tag), true);
        }
        throw new UnsupportedEvalException(ctx.getClass(), "supported pattern is [values('..')]");
    }

    @Override
    public GraphBuilder visitTraversalMethod_elementMap(
            GremlinGSParser.TraversalMethod_elementMapContext ctx) {
        RexNode expr =
                builder.call(
                        GraphStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
                        getProperties(ctx, null).stream()
                                .flatMap(
                                        k ->
                                                Stream.of(
                                                        builder.literal(k),
                                                        builder.variable(null, k)))
                                .collect(Collectors.toList()));
        String tag = getNextTag(
                new TraversalMethodIterator(
                        (GremlinGSParser.TraversalMethodContext) ctx.getParent()));
        return builder.project(
                ImmutableList.of(expr), tag == null ? ImmutableList.of() : ImmutableList.of(tag), true);
    }

    @Override
    public GraphBuilder visitTraversalMethod_select(
            GremlinGSParser.TraversalMethod_selectContext ctx) {
        String tag = getNextTag(
                new TraversalMethodIterator(
                        (GremlinGSParser.TraversalMethodContext) ctx.getParent()));
        RexNode expr;
        if (ctx.stringLiteral() != null) {
            List<String> selectTags =
                    Lists.newArrayList(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()));
            if (ctx.stringLiteralList() != null) {
                String[] tags = GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList());
                selectTags.addAll(Arrays.asList(tags));
            }
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
                expr = builder.call(GraphStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR, mapParameters);
            }
        } else if (ctx.traversalColumn() != null) {
            Column column =
                    TraversalEnumParser.parseTraversalEnumFromContext(
                            Column.class, ctx.traversalColumn());
            expr = builder.variable(column.name());
        } else {
            throw new UnsupportedEvalException(
                    GremlinGSParser.TraversalMethod_selectbyContext.class,
                    ctx.getText() + " is unsupported yet");
        }
        return builder.project(ImmutableList.of(expr), tag == null ? ImmutableList.of() : ImmutableList.of(tag), true);
    }

    public GraphBuilder getGraphBuilder() {
        return this.builder;
    }

    private RexNode convertSelectByCtx(
            List<GremlinGSParser.TraversalMethod_selectbyContext> byCtxs, int i, String tag) {
        int ctxCnt = byCtxs.size();
        if (ctxCnt == 0) {
            return builder.variable(tag);
        }
        GremlinGSParser.TraversalMethod_selectbyContext byCtx = byCtxs.get(i % ctxCnt);
        int byChildCount = byCtx.getChildCount();
        if (byChildCount == 3) { // select(..).by()
            return builder.variable(tag);
        } else if (byChildCount == 4 && byCtx.stringLiteral() != null) { // select(..).by('name')
            return builder.variable(
                    tag, GenericLiteralVisitor.getStringLiteral(byCtx.stringLiteral()));
        } else if (byChildCount == 4
                && byCtx.traversalToken() != null) { // select(..).by(T.label/T.id)
            T token =
                    TraversalEnumParser.parseTraversalEnumFromContext(
                            T.class, byCtx.traversalToken());
            return builder.variable(tag, token.getAccessor());
        } else if (byCtx.traversalMethod_valueMap() != null) { // select(..).by(valueMap('name'))
            return builder.call(
                    GraphStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
                    getProperties(byCtx.traversalMethod_valueMap(), tag).stream()
                            .flatMap(k -> Stream.of(builder.literal(k), builder.variable(tag, k)))
                            .collect(Collectors.toList()));
        } else if (byCtx.traversalMethod_elementMap()
                != null) { // select(..).by(elementMap('name'))
            return builder.call(
                    GraphStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
                    getProperties(byCtx.traversalMethod_elementMap(), tag).stream()
                            .flatMap(k -> Stream.of(builder.literal(k), builder.variable(tag, k)))
                            .collect(Collectors.toList()));
        }
        throw new UnsupportedEvalException(
                GremlinGSParser.TraversalMethod_selectbyContext.class,
                byCtx.getText() + " is unsupported yet in select");
    }

    private boolean pathExpandPattern(GremlinGSParser.StringLiteralListContext ctx) {
        String[] labels = GenericLiteralVisitor.getStringLiteralList(ctx);
        return labels != null && labels.length > 0 && rangeExpression(labels[0]);
    }

    private boolean rangeExpression(String label) {
        return label.matches("^\\d+\\.\\.\\d+");
    }

    private LabelConfig getLabelConfig(GremlinGSParser.StringLiteralListContext ctx) {
        String[] labels = GenericLiteralVisitor.getStringLiteralList(ctx);
        if (labels == null || labels.length == 0) {
            return new LabelConfig(true);
        } else {
            LabelConfig labelConfig = new LabelConfig(false);
            for (int i = 0; i < labels.length; ++i) {
                labelConfig.addLabel(labels[i]);
            }
            return labelConfig;
        }
    }

    private List<String> getProperties(
            GremlinGSParser.TraversalMethod_valueMapContext ctx, @Nullable String tag) {
        String[] properties = GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList());
        return (properties == null || properties.length == 0)
                ? getAllProperties(tag)
                : Arrays.asList(properties);
    }

    private List<String> getProperties(
            GremlinGSParser.TraversalMethod_elementMapContext ctx, @Nullable String tag) {
        String[] properties = GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList());
        List<String> propertyList =
                Lists.newArrayList(GraphProperty.LABEL_KEY, GraphProperty.ID_KEY);
        if (properties == null || properties.length == 0) {
            propertyList.addAll(getAllProperties(tag));
        } else {
            propertyList.addAll(Arrays.asList(properties));
        }
        return propertyList;
    }

    private List<String> getAllProperties(@Nullable String tag) {
        RexGraphVariable curVar = builder.variable(tag);
        RelDataType dataType = curVar.getType();
        Preconditions.checkArgument(
                dataType instanceof GraphSchemaType, "can not get property from type=", dataType);
        return dataType.getFieldList().stream().map(k -> k.getName()).collect(Collectors.toList());
    }

    protected @Nullable String getNextTag(TraversalMethodIterator methodIterator) {
        List<String> tags = Lists.newArrayList();
        while (methodIterator.hasNext()) {
            GremlinGSParser.TraversalMethodContext next = methodIterator.next();
            if (next.traversalMethod_as() != null) {
                tags.add(
                        GenericLiteralVisitor.getStringLiteral(
                                next.traversalMethod_as().stringLiteral()));
            } else if (next.traversalMethod_has() != null
                    || next.traversalMethod_hasId() != null
                    || next.traversalMethod_hasLabel() != null
                    || next.traversalMethod_hasNot() != null
                    || next.traversalMethod_is() != null
                    || next.traversalMethod_where() != null
                    || next.traversalMethod_not() != null
                    || next.traversalMethod_identity() != null
                    || next.traversalMethod_limit() != null
                    || next.traversalMethod_order() != null
                    || next.traversalMethod_dedup() != null) {
                // continue
            } else {
                break;
            }
        }
        return tags.isEmpty() ? null : tags.get(tags.size() - 1);
    }
}

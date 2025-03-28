/*
 *
 *  * Copyright 2020 Alibaba Group Holding Limited.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.alibaba.graphscope.cypher.antlr4.visitor;

import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalExpand;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalSource;
import com.alibaba.graphscope.common.ir.rel.type.FieldMappings;
import com.alibaba.graphscope.common.ir.rel.type.TargetGraph;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.config.ExpandConfig;
import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;
import com.alibaba.graphscope.common.ir.tools.config.LabelConfig;
import com.alibaba.graphscope.common.ir.tools.config.SourceConfig;
import com.alibaba.graphscope.grammar.CypherGSBaseVisitor;
import com.alibaba.graphscope.grammar.CypherGSParser;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.rex.RexNode;
import org.apache.commons.lang3.ObjectUtils;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Visit AST of {@code Pattern} to generate {@code TargetGraph}
 */
public class TargetGraphVisitor extends CypherGSBaseVisitor<List<TargetGraph>> {
    private final GraphBuilder parentBuilder;
    private final ExpressionVisitor expressionVisitor;
    private final GraphBuilder builder;

    public TargetGraphVisitor(GraphBuilderVisitor parentVisitor) {
        this.parentBuilder = parentVisitor.getGraphBuilder();
        this.expressionVisitor = parentVisitor.getExpressionVisitor();
        this.builder =
                GraphBuilder.create(
                        parentBuilder.getContext(),
                        (GraphOptCluster) parentBuilder.getCluster(),
                        parentBuilder.getRelOptSchema());
    }

    @Override
    public List<TargetGraph> visitOC_PatternElement(CypherGSParser.OC_PatternElementContext ctx) {
        List<CypherGSParser.OC_PatternElementChainContext> elementChain =
                ctx.oC_PatternElementChain();
        // create vertex
        if (ObjectUtils.isEmpty(elementChain)) {
            return visitOC_NodePattern(ctx.oC_NodePattern());
        }
        if (elementChain.size() > 1) {
            throw new UnsupportedOperationException("cannot create multiple edges in one pattern");
        }
        // create edge
        return visitOC_RelationshipPattern(
                ctx.oC_NodePattern(),
                elementChain.get(0).oC_NodePattern(),
                elementChain.get(0).oC_RelationshipPattern());
    }

    @Override
    public List<TargetGraph> visitOC_NodePattern(CypherGSParser.OC_NodePatternContext ctx) {
        SourceConfig config = Utils.sourceConfig(ctx);
        Preconditions.checkArgument(
                !config.getLabels().isAll(), "cannot create vertex with unknown label");
        return config.getLabels().getLabels().stream()
                .map(
                        label -> {
                            GraphLogicalSource source =
                                    (GraphLogicalSource)
                                            builder.source(
                                                            new SourceConfig(
                                                                    config.getOpt(),
                                                                    new LabelConfig(false)
                                                                            .addLabel(label),
                                                                    config.getAlias()))
                                                    .peek();
                            TargetGraph target =
                                    new TargetGraph.Vertex(
                                            source.getTable(),
                                            new FieldMappings(visitProperties(ctx.oC_Properties())),
                                            config.getAlias());
                            builder.build();
                            return target;
                        })
                .collect(Collectors.toList());
    }

    public List<TargetGraph> visitOC_RelationshipPattern(
            CypherGSParser.OC_NodePatternContext srcCtx,
            CypherGSParser.OC_NodePatternContext dstCtx,
            CypherGSParser.OC_RelationshipPatternContext edgeCtx) {
        List<TargetGraph> srcGraph = visitOC_NodePattern(srcCtx);
        List<TargetGraph> dstGraph = visitOC_NodePattern(dstCtx);
        ExpandConfig config = Utils.expandConfig(edgeCtx);
        GraphLogicalExpand expand =
                (GraphLogicalExpand)
                        builder.source(Utils.sourceConfig(srcCtx)).expand(config).peek();
        Preconditions.checkArgument(
                expand.getOpt() != GraphOpt.Expand.BOTH, "cannot create edge with BOTH direction");
        List<TargetGraph> targetGraphs = Lists.newArrayList();
        srcGraph.forEach(
                src -> {
                    dstGraph.forEach(
                            dst -> {
                                TargetGraph.Edge target =
                                        new TargetGraph.Edge(
                                                expand.getTable(),
                                                new FieldMappings(
                                                        visitProperties(
                                                                edgeCtx.oC_RelationshipDetail())),
                                                config.getAlias());
                                switch (expand.getOpt()) {
                                    case OUT:
                                        target.withSrcVertex(src).withDstVertex(dst);
                                        break;
                                    case IN:
                                    default:
                                        target.withSrcVertex(dst).withDstVertex(src);
                                }
                                targetGraphs.add(target);
                            });
                });
        builder.build();
        return targetGraphs;
    }

    private List<FieldMappings.Entry> visitProperties(CypherGSParser.OC_PropertiesContext ctx) {
        List<FieldMappings.Entry> entries = Lists.newArrayList();
        if (ctx != null) {
            CypherGSParser.OC_MapLiteralContext mapCtx = ctx.oC_MapLiteral();
            for (int i = 0; i < mapCtx.oC_PropertyKeyName().size(); ++i) {
                RexNode target = builder.variable(null, mapCtx.oC_PropertyKeyName(i).getText());
                RexNode source =
                        expressionVisitor.visitOC_Expression(mapCtx.oC_Expression(i)).getExpr();
                entries.add(new FieldMappings.Entry(source, target));
            }
        }
        return entries;
    }

    private List<FieldMappings.Entry> visitProperties(
            CypherGSParser.OC_RelationshipDetailContext ctx) {
        return ctx == null ? ImmutableList.of() : visitProperties(ctx.oC_Properties());
    }
}

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
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalGetV;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalSource;
import com.alibaba.graphscope.common.ir.rel.type.FieldMappings;
import com.alibaba.graphscope.common.ir.rel.type.TargetGraph;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.config.ExpandConfig;
import com.alibaba.graphscope.common.ir.tools.config.SourceConfig;
import com.alibaba.graphscope.grammar.CypherGSBaseVisitor;
import com.alibaba.graphscope.grammar.CypherGSParser;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/**
 * Visit AST of {@code Pattern} to generate {@code TargetGraph}
 */
public class TargetGraphVisitor extends CypherGSBaseVisitor<TargetGraph> {
    private final GraphBuilder parentBuilder;
    private final ExpressionVisitor expressionVisitor;
    private final GraphBuilder builder;

    private TargetGraph targetCache = null;

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
    public TargetGraph visitOC_NodePattern(CypherGSParser.OC_NodePatternContext ctx) {
        // source
        if (!(ctx.parent instanceof CypherGSParser.OC_PatternElementChainContext)) {
            Preconditions.checkArgument(
                    targetCache == null, "optTable graph should be null before visiting source");
            SourceConfig config = Utils.sourceConfig(ctx);
            builder.source(config);
            GraphLogicalSource source = (GraphLogicalSource) builder.peek();
            targetCache =
                    new TargetGraph.Vertex(
                            source.getTable(),
                            new FieldMappings(visitProperties(ctx.oC_Properties())),
                            config.getAlias());
        } else { // getV
            Preconditions.checkArgument(
                    targetCache != null && targetCache instanceof TargetGraph.Edge,
                    "edge optTable graph should have existed before visiting getV");
            builder.getV(Utils.getVConfig(ctx));
            GraphLogicalGetV getV = (GraphLogicalGetV) builder.peek();
            ((TargetGraph.Edge) targetCache)
                    .withDstVertex(
                            new TargetGraph.Vertex(
                                    getV.getTable(),
                                    new FieldMappings(visitProperties(ctx.oC_Properties())),
                                    getV.getAliasName()));
        }
        return targetCache;
    }

    @Override
    public TargetGraph visitOC_RelationshipPattern(
            CypherGSParser.OC_RelationshipPatternContext ctx) {
        Preconditions.checkArgument(
                targetCache != null && targetCache instanceof TargetGraph.Vertex,
                "vertex optTable cache should have existed before visiting edge");
        ExpandConfig config = Utils.expandConfig(ctx);
        builder.expand(config);
        GraphLogicalExpand expand = (GraphLogicalExpand) builder.peek();
        targetCache =
                new TargetGraph.Edge(
                                expand.getTable(),
                                new FieldMappings(visitProperties(ctx.oC_RelationshipDetail())),
                                config.getAlias())
                        .withSrcVertex(targetCache);
        return this.targetCache;
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

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

import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphStdOperatorTable;
import com.alibaba.graphscope.common.ir.tools.config.*;
import com.alibaba.graphscope.grammar.CypherGSParser;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

public abstract class Utils extends com.alibaba.graphscope.common.antlr4.Utils {
    public static SourceConfig sourceConfig(CypherGSParser.OC_NodePatternContext ctx) {
        String alias = getAliasName(ctx.oC_Variable());
        LabelConfig config = labelConfig(ctx.oC_NodeLabels());
        // source
        return new SourceConfig(GraphOpt.Source.VERTEX, config, alias);
    }

    public static GetVConfig getVConfig(CypherGSParser.OC_NodePatternContext ctx) {
        String alias = getAliasName(ctx.oC_Variable());
        LabelConfig config = labelConfig(ctx.oC_NodeLabels());
        // getV
        return new GetVConfig(getVOpt(ctx), config, alias);
    }

    public static ExpandConfig expandConfig(CypherGSParser.OC_RelationshipPatternContext ctx) {
        CypherGSParser.OC_RelationshipDetailContext detailCtx = ctx.oC_RelationshipDetail();
        return expandConfig(detailCtx, directionOpt(ctx));
    }

    public static ExpandConfig expandConfig(
            CypherGSParser.OC_RelationshipDetailContext ctx, GraphOpt.Expand opt) {
        String alias = getAliasName(ctx.oC_Variable());
        LabelConfig config = labelConfig(ctx.oC_RelationshipTypes());
        return new ExpandConfig(opt, config, alias);
    }

    public static LabelConfig labelConfig(CypherGSParser.OC_NodeLabelsContext ctx) {
        LabelConfig config;
        if (ctx == null) {
            config = new LabelConfig(true);
        } else {
            config = new LabelConfig(false);
            for (CypherGSParser.OC_LabelNameContext ctx1 : ctx.oC_LabelName()) {
                if (ctx1 == null) continue;
                config.addLabel(getLabelName(ctx1));
            }
        }
        return config;
    }

    public static LabelConfig labelConfig(CypherGSParser.OC_RelationshipTypesContext ctx) {
        LabelConfig config;
        if (ctx == null) {
            config = new LabelConfig(true);
        } else {
            config = new LabelConfig(false);
            for (CypherGSParser.OC_RelTypeNameContext ctx1 : ctx.oC_RelTypeName()) {
                if (ctx1 == null) continue;
                config.addLabel(getLabelName(ctx1));
            }
        }
        return config;
    }

    public static GraphOpt.GetV getVOpt(CypherGSParser.OC_NodePatternContext ctx) {
        if (ctx.parent instanceof CypherGSParser.OC_PatternElementChainContext) {
            return getVOpt(
                    ((CypherGSParser.OC_PatternElementChainContext) ctx.parent)
                            .oC_RelationshipPattern());
        } else {
            throw new IllegalArgumentException("cannot infer opt from ctx " + ctx);
        }
    }

    public static GraphOpt.GetV getVOpt(CypherGSParser.OC_RelationshipPatternContext ctx) {
        if (ctx.oC_LeftArrowHead() == null && ctx.oC_RightArrowHead() == null
                || ctx.oC_LeftArrowHead() != null && ctx.oC_RightArrowHead() != null) {
            return GraphOpt.GetV.OTHER;
        } else if (ctx.oC_RightArrowHead() != null) {
            return GraphOpt.GetV.END;
        } else {
            return GraphOpt.GetV.START;
        }
    }

    public static GraphOpt.Expand directionOpt(CypherGSParser.OC_RelationshipPatternContext ctx) {
        if (ctx.oC_LeftArrowHead() == null && ctx.oC_RightArrowHead() == null
                || ctx.oC_LeftArrowHead() != null && ctx.oC_RightArrowHead() != null) {
            return GraphOpt.Expand.BOTH;
        } else if (ctx.oC_LeftArrowHead() != null) {
            return GraphOpt.Expand.IN;
        } else {
            return GraphOpt.Expand.OUT;
        }
    }

    public static final List<RexNode> propertyFilters(
            GraphBuilder builder,
            ExpressionVisitor expressionVisitor,
            CypherGSParser.OC_PropertiesContext ctx) {
        CypherGSParser.OC_MapLiteralContext mapCtx = ctx.oC_MapLiteral();
        List<RexNode> filters = new ArrayList<>();
        for (int i = 0; i < mapCtx.oC_PropertyKeyName().size(); ++i) {
            RexNode variable = builder.variable(null, mapCtx.oC_PropertyKeyName(i).getText());
            RexNode value = expressionVisitor.visitOC_Expression(mapCtx.oC_Expression(i)).getExpr();
            if (value.getKind() == SqlKind.ARRAY_VALUE_CONSTRUCTOR) {
                filters.add(
                        builder.getRexBuilder().makeIn(variable, ((RexCall) value).getOperands()));
            } else {
                filters.add(builder.call(GraphStdOperatorTable.EQUALS, variable, value));
            }
        }
        return filters;
    }

    public static @Nullable String getAliasName(CypherGSParser.OC_VariableContext ctx) {
        return ctx == null ? null : (String) LiteralVisitor.INSTANCE.visit(ctx);
    }

    public static String getLabelName(CypherGSParser.OC_LabelNameContext ctx) {
        return (String) LiteralVisitor.INSTANCE.visit(ctx);
    }

    public static String getLabelName(CypherGSParser.OC_RelTypeNameContext ctx) {
        return (String) LiteralVisitor.INSTANCE.visit(ctx);
    }
}

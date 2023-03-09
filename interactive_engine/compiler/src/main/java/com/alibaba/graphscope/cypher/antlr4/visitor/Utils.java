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

import com.alibaba.graphscope.common.ir.tools.GraphStdOperatorTable;
import com.alibaba.graphscope.common.ir.tools.config.*;
import com.alibaba.graphscope.grammar.CypherGSParser;

import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.calcite.sql.SqlOperator;

import java.util.ArrayList;
import java.util.List;

public abstract class Utils {
    public static SourceConfig sourceConfig(CypherGSParser.OC_NodePatternContext ctx) {
        String alias = (ctx.oC_Variable() != null) ? ctx.oC_Variable().getText() : null;
        LabelConfig config = labelConfig(ctx.oC_NodeLabels());
        // source
        return new SourceConfig(GraphOpt.Source.VERTEX, config, alias);
    }

    public static GetVConfig getVConfig(CypherGSParser.OC_NodePatternContext ctx) {
        String alias = (ctx.oC_Variable() != null) ? ctx.oC_Variable().getText() : null;
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
        String alias = (ctx.oC_Variable() != null) ? ctx.oC_Variable().getText() : null;
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
                config.addLabel(ctx1.getText());
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
                config.addLabel(ctx1.getText());
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
            return GraphOpt.GetV.BOTH;
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

    public static List<SqlOperator> getOperators(
            List<ParseTree> trees, List<String> opSigns, boolean isPrefix) {
        List<SqlOperator> operators = new ArrayList<>();
        for (ParseTree tree : trees) {
            if (tree instanceof TerminalNode && opSigns.contains(tree.getText())) {
                if (tree.getText().equals("+")) {
                    if (isPrefix) {
                        operators.add(GraphStdOperatorTable.UNARY_PLUS);
                    } else {
                        operators.add(GraphStdOperatorTable.PLUS);
                    }
                } else if (tree.getText().equals("-")) {
                    if (isPrefix) {
                        operators.add(GraphStdOperatorTable.UNARY_MINUS);
                    } else {
                        operators.add(GraphStdOperatorTable.MINUS);
                    }
                } else if (tree.getText().equals("*")) {
                    operators.add(GraphStdOperatorTable.MULTIPLY);
                } else if (tree.getText().equals("/")) {
                    operators.add(GraphStdOperatorTable.DIVIDE);
                } else if (tree.getText().equals("%")) {
                    operators.add(GraphStdOperatorTable.MOD);
                } else if (tree.getText().equals("^")) {
                    operators.add(GraphStdOperatorTable.POWER);
                } else if (tree.getText().equals("=")) {
                    operators.add(GraphStdOperatorTable.EQUALS);
                } else if (tree.getText().equals("<>")) {
                    operators.add(GraphStdOperatorTable.NOT_EQUALS);
                } else if (tree.getText().equals("<")) {
                    operators.add(GraphStdOperatorTable.LESS_THAN);
                } else if (tree.getText().equals(">")) {
                    operators.add(GraphStdOperatorTable.GREATER_THAN);
                } else if (tree.getText().equals("<=")) {
                    operators.add(GraphStdOperatorTable.LESS_THAN_OR_EQUAL);
                } else if (tree.getText().equals(">=")) {
                    operators.add(GraphStdOperatorTable.GREATER_THAN_OR_EQUAL);
                } else {
                    throw new UnsupportedOperationException(
                            "operator " + tree.getText() + " is unsupported yet");
                }
            }
        }
        return operators;
    }
}

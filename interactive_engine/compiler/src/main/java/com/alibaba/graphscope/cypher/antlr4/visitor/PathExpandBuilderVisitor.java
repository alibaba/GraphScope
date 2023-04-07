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

import com.alibaba.graphscope.common.ir.tools.config.ExpandConfig;
import com.alibaba.graphscope.common.ir.tools.config.PathExpandConfig;
import com.alibaba.graphscope.grammar.CypherGSBaseVisitor;
import com.alibaba.graphscope.grammar.CypherGSParser;

import java.util.Objects;

public class PathExpandBuilderVisitor extends CypherGSBaseVisitor<PathExpandConfig.Builder> {
    private final GraphBuilderVisitor parent;
    private final PathExpandConfig.Builder builder;

    public PathExpandBuilderVisitor(GraphBuilderVisitor parent) {
        this.parent = Objects.requireNonNull(parent);
        // PATH_OPT = ARBITRARY and RESULT_OPT = END_V are set by default
        this.builder = PathExpandConfig.newBuilder(parent.getGraphBuilder());
    }

    @Override
    public PathExpandConfig.Builder visitOC_RelationshipPattern(
            CypherGSParser.OC_RelationshipPatternContext ctx) {
        ExpandConfig expandConfig = Utils.expandConfig(ctx);
        // set expand base in path_expand
        builder.expand(expandConfig);
        // fuse filters with expand base in path_expand
        visitOC_Properties(ctx.oC_RelationshipDetail().oC_Properties());
        // set path expand alias
        if (expandConfig.getAlias() != null) {
            builder.alias(expandConfig.getAlias());
        }
        // set path expand hops
        return visitOC_RangeLiteral(ctx.oC_RelationshipDetail().oC_RangeLiteral());
    }

    @Override
    public PathExpandConfig.Builder visitOC_NodePattern(CypherGSParser.OC_NodePatternContext ctx) {
        // set getV base in path_expand
        return builder.getV(Utils.getVConfig(ctx));
    }

    @Override
    public PathExpandConfig.Builder visitOC_Properties(CypherGSParser.OC_PropertiesContext ctx) {
        return (ctx == null)
                ? builder
                : builder.filter(
                        Utils.propertyFilters(
                                this.builder.getInnerBuilder(),
                                this.parent.getExpressionVisitor(),
                                ctx));
    }

    @Override
    public PathExpandConfig.Builder visitOC_RangeLiteral(
            CypherGSParser.OC_RangeLiteralContext ctx) {
        if (ctx != null && ctx.oC_IntegerLiteral().size() > 1) {
            int val1 = Integer.valueOf(ctx.oC_IntegerLiteral(0).getText());
            int val2 = Integer.valueOf(ctx.oC_IntegerLiteral(1).getText());
            return builder.range(val1, val2 - val1);
        } else {
            return builder;
        }
    }
}

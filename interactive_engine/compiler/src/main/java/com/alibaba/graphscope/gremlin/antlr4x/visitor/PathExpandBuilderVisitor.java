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

import com.alibaba.graphscope.common.ir.tools.config.*;
import com.alibaba.graphscope.grammar.GremlinGSBaseVisitor;
import com.alibaba.graphscope.grammar.GremlinGSParser;
import com.alibaba.graphscope.gremlin.Utils;
import com.alibaba.graphscope.gremlin.antlr4.GenericLiteralVisitor;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Objects;

public class PathExpandBuilderVisitor extends GremlinGSBaseVisitor<PathExpandConfig.Builder> {
    private final GraphBuilderVisitor parent;
    private final PathExpandConfig.Builder builder;

    public PathExpandBuilderVisitor(GraphBuilderVisitor parent) {
        this.parent = Objects.requireNonNull(parent);
        // PATH_OPT = ARBITRARY and RESULT_OPT = END_V are set by default
        this.builder = PathExpandConfig.newBuilder(parent.getGraphBuilder());
    }

    @Override
    public PathExpandConfig.Builder visitTraversalMethod_out(
            GremlinGSParser.TraversalMethod_outContext ctx) {
        String[] labels = GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList());
        Preconditions.checkArgument(
                labels != null && labels.length > 0, "arguments can not be empty in path expand");
        String[] ranges = labels[0].split("\\.\\.");
        int lower = Integer.valueOf(ranges[0]);
        int upper = Integer.valueOf(ranges[1]);
        // set path_opt and result_opt
        List<GremlinGSParser.TraversalMethod_withContext> nextWith =
                getNextWith((GremlinGSParser.TraversalMethodContext) ctx.getParent());
        nextWith.forEach(k -> visitTraversalMethod_with(k));
        // set expand config, getV config, range, alias
        String alias =
                parent.getNextTag(
                        new TraversalMethodIterator(
                                (GremlinGSParser.TraversalMethodContext) ctx.getParent()));
        return builder.expand(new ExpandConfig(GraphOpt.Expand.OUT, getExpandLabelConfig(labels)))
                .getV(new GetVConfig(GraphOpt.GetV.END))
                .range(lower, upper - lower)
                .alias(alias);
    }

    @Override
    public PathExpandConfig.Builder visitTraversalMethod_in(
            GremlinGSParser.TraversalMethod_inContext ctx) {
        String[] labels = GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList());
        Preconditions.checkArgument(
                labels != null && labels.length > 0, "arguments can not be empty in path expand");
        String[] ranges = labels[0].split("\\.\\.");
        int lower = Integer.valueOf(ranges[0]);
        int upper = Integer.valueOf(ranges[1]);
        // set path_opt and result_opt
        List<GremlinGSParser.TraversalMethod_withContext> nextWith =
                getNextWith((GremlinGSParser.TraversalMethodContext) ctx.getParent());
        nextWith.forEach(k -> visitTraversalMethod_with(k));
        // set expand config, getV config, range, alias
        String alias =
                parent.getNextTag(
                        new TraversalMethodIterator(
                                (GremlinGSParser.TraversalMethodContext) ctx.getParent()));
        return builder.expand(new ExpandConfig(GraphOpt.Expand.IN, getExpandLabelConfig(labels)))
                .getV(new GetVConfig(GraphOpt.GetV.START))
                .range(lower, upper - lower)
                .alias(alias);
    }

    @Override
    public PathExpandConfig.Builder visitTraversalMethod_both(
            GremlinGSParser.TraversalMethod_bothContext ctx) {
        String[] labels = GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList());
        Preconditions.checkArgument(
                labels != null && labels.length > 0, "arguments can not be empty in path expand");
        String[] ranges = labels[0].split("\\.\\.");
        int lower = Integer.valueOf(ranges[0]);
        int upper = Integer.valueOf(ranges[1]);
        // set path_opt and result_opt
        List<GremlinGSParser.TraversalMethod_withContext> nextWith =
                getNextWith((GremlinGSParser.TraversalMethodContext) ctx.getParent());
        nextWith.forEach(k -> visitTraversalMethod_with(k));
        // set expand config, getV config, range, alias
        String alias =
                parent.getNextTag(
                        new TraversalMethodIterator(
                                (GremlinGSParser.TraversalMethodContext) ctx.getParent()));
        return builder.expand(new ExpandConfig(GraphOpt.Expand.BOTH, getExpandLabelConfig(labels)))
                .getV(new GetVConfig(GraphOpt.GetV.OTHER))
                .range(lower, upper - lower)
                .alias(alias);
    }

    @Override
    public PathExpandConfig.Builder visitTraversalMethod_with(
            GremlinGSParser.TraversalMethod_withContext ctx) {
        String optKey = GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral());
        Object optValue =
                GenericLiteralVisitor.getInstance().visitGenericLiteral(ctx.genericLiteral());
        switch (optKey.toUpperCase()) {
            case "PATH_OPT":
                return builder.pathOpt(
                        GraphOpt.PathExpandPath.valueOf(String.valueOf(optValue).toUpperCase()));
            case "RESULT_OPT":
                return builder.resultOpt(
                        GraphOpt.PathExpandResult.valueOf(String.valueOf(optValue).toUpperCase()));
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

    private LabelConfig getExpandLabelConfig(String[] labels) {
        if (labels.length <= 1) {
            return new LabelConfig(true);
        } else {
            labels = Utils.removeStringEle(0, labels);
            LabelConfig expandLabels = new LabelConfig(false);
            for (int i = 0; i < labels.length; ++i) {
                expandLabels.addLabel(labels[i]);
            }
            return expandLabels;
        }
    }
}

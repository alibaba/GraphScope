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

package com.alibaba.graphscope.gremlin.antlr4;

import com.alibaba.graphscope.grammar.GremlinGSBaseVisitor;
import com.alibaba.graphscope.grammar.GremlinGSParser;
import com.alibaba.graphscope.gremlin.plugin.traversal.IrCustomizedTraversalSource;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.util.List;

public class GraphTraversalSourceVisitor extends GremlinGSBaseVisitor<GraphTraversalSource> {
    private GraphTraversalSource g;

    public GraphTraversalSourceVisitor(GraphTraversalSource g) {
        this.g = g;
    }

    @Override
    public GraphTraversalSource visitTraversalSource(GremlinGSParser.TraversalSourceContext ctx) {
        List<GremlinGSParser.TraversalMethod_withContext> withCtxList = ctx.traversalMethod_with();
        if (ObjectUtils.isNotEmpty(withCtxList)) {
            withCtxList.forEach(k -> visitTraversalMethod_with(k));
        }
        return g;
    }

    @Override
    public GraphTraversalSource visitTraversalMethod_with(
            GremlinGSParser.TraversalMethod_withContext ctx) {
        if (ctx.stringLiteral() != null
                && ctx.genericLiteral() != null
                && g instanceof IrCustomizedTraversalSource) {
            IrCustomizedTraversalSource customSource = (IrCustomizedTraversalSource) g;
            String key = GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral());
            Object value =
                    GenericLiteralVisitor.getInstance().visitGenericLiteral(ctx.genericLiteral());
            customSource.addConfig(key, value);
        }
        return g;
    }
}

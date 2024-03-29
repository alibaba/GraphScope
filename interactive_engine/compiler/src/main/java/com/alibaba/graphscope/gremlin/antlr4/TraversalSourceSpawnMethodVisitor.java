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
import com.alibaba.graphscope.gremlin.antlr4x.visitor.LiteralList;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.util.List;

public class TraversalSourceSpawnMethodVisitor extends GremlinGSBaseVisitor<GraphTraversal> {
    final GraphTraversalSource g;

    public TraversalSourceSpawnMethodVisitor(final GraphTraversalSource g) {
        this.g = g;
    }

    @Override
    public GraphTraversal visitTraversalSourceSpawnMethod(
            GremlinGSParser.TraversalSourceSpawnMethodContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public GraphTraversal visitTraversalSourceSpawnMethod_V(
            GremlinGSParser.TraversalSourceSpawnMethod_VContext ctx) {
        List<Number> ids =
                new LiteralList(ctx.oC_ListLiteral(), ctx.oC_Expression()).toList(Number.class);
        if (!ids.isEmpty()) {
            return g.V(ids.toArray(new Number[0]));
        } else {
            return g.V();
        }
    }

    @Override
    public GraphTraversal visitTraversalSourceSpawnMethod_E(
            GremlinGSParser.TraversalSourceSpawnMethod_EContext ctx) {
        List<Number> ids =
                new LiteralList(ctx.oC_ListLiteral(), ctx.oC_Expression()).toList(Number.class);
        if (!ids.isEmpty()) {
            return g.E(ids.toArray(new Number[0]));
        } else {
            return g.E();
        }
    }
}

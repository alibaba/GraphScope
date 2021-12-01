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

import com.alibaba.graphscope.gremlin.exception.UnsupportedEvalException;
import org.apache.tinkerpop.gremlin.language.grammar.GremlinGS_0_2BaseVisitor;
import org.apache.tinkerpop.gremlin.language.grammar.GremlinGS_0_2Parser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

public class TraversalSourceSpawnMethodVisitor extends GremlinGS_0_2BaseVisitor<GraphTraversal> {
    final GraphTraversalSource g;

    public TraversalSourceSpawnMethodVisitor(final GraphTraversalSource g) {
        this.g = g;
    }

    @Override
    public GraphTraversal visitTraversalSourceSpawnMethod(GremlinGS_0_2Parser.TraversalSourceSpawnMethodContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public GraphTraversal visitTraversalSourceSpawnMethod_V(GremlinGS_0_2Parser.TraversalSourceSpawnMethod_VContext ctx) {
        if (ctx.getChildCount() != 3) {
            throw new UnsupportedEvalException(ctx.getClass(), "supported pattern is [g.V()]");
        }
        return g.V();
    }

    @Override
    public GraphTraversal visitTraversalSourceSpawnMethod_E(GremlinGS_0_2Parser.TraversalSourceSpawnMethod_EContext ctx) {
        if (ctx.getChildCount() != 3) {
            throw new UnsupportedEvalException(ctx.getClass(), "supported pattern is [g.E()]");
        }
        return g.E();
    }
}

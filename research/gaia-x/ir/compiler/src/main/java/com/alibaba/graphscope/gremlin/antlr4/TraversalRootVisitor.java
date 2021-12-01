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
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.tinkerpop.gremlin.language.grammar.GremlinGS_0_2BaseVisitor;
import org.apache.tinkerpop.gremlin.language.grammar.GremlinGS_0_2Parser;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

public class TraversalRootVisitor<G extends Traversal> extends GremlinGS_0_2BaseVisitor<Traversal> {
    final GremlinGS_0_2BaseVisitor<GraphTraversalSource> gvisitor;

    public TraversalRootVisitor(final GremlinGS_0_2BaseVisitor<GraphTraversalSource> gvisitor) {
        this.gvisitor = gvisitor;
    }

    @Override
    public Traversal visitRootTraversal(final GremlinGS_0_2Parser.RootTraversalContext ctx) {
        int childCount = ctx.getChildCount();
        String notice = "supported pattern of root is [g.V()] or [g.V().{...}]";
        if (childCount != 3 && childCount != 5) {
            throw new UnsupportedEvalException(ctx.getClass(), notice);
        }
        ParseTree first = ctx.getChild(0);
        GraphTraversalSource g = this.gvisitor.visitTraversalSource((GremlinGS_0_2Parser.TraversalSourceContext) first);
        ParseTree third = ctx.getChild(2);
        GraphTraversal graphTraversal = (new TraversalSourceSpawnMethodVisitor(g)).visitTraversalSourceSpawnMethod(
                (GremlinGS_0_2Parser.TraversalSourceSpawnMethodContext) third);
        if (childCount == 5) {
            ParseTree forth = ctx.getChild(4);
            TraversalMethodVisitor methodVisitor = new TraversalMethodVisitor(this.gvisitor, graphTraversal);
            return methodVisitor.visitChainedTraversal((GremlinGS_0_2Parser.ChainedTraversalContext) forth);
        } else {
            return graphTraversal;
        }
    }

    @Override
    public Traversal visitChainedTraversal(GremlinGS_0_2Parser.ChainedTraversalContext ctx) {
        int childCount = ctx.getChildCount();
        String notice = "supported pattern of chained is [..out()] or [..{...}.out()]";
        if (childCount != 1 && childCount != 3) {
            throw new UnsupportedEvalException(ctx.getClass(), notice);
        }
        if (childCount == 1) {
            return visitChildren(ctx);
        } else {
            visit(ctx.getChild(0));
            return visit(ctx.getChild(2));
        }
    }
}

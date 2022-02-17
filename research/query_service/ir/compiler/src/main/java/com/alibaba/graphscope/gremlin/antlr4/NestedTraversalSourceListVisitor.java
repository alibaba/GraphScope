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

import org.apache.tinkerpop.gremlin.language.grammar.GremlinGSBaseVisitor;
import org.apache.tinkerpop.gremlin.language.grammar.GremlinGSParser;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

/**
 * This class implements Gremlin grammar's nested-traversal-list methods that returns a {@link Traversal} {@code []}
 * to the callers.
 */
public class NestedTraversalSourceListVisitor extends GremlinGSBaseVisitor<Traversal[]> {

    protected final GremlinGSBaseVisitor<GraphTraversal> tvisitor;

    public NestedTraversalSourceListVisitor(final GremlinGSBaseVisitor<GraphTraversal> tvisitor) {
        this.tvisitor = tvisitor;
    }

    @Override
    public Traversal[] visitNestedTraversalExpr(final GremlinGSParser.NestedTraversalExprContext ctx) {
        final int childCount = ctx.getChildCount();

        // handle arbitrary number of traversals that are separated by comma
        final Traversal[] results = new Traversal[(childCount + 1) / 2];
        int childIndex = 0;
        while (childIndex < ctx.getChildCount()) {
            results[childIndex / 2] = tvisitor.visitNestedTraversal(
                    (GremlinGSParser.NestedTraversalContext) ctx.getChild(childIndex));
            // skip comma child
            childIndex += 2;
        }

        return results;
    }
}
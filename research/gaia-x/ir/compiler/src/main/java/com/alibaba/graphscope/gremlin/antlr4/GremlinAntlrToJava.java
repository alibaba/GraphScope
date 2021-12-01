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
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

public class GremlinAntlrToJava extends GremlinGS_0_2BaseVisitor<Object> {
    final GraphTraversalSource g;

    final GremlinGS_0_2BaseVisitor<GraphTraversalSource> gvisitor;
    final GremlinGS_0_2BaseVisitor<GraphTraversal> tvisitor;

    private static GremlinAntlrToJava instance;

    public static GremlinAntlrToJava getInstance(GraphTraversalSource g) {
        if (instance == null) {
            instance = new GremlinAntlrToJava(g);
        }
        return instance;
    }

    private GremlinAntlrToJava(GraphTraversalSource g) {
        this.g = g;
        this.gvisitor = new GraphTraversalSourceVisitor(this.g);
        this.tvisitor = new TraversalRootVisitor(this.gvisitor);
    }

    @Override
    public Object visitQuery(GremlinGS_0_2Parser.QueryContext ctx) {
        final int childCount = ctx.getChildCount();
        String notice = "supported pattern of query is [g] or [g.V()...]";
        if (childCount != 1) {
            throw new UnsupportedEvalException(ctx.getClass(), notice);
        }
        final ParseTree firstChild = ctx.getChild(0);

        if (firstChild instanceof GremlinGS_0_2Parser.TraversalSourceContext) {
            return this.gvisitor.visitTraversalSource((GremlinGS_0_2Parser.TraversalSourceContext) firstChild);
        } else if (firstChild instanceof GremlinGS_0_2Parser.RootTraversalContext) {
            return this.tvisitor.visitRootTraversal((GremlinGS_0_2Parser.RootTraversalContext) firstChild);
        } else {
            throw new UnsupportedEvalException(firstChild.getClass(), notice);
        }
    }
}

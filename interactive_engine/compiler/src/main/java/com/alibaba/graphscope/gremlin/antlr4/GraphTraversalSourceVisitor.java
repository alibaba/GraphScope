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
import com.alibaba.graphscope.gremlin.exception.UnsupportedEvalException;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

public class GraphTraversalSourceVisitor extends GremlinGSBaseVisitor<GraphTraversalSource> {
    private GraphTraversalSource g;

    public GraphTraversalSourceVisitor(GraphTraversalSource g) {
        this.g = g;
    }

    @Override
    public GraphTraversalSource visitTraversalSource(GremlinGSParser.TraversalSourceContext ctx) {
        if (ctx.getChildCount() != 1) {
            throw new UnsupportedEvalException(
                    ctx.getClass(), "supported pattern of source is [g]");
        }
        return g;
    }
}

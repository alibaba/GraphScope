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

package com.alibaba.graphscope.gremlin.plugin.traversal;

import com.alibaba.graphscope.gremlin.plugin.step.ExprStep;
import com.alibaba.graphscope.gremlin.plugin.step.PathExpandStep;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class IrCustomizedTraversal<S, E> extends DefaultTraversal<S, E> implements GraphTraversal.Admin<S, E> {
    public IrCustomizedTraversal() {
        super();
    }

    public IrCustomizedTraversal(final GraphTraversalSource graphTraversalSource) {
        super(graphTraversalSource);
    }

    public IrCustomizedTraversal(final Graph graph) {
        super(graph);
    }

    @Override
    public GraphTraversal.Admin<S, E> asAdmin() {
        return this;
    }

    @Override
    public GraphTraversal<S, E> iterate() {
        return GraphTraversal.Admin.super.iterate();
    }

    @Override
    public IrCustomizedTraversal<S, E> clone() {
        return (IrCustomizedTraversal<S, E>) super.clone();
    }

    public GraphTraversal<S, Vertex> out(Traversal rangeTraversal, String... labels) {
        this.asAdmin().getBytecode().addStep("flatMap", rangeTraversal, labels);
        return this.asAdmin().addStep(new PathExpandStep(this.asAdmin(), Direction.OUT, rangeTraversal, labels));
    }

    public GraphTraversal<S, Vertex> in(Traversal rangeTraversal, String... labels) {
        this.asAdmin().getBytecode().addStep("flatMap", rangeTraversal, labels);
        return this.asAdmin().addStep(new PathExpandStep(this.asAdmin(), Direction.IN, rangeTraversal, labels));
    }

    public GraphTraversal<S, Vertex> both(Traversal rangeTraversal, String... labels) {
        this.asAdmin().getBytecode().addStep("flatMap", rangeTraversal, labels);
        return this.asAdmin().addStep(new PathExpandStep(this.asAdmin(), Direction.BOTH, rangeTraversal, labels));
    }

    public GraphTraversal<S, Vertex> expr(String expr) {
        this.asAdmin().getBytecode().addStep("expr", expr);
        return this.asAdmin().addStep(new ExprStep(this.asAdmin(), expr));
    }
}

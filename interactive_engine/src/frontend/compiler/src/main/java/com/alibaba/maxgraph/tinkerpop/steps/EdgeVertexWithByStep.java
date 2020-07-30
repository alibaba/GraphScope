/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.tinkerpop.steps;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.ByModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FlatMapStep;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;
import java.util.function.Function;

public class EdgeVertexWithByStep extends EdgeVertexStep implements ByModulating {
    private static final long serialVersionUID = -6950750978535566434L;
    private Function function = null;

    public EdgeVertexWithByStep(final Traversal.Admin traversal, final Direction direction) {
        super(traversal, direction);
    }

    @Override
    protected Iterator<Vertex> flatMap(Traverser.Admin<Edge> traverser) {
        return super.flatMap(traverser);
    }

    @Override
    public void addLabel(final String label) {
        super.addLabel(label);
    }

    public EdgeVertexStep getEdgeVertexStep() {
        return this;
    }

    public Function getFunction() {
        return function;
    }

    public void modulateBy(final Function function) throws UnsupportedOperationException {
        this.function = function;
    }

}

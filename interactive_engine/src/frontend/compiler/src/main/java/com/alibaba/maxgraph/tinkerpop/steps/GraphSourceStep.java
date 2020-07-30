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

import com.alibaba.maxgraph.sdkcommon.compiler.custom.graph.MaxGraphSource;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FlatMapStep;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;

public class GraphSourceStep extends FlatMapStep<Vertex, Element> {
    private MaxGraphSource graphSource;

    public GraphSourceStep(Traversal.Admin traversal, MaxGraphSource graphSource) {
        super(traversal);
        this.graphSource = graphSource;
    }

    @Override
    protected Iterator<Element> flatMap(Traverser.Admin<Vertex> traverser) {
        throw new UnsupportedOperationException("flatMap");
    }

    public MaxGraphSource getGraphSource() {
        return graphSource;
    }
}

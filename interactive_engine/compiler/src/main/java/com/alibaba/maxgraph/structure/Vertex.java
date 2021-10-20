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
package com.alibaba.maxgraph.structure;

import java.util.Iterator;
import java.util.Map;

import com.alibaba.maxgraph.sdkcommon.graph.ElementId;
import com.alibaba.maxgraph.structure.graph.MaxGraph;

import com.google.common.collect.Iterators;
import org.apache.tinkerpop.gremlin.structure.Direction;

import static com.google.common.base.Preconditions.checkNotNull;

public class Vertex extends AbstractElement {

    public Vertex(ElementId id, String label, Map<String, Object> properties, MaxGraph graph) {
        super(id, label, properties, checkNotNull(graph));
    }

    public Iterator<Edge> getOutEdges(String... label) {
        return this.graph.getEdges(this, Direction.OUT, label);
    }

    public Iterator<Edge> getInEdges(String... label) {
        return this.graph.getEdges(this, Direction.IN, label);
    }

    public Iterator<Edge> getEdges(String... label) {
        return Iterators.concat(getOutEdges(label), getInEdges(label));
    }

    public Edge addEdgeTo(Vertex dst, String label, Map<String, Object> pros) {
        return this.graph.addEdge(label, this, dst, pros);
    }

    @Override
    public void addProperty(String key, Object value) {
        super.addProperty(key, value);
        super.graph.addVertex(super.label, super.getProperties());
    }

}

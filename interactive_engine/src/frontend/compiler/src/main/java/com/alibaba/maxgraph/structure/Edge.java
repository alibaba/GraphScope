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

import com.alibaba.maxgraph.sdkcommon.graph.ElementId;
import com.alibaba.maxgraph.structure.graph.MaxGraph;

import java.util.Map;

public class Edge extends AbstractElement {

    private final Vertex src;
    private final Vertex dst;

    public Edge(ElementId id, String label, Map<String, Object> properties, Vertex src, Vertex dst, MaxGraph graph) {
        super(id, label, properties, graph);
        this.src = src;
        this.dst = dst;
    }

    public Vertex getSrcVertex() {
        return src;
    }

    public Vertex getDstVertex() {
        return dst;
    }

    @Override
    public void addProperty(String key, Object value) {
        super.addProperty(key, value);
        super.graph.updateEdge(getSrcVertex(), getDstVertex(), super.label, super.id.id(), super.getProperties());
    }
}

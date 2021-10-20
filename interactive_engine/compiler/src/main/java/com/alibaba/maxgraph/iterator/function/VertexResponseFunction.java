/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.iterator.function;

import com.alibaba.maxgraph.compiler.api.schema.GraphElement;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.proto.GremlinQuery;
import com.alibaba.maxgraph.sdkcommon.graph.CompositeId;
import com.alibaba.maxgraph.server.query.RpcProcessorUtils;
import com.alibaba.maxgraph.structure.Vertex;
import com.alibaba.maxgraph.structure.graph.MaxGraph;

import java.util.Map;
import java.util.function.Function;

/** Convert VertexResponse to gremlin vertex */
public class VertexResponseFunction implements Function<GremlinQuery.VertexResponse, Vertex> {
    private GraphSchema schema;
    private MaxGraph graph;

    public VertexResponseFunction(GraphSchema schema, MaxGraph graph) {
        this.schema = schema;
        this.graph = graph;
    }

    @Override
    public Vertex apply(GremlinQuery.VertexResponse nextV) {
        GremlinQuery.VertexId id = nextV.getId();
        CompositeId rId = new CompositeId(id.getId(), id.getTypeId());
        GraphElement type = schema.getElement(id.getTypeId());
        Map<String, Object> properties =
                RpcProcessorUtils.deserializeProperty(nextV.getPros().toByteArray(), type, schema);

        return new Vertex(rId, type.getLabel(), properties, this.graph);
    }
}

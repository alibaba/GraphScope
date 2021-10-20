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
import com.alibaba.maxgraph.proto.StoreApi;
import com.alibaba.maxgraph.sdkcommon.graph.CompositeId;
import com.alibaba.maxgraph.server.query.RpcProcessorUtils;
import com.alibaba.maxgraph.structure.Edge;
import com.alibaba.maxgraph.structure.Vertex;
import com.alibaba.maxgraph.structure.graph.MaxGraph;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkNotNull;

public class EdgeResponseFunction implements Function<StoreApi.GraphEdgeReponse, Edge> {
    private GraphSchema schema;
    private MaxGraph graph;

    public EdgeResponseFunction(GraphSchema schema, MaxGraph graph) {
        this.schema = schema;
        this.graph = graph;
    }

    @Override
    public Edge apply(StoreApi.GraphEdgeReponse edgeReponse) {
        CompositeId eid = new CompositeId(edgeReponse.getEdgeId(), edgeReponse.getTypeId());
        GraphElement element = schema.getElement(eid.typeId());
        String label = element.getLabel();
        Map<String, Object> properties =
                RpcProcessorUtils.deserializeProperty(
                        edgeReponse.getPros().toByteArray(), element, schema);
        GremlinQuery.VertexId srcId = edgeReponse.getSrcId();
        GremlinQuery.VertexId dstId = edgeReponse.getDstId();
        Iterator<Vertex> vertexIterator =
                graph.getVertex(
                        Sets.newHashSet(
                                new CompositeId(srcId.getId(), srcId.getTypeId()),
                                new CompositeId(dstId.getId(), dstId.getTypeId())));
        Vertex srcVertex = null, dstVertex = null;
        while (vertexIterator.hasNext()) {
            Vertex vertex = vertexIterator.next();
            if (vertex.id.id() == srcId.getId()) {
                srcVertex = vertex;
            }
            if (vertex.id.id() == dstId.getId()) {
                dstVertex = vertex;
            }
        }
        if (null == srcVertex) {
            try {
                GraphElement graphElement = schema.getElement(srcId.getTypeId());
                srcVertex =
                        new Vertex(
                                new CompositeId(srcId.getId(), srcId.getTypeId()),
                                graphElement.getLabel(),
                                Maps.newHashMap(),
                                graph);
            } catch (Exception ignored) {
                srcVertex =
                        new Vertex(
                                new CompositeId(srcId.getId(), srcId.getTypeId()),
                                "",
                                Maps.newHashMap(),
                                graph);
            }
        }
        if (null == dstVertex) {
            try {
                GraphElement graphElement = schema.getElement(dstId.getTypeId());
                dstVertex =
                        new Vertex(
                                new CompositeId(dstId.getId(), dstId.getTypeId()),
                                graphElement.getLabel(),
                                Maps.newHashMap(),
                                graph);
            } catch (Exception ignored) {
                dstVertex =
                        new Vertex(
                                new CompositeId(dstId.getId(), dstId.getTypeId()),
                                "",
                                Maps.newHashMap(),
                                graph);
            }
        }

        return new Edge(eid, label, properties, srcVertex, dstVertex, this.graph);
    }
}

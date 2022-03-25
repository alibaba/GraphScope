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

package com.alibaba.graphscope.gremlin.integration.result;

import com.alibaba.graphscope.common.client.ResultParser;
import com.alibaba.graphscope.gremlin.result.GremlinResultProcessor;
import com.google.common.collect.ImmutableMap;

import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.process.remote.traversal.DefaultRemoteTraverser;
import org.apache.tinkerpop.gremlin.server.Context;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class GremlinTestResultProcessor extends GremlinResultProcessor {
    private static Logger logger = LoggerFactory.getLogger(GremlinTestResultProcessor.class);
    private Map<String, Object> cachedProperties;
    private static String VERTEX_PROPERTIES = "vertex_properties";
    private static String EDGE_PROPERTIES = "edge_properties";

    public GremlinTestResultProcessor(
            Context writeResult, ResultParser resultParser, GraphProperties testGraph) {
        super(writeResult, resultParser);
        this.cachedProperties = testGraph.getProperties();
    }

    @Override
    public void finish() {
        synchronized (this) {
            if (!locked) {
                logger.debug("process finish");
                formatResultIfNeed();
                writeResultList(writeResult, resultCollectors, ResponseStatusCode.SUCCESS);
                locked = true;
            }
        }
    }

    @Override
    protected void formatResultIfNeed() {
        super.formatResultIfNeed();
        List<Object> testTraversers =
                resultCollectors.stream()
                        .map(
                                k -> {
                                    if (k instanceof DetachedVertex) {
                                        DetachedVertex vertex = (DetachedVertex) k;
                                        return new DetachedVertex(
                                                vertex.id(),
                                                vertex.label(),
                                                getVertexProperties(vertex));
                                    } else if (k instanceof DetachedEdge) {
                                        DetachedEdge edge = (DetachedEdge) k;
                                        Vertex outVertex = edge.outVertex();
                                        Vertex inVertex = edge.inVertex();
                                        return new DetachedEdge(
                                                edge.id(),
                                                edge.label(),
                                                getEdgeProperties(edge),
                                                outVertex.id(),
                                                outVertex.label(),
                                                inVertex.id(),
                                                inVertex.label());
                                    } else {
                                        return k;
                                    }
                                })
                        .map(k -> new DefaultRemoteTraverser(k, 1))
                        .collect(Collectors.toList());
        resultCollectors.clear();
        resultCollectors.addAll(testTraversers);
    }

    private Map<String, Object> getVertexProperties(Vertex vertex) {
        Map<String, Object> vertexProperties =
                (Map<String, Object>) cachedProperties.get(VERTEX_PROPERTIES);
        String idAsStr = String.valueOf(vertex.id());
        Map<String, Object> properties = (Map<String, Object>) vertexProperties.get(idAsStr);
        if (properties != null) {
            Map<String, Object> formatProperties = new HashMap<>();
            properties.forEach(
                    (k, v) -> {
                        formatProperties.put(
                                k,
                                Collections.singletonList(ImmutableMap.of("id", 1L, "value", v)));
                    });
            return formatProperties;
        } else {
            return Collections.emptyMap();
        }
    }

    private Map<String, Object> getEdgeProperties(Edge edge) {
        Map<String, Object> edgeProperties =
                (Map<String, Object>) cachedProperties.get(EDGE_PROPERTIES);
        String idAsStr = String.valueOf(edge.id());
        Map<String, Object> properties = (Map<String, Object>) edgeProperties.get(idAsStr);
        return (properties == null) ? Collections.emptyMap() : properties;
    }
}

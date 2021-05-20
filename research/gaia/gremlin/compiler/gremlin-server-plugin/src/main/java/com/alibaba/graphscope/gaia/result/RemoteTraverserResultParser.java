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
package com.alibaba.graphscope.gaia.result;

import com.alibaba.graphscope.common.proto.GremlinResult;
import com.alibaba.graphscope.gaia.plan.translator.builder.ConfigBuilder;
import com.alibaba.graphscope.gaia.store.GraphStoreService;
import com.alibaba.graphscope.gaia.store.StaticGraphStore;
import com.google.common.collect.ImmutableMap;
import org.apache.tinkerpop.gremlin.process.remote.traversal.DefaultRemoteTraverser;
import org.apache.tinkerpop.gremlin.process.remote.traversal.RemoteTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MutablePath;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.*;

public class RemoteTraverserResultParser extends DefaultResultParser {
    private static final Logger logger = LoggerFactory.getLogger(RemoteTraverserResultParser.class);

    public RemoteTraverserResultParser(ConfigBuilder builder) {
        super(builder);
    }

    private final GraphStoreService graphStore = StaticGraphStore.INSTANCE;

    @Override
    public List<Object> parseFrom(GremlinResult.Result resultData) {
        List<Object> result = new ArrayList<>();
        if (resultData.getInnerCase() == GremlinResult.Result.InnerCase.PATHS) {
            resultData.getPaths().getItemList().forEach(p -> {
                result.add(transform(parsePath(p)));
            });
        } else if (resultData.getInnerCase() == GremlinResult.Result.InnerCase.ELEMENTS) {
            resultData.getElements().getItemList().forEach(e -> {
                result.add(transform(parseElement(e)));
            });
        } else if (resultData.getInnerCase() == GremlinResult.Result.InnerCase.TAG_ENTRIES) {
            resultData.getTagEntries().getItemList().forEach(e -> {
                result.add(transform(parseTagPropertyValue(e)));
            });
        } else if (resultData.getInnerCase() == GremlinResult.Result.InnerCase.MAP_RESULT) {
            resultData.getMapResult().getItemList().forEach(e -> {
                Map entry = Collections.singletonMap(parsePairElement(e.getFirst()), parsePairElement(e.getSecond()));
                result.add(transform(entry));
            });
        } else if (resultData.getInnerCase() == GremlinResult.Result.InnerCase.VALUE) {
            result.add(transform(parseValue(resultData.getValue())));
        } else if (resultData.getInnerCase() == GremlinResult.Result.InnerCase.VALUE_LIST) {
            resultData.getValueList().getItemList().forEach(k -> {
                result.add(transform(parseValue(k)));
            });
        } else {
            throw new UnsupportedOperationException("");
        }
        return result;
    }

    @Override
    protected Object parseElement(GremlinResult.GraphElement elementPB) {
        if (elementPB.getInnerCase() == GremlinResult.GraphElement.InnerCase.EDGE) {
            GremlinResult.Edge edge = elementPB.getEdge();
            String edgeLabelName = graphStore.getLabel(Long.valueOf(edge.getLabel()));
            return new DetachedEdge(extractEdgeId(edge), edgeLabelName, extractProperties(edge),
                    edge.getSrcId(), extractVertexLabel(edge.getSrcId()),
                    edge.getDstId(), extractVertexLabel(edge.getDstId()));
        }
        if (elementPB.getInnerCase() == GremlinResult.GraphElement.InnerCase.VERTEX) {
            GremlinResult.Vertex vertex = elementPB.getVertex();
            String vertexLabelName = graphStore.getLabel(Long.valueOf(vertex.getLabel()));
            return new DetachedVertex(vertex.getId(), vertexLabelName, extractProperties(vertex));
        }
        throw new RuntimeException("graph element type not set");
    }

    @Override
    protected Path parsePath(GremlinResult.Path pathPB) {
        Path path = MutablePath.make();
        pathPB.getPathList().forEach(p -> {
            path.extend(parseElement(p), Collections.EMPTY_SET);
        });
        return path;
    }

    private RemoteTraverser transform(Object object) {
        return new DefaultRemoteTraverser(object, 1);
    }

    private Map<String, Object> extractProperties(GremlinResult.Edge edge) {
        Map<String, Object> result = new HashMap<>();
        Set<String> keys = graphStore.getEdgeKeys(extractEdgeId(edge));
        for (String key : keys) {
            Optional propertyOpt = graphStore.getEdgeProperty(extractEdgeId(edge), key);
            if (propertyOpt.isPresent()) {
                result.put(key, propertyOpt.get());
            }
        }
        return result;
    }

    private Map<String, Object> extractProperties(GremlinResult.Vertex vertex) {
        Map<String, Object> result = new HashMap<>();
        Set<String> keys = graphStore.getVertexKeys(extractVertexId(vertex));
        for (String key : keys) {
            Optional propertyOpt = graphStore.getVertexProperty(extractVertexId(vertex), key);
            if (propertyOpt.isPresent()) {
                result.put(key, Collections.singletonList(ImmutableMap.of("id", 1L, "value", propertyOpt.get())));
            }
        }
        return result;
    }

    private String extractVertexLabel(long vertexId) {
        // pre 8 bits
        long labelId = (vertexId >> 56) & 0xff;
        return graphStore.getLabel(labelId);
    }

    private BigInteger extractEdgeId(GremlinResult.Edge edge) {
        return new BigInteger(edge.getId().toByteArray());
    }

    private BigInteger extractVertexId(GremlinResult.Vertex vertex) {
        return new BigInteger(String.valueOf(vertex.getId()));
    }
}

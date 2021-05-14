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

import java.util.*;

public class RemoteTraverserResultParser extends DefaultResultParser {
    private static final Logger logger = LoggerFactory.getLogger(RemoteTraverserResultParser.class);

    public RemoteTraverserResultParser(ConfigBuilder builder) {
        super(builder);
    }

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
                result.add(transform(entry.entrySet().iterator().next()));
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
            return new DetachedEdge(edge.getId(), edge.getLabel(), extractProperties(edge),
                    edge.getSrcId(), edge.getSrcLabel(), edge.getDstId(), edge.getDstLabel());
        }
        if (elementPB.getInnerCase() == GremlinResult.GraphElement.InnerCase.VERTEX) {
            GremlinResult.Vertex vertex = elementPB.getVertex();
            return new DetachedVertex(vertex.getId(), vertex.getLabel(), extractProperties(vertex));
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
        StaticGraphStore graphStore = StaticGraphStore.INSTANCE;
        Set<String> keys = graphStore.getEdgeKeys(edge.getId());
        for (String key : keys) {
            result.put(key, graphStore.getEdgeProperty(edge.getId(), key));
        }
        return result;
    }

    private Map<String, Object> extractProperties(GremlinResult.Vertex vertex) {
        Map<String, Object> result = new HashMap<>();
        StaticGraphStore graphStore = StaticGraphStore.INSTANCE;
        Set<String> keys = graphStore.getVertexKeys(vertex.getId());
        for (String key : keys) {
            result.put(key, Collections.singletonList(ImmutableMap.of("id", 1L,
                    "value", graphStore.getVertexProperty(vertex.getId(), key))));
        }
        return result;
    }
}

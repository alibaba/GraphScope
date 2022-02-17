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

package com.alibaba.graphscope.gremlin.result;

import com.alibaba.graphscope.gaia.proto.Common;
import com.alibaba.graphscope.gaia.proto.IrResult;
import com.alibaba.graphscope.gremlin.exception.GremlinResultParserException;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ParserUtils {
    private static final Logger logger = LoggerFactory.getLogger(ParserUtils.class);

    public static Object parseElement(IrResult.Element element) {
        switch (element.getInnerCase()) {
            case VERTEX:
                return parseVertex(element.getVertex());
            case EDGE:
                return parseEdge(element.getEdge());
            case GRAPH_PATH:
                IrResult.GraphPath graphPath = element.getGraphPath();
                return graphPath.getPathList().stream().map(k -> {
                    if (k.getInnerCase() == IrResult.GraphPath.VertexOrEdge.InnerCase.VERTEX) {
                        return parseVertex(k.getVertex());
                    } else if (k.getInnerCase() == IrResult.GraphPath.VertexOrEdge.InnerCase.EDGE) {
                        return parseEdge(k.getEdge());
                    } else {
                        throw new GremlinResultParserException(k.getInnerCase() + " is invalid");
                    }
                }).collect(Collectors.toList());
            case OBJECT:
                return parseCommonValue(element.getObject());
            default:
                throw new GremlinResultParserException(element.getInnerCase() + " is invalid");
        }
    }

    public static List<Object> parseCollection(IrResult.Collection collection) {
        return collection.getCollectionList().stream().map(k -> parseElement(k)).collect(Collectors.toList());
    }

    public static IrResult.Entry getHeadEntry(IrResult.Results results) {
        return results.getRecord().getColumns(0).getEntry();
    }

    private static Object parseCommonValue(Common.Value value) {
        switch (value.getItemCase()) {
            case BOOLEAN:
                return value.getBoolean();
            case I32:
                return value.getI32();
            case I64:
                return value.getI64();
            case F64:
                return value.getF64();
            case STR:
                return value.getStr();
            case PAIR_ARRAY:
                Common.PairArray pairs = value.getPairArray();
                Map pairInMap = new HashMap();
                pairs.getItemList().forEach(pair -> {
                    pairInMap.put(parseCommonValue(pair.getKey()), parseCommonValue(pair.getVal()));
                });
                return pairInMap;
            case STR_ARRAY:
                return value.getStrArray().getItemList();
            case NONE:
                return EmptyValue.INSTANCE;
            default:
                throw new GremlinResultParserException(value.getItemCase() + " is unsupported yet");

        }
    }

    private static Vertex parseVertex(IrResult.Vertex vertex) {
        Map<String, Object> properties = parseProperties(vertex.getPropertiesList());
        return new DetachedVertex(vertex.getId(), vertex.getLabel().getName(), properties);
    }

    private static Edge parseEdge(IrResult.Edge edge) {
        Map<String, Object> edgeProperties = parseProperties(edge.getPropertiesList());
        return new DetachedEdge(edge.getId(), edge.getLabel().getName(), edgeProperties,
                edge.getSrcId(), edge.getSrcLabel().getName(), edge.getDstId(), edge.getDstLabel().getName());
    }

    private static Map<String, Object> parseProperties(List<IrResult.Property> properties) {
        return new HashMap<>();
    }
}

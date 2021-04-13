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

import com.alibaba.graphscope.common.proto.Common;
import com.alibaba.graphscope.common.proto.GremlinResult;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ResultParser {
    private static final Logger logger = LoggerFactory.getLogger(ResultParser.class);

    protected static List<Object> parseFrom(GremlinResult.Result resultData) {
        List<Object> result = new ArrayList<>();
        if (resultData.getInnerCase() == GremlinResult.Result.InnerCase.PATHS) {
            resultData.getPaths().getItemList().forEach(p -> {
                result.add(parsePath(p));
            });
        } else if (resultData.getInnerCase() == GremlinResult.Result.InnerCase.ELEMENTS) {
            resultData.getElements().getItemList().forEach(e -> {
                result.add(parseElement(e));
            });
        } else if (resultData.getInnerCase() == GremlinResult.Result.InnerCase.TAG_PROPERTIES) {
            resultData.getTagProperties().getItemList().forEach(e -> {
                result.add(parseTagPropertyValue(e));
            });
        } else if (resultData.getInnerCase() == GremlinResult.Result.InnerCase.MAP_RESULT) {
            resultData.getMapResult().getItemList().forEach(e -> {
                result.add(Pair.of(parsePairElement(e.getFirst()), parsePairElement(e.getSecond())));
            });
        } else if (resultData.getInnerCase() == GremlinResult.Result.InnerCase.VALUE) {
            result.add(parseValue(resultData.getValue()));
        } else if (resultData.getInnerCase() == GremlinResult.Result.InnerCase.VALUE_LIST) {
            resultData.getValueList().getItemList().forEach(k -> {
                result.add(parseValue(k));
            });
        } else {
            throw new UnsupportedOperationException("");
        }
        return result;
    }

    protected static List<Long> parseElement(GremlinResult.GraphElement elementPB) {
        List<Long> element = new ArrayList<>();
        if (elementPB.getInnerCase() == GremlinResult.GraphElement.InnerCase.EDGE) {
            element.add(elementPB.getEdge().getSrcId());
            element.add(elementPB.getEdge().getDstId());
        } else if (elementPB.getInnerCase() == GremlinResult.GraphElement.InnerCase.VERTEX) {
            element.add(elementPB.getVertex().getId());
        } else {
            logger.error("graph element type not set");
        }
        return element;
    }

    protected static List<Object> parsePath(GremlinResult.Path pathPB) {
        List<Object> path = new ArrayList<>();
        pathPB.getPathList().forEach(p -> {
            path.add(parseElement(p));
        });
        return path;
    }

    protected static Map<Integer, Map<String, Object>> parseTagPropertyValue(GremlinResult.TagProperties tagProperties) {
        Map<Integer, Map<String, Object>> result = new HashMap<>();
        tagProperties.getItemList().forEach(iterm -> {
            Map<String, Object> properties = result.computeIfAbsent(iterm.getTag(), k -> new HashMap<>());
            iterm.getPropsList().forEach(p -> {
                properties.put(p.getKey(), parseValue(p.getValue()));
            });
        });
        return result;
    }

    protected static Object parsePairElement(GremlinResult.PairElement pairElement) {
        if (pairElement.getInnerCase() == GremlinResult.PairElement.InnerCase.GRAPH_ELEMENT) {
            return parseElement(pairElement.getGraphElement());
        } else if (pairElement.getInnerCase() == GremlinResult.PairElement.InnerCase.VALUE) {
            return parseValue(pairElement.getValue());
        } else if (pairElement.getInnerCase() == GremlinResult.PairElement.InnerCase.GRAPH_ELEMENT_LIST) {
            List<Object> result = new ArrayList();
            pairElement.getGraphElementList().getItemList().forEach(e -> {
                result.add(parseElement(e));
            });
            return result;
        } else if (pairElement.getInnerCase() == GremlinResult.PairElement.InnerCase.VALUE_LIST) {
            List<Object> result = new ArrayList();
            pairElement.getValueList().getItemList().forEach(e -> {
                result.add(parseValue(e));
            });
            return result;
        } else {
            throw new UnsupportedOperationException("parse pair element not support " + pairElement.getInnerCase());
        }
    }

    protected static Object parseValue(Common.Value value) {
        if (value.getItemCase() == Common.Value.ItemCase.BOOLEAN) {
            return value.getBoolean();
        } else if (value.getItemCase() == Common.Value.ItemCase.I32) {
            return value.getI32();
        } else if (value.getItemCase() == Common.Value.ItemCase.I64) {
            return value.getI64();
        } else if (value.getItemCase() == Common.Value.ItemCase.F64) {
            return value.getF64();
        } else if (value.getItemCase() == Common.Value.ItemCase.STR) {
            return value.getStr();
        } else if (value.getItemCase() == Common.Value.ItemCase.BLOB) {
            return value.getBlob();
        } else if (value.getItemCase() == Common.Value.ItemCase.I32_ARRAY) {
            return value.getI32Array();
        } else if (value.getItemCase() == Common.Value.ItemCase.F64_ARRAY) {
            return value.getF64Array();
        } else if (value.getItemCase() == Common.Value.ItemCase.I64_ARRAY) {
            return value.getI64Array();
        } else if (value.getItemCase() == Common.Value.ItemCase.STR_ARRAY) {
            return value.getStrArray();
        } else {
            throw new UnsupportedOperationException("parse value not support " + value.getItemCase());
        }
    }
}

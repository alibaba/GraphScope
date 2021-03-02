/**
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
package com.compiler.demo.server.result;

import com.alibaba.graphscope.common.proto.GremlinResult;
import com.alibaba.pegasus.service.proto.PegasusClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ResultParser {
    private static final Logger logger = LoggerFactory.getLogger(ResultParser.class);

    protected static List<Object> parseFrom(PegasusClient.JobResponse response) throws Exception {
        GremlinResult.Result resultPB = GremlinResult.Result.parseFrom(response.getData());
        List<Object> result = new ArrayList<>();
        if (resultPB.getInnerCase() == GremlinResult.Result.InnerCase.PATHS) {
            resultPB.getPaths().getItemList().forEach(p -> {
                result.add(parsePath(p));
            });
        } else if (resultPB.getInnerCase() == GremlinResult.Result.InnerCase.ELEMENTS) {
            resultPB.getElements().getItemList().forEach(e -> {
                result.add(parseElement(e));
            });
        } else if (resultPB.getInnerCase() == GremlinResult.Result.InnerCase.TAG_PROPERTIES) {
            resultPB.getTagProperties().getItemList().forEach(e -> {
                result.add(parseTagPropertyValue(e));
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

    protected static Map<String, Map<String, Object>> parseTagPropertyValue(GremlinResult.TagProperties tagProperties) {
        Map<String, Map<String, Object>> result = new HashMap<>();
        tagProperties.getItemList().forEach(iterm -> {
            Map<String, Object> properties = result.computeIfAbsent(iterm.getTag(), k -> new HashMap<>());
            iterm.getPropsList().forEach(p -> {
                properties.put(p.getKey(), p.getValue());
            });
        });
        return result;
    }
}

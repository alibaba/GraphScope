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
package com.alibaba.maxgraph.v2.common.frontend.result;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.AbstractTraverser;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ResultConvertUtils {
    /**
     * Convert QueryResult to gremlin structure
     *
     * @param result The given result
     * @return The gremlin structure
     */
    public static Object convertGremlinResult(Object result) {
        if (result instanceof QueryResult) {
            return ((QueryResult) result).convertToGremlinStructure();
        } else if (result instanceof List) {
            List<Object> list = (List<Object>) result;
            return list.stream().map(v -> convertGremlinResult(v)).collect(Collectors.toList());
        } else if (result instanceof Tree) {
            Tree<Object> map = (Tree<Object>) result;
            if (map.isEmpty()) {
                return map;
            }
            Tree<Object> resultTree = new Tree<>();
            for (Map.Entry<Object, Tree<Object>> entry : map.entrySet()) {
                resultTree.put(convertGremlinResult(entry.getKey()),
                        (Tree<Object>) convertGremlinResult(entry.getValue()));
            }
            return resultTree;
        } else if (result instanceof Map) {
            Map<Object, Object> map = (Map<Object, Object>) result;
            Map<Object, Object> resultMap = Maps.newHashMap();
            for (Map.Entry<Object, Object> entry : map.entrySet()) {
                resultMap.put(convertGremlinResult(entry.getKey()), convertGremlinResult(entry.getValue()));
            }
            return resultMap;
        } else if (result instanceof Element) {
            return DetachedFactory.detach(result, true);
        } else if (result instanceof AbstractTraverser) {
            AbstractTraverser traverser = (AbstractTraverser) result;
            traverser.set(convertGremlinResult(traverser.get()));
            return traverser;
        } else if (result instanceof String && StringUtils.startsWith((String) result, "~")) {
            return T.fromString((String) result);
        } else {
            return result;
        }
    }
}

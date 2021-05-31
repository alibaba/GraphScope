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

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MutablePath;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;

import java.util.List;
import java.util.Set;

/**
 * Path results in maxgraph
 */
public class PathResult implements QueryResult {
    private List<PathValueResult> pathValueResultList;

    public PathResult() {

    }

    public PathResult(List<PathValueResult> pathValueResultList) {
        this.pathValueResultList = pathValueResultList;
    }

    public List<PathValueResult> getPathValueResultList() {
        return pathValueResultList;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PathResult that = (PathResult) o;
        return Objects.equal(pathValueResultList, that.pathValueResultList);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(pathValueResultList);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("pathValueResultList", pathValueResultList)
                .toString();
    }

    @Override
    public Object convertToGremlinStructure() {
        Path path = MutablePath.make();
        for (PathValueResult result : this.pathValueResultList) {
            Object pathValue = result.getValue() instanceof QueryResult ? ((QueryResult) result.getValue()).convertToGremlinStructure() : result.getValue();
            Set<String> labelList = null == result.getLabelList() ? Sets.newHashSet() : result.getLabelList();
            path.extend(pathValue, labelList);
        }
        return DetachedFactory.detach(path, true);
    }
}

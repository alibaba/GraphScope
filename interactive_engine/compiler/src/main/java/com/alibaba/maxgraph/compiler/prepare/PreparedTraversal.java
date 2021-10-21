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
package com.alibaba.maxgraph.compiler.prepare;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

public class PreparedTraversal {
    private String prepareId;
    private GraphTraversal traversal;

    public PreparedTraversal(String prepareId, GraphTraversal traversal) {
        this.prepareId = prepareId;
        this.traversal = traversal;
    }

    public String getPrepareId() {
        return prepareId;
    }

    public GraphTraversal getTraversal() {
        return traversal;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PreparedTraversal that = (PreparedTraversal) o;
        return Objects.equal(prepareId, that.prepareId) &&
                Objects.equal(traversal, that.traversal);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(prepareId, traversal);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("prepareId", prepareId)
                .add("traversal", traversal)
                .toString();
    }
}

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
package com.alibaba.graphscope.groot.common.schema.impl;

import com.alibaba.graphscope.groot.common.schema.api.EdgeRelation;
import com.alibaba.graphscope.groot.common.schema.api.GraphVertex;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

public class DefaultEdgeRelation implements EdgeRelation {
    private final GraphVertex source;
    private final GraphVertex target;
    private final long tableId;

    public DefaultEdgeRelation(GraphVertex source, GraphVertex target) {
        this(source, target, -1L);
    }

    public DefaultEdgeRelation(GraphVertex source, GraphVertex target, long tableId) {
        this.source = source;
        this.target = target;
        this.tableId = tableId;
    }

    @Override
    public GraphVertex getSource() {
        return source;
    }

    @Override
    public GraphVertex getTarget() {
        return target;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("source", source)
                .add("target", target)
                .add("tableId", tableId)
                .toString();
    }

    @Override
    public long getTableId() {
        return this.tableId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DefaultEdgeRelation that = (DefaultEdgeRelation) o;
        return tableId == that.tableId
                && Objects.equal(source, that.source)
                && Objects.equal(target, that.target);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(source, target, tableId);
    }
}

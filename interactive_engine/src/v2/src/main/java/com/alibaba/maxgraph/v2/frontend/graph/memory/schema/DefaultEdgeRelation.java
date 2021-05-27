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
package com.alibaba.maxgraph.v2.frontend.graph.memory.schema;

import com.alibaba.maxgraph.v2.common.frontend.api.schema.EdgeRelation;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.VertexType;
import com.google.common.base.MoreObjects;

/**
 * Default edge relation
 */
public class DefaultEdgeRelation implements EdgeRelation {
    private VertexType source;
    private VertexType target;
    private long tableId;

    public DefaultEdgeRelation(VertexType source, VertexType target, long tableId) {
        this.source = source;
        this.target = target;
        this.tableId = tableId;
    }

    public DefaultEdgeRelation(VertexType source, VertexType target) {
        this(source, target, -1L);
    }

    @Override
    public VertexType getSource() {
        return source;
    }

    @Override
    public VertexType getTarget() {
        return target;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("source", this.getSource().getLabel())
                .add("target", this.getTarget().getLabel())
                .toString();
    }

    @Override
    public long getTableId() {
        return this.tableId;
    }
}

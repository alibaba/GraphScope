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
package com.alibaba.maxgraph.compiler.schema;

import com.alibaba.maxgraph.compiler.api.schema.EdgeRelation;
import com.alibaba.maxgraph.compiler.api.schema.GraphVertex;
import com.google.common.base.MoreObjects;

public class DefaultEdgeRelation implements EdgeRelation {
    private GraphVertex source;
    private GraphVertex target;
    private long tableId;

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
}

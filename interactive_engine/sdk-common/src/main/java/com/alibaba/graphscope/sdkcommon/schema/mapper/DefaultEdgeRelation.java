/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.sdkcommon.schema.mapper;

import com.alibaba.graphscope.compiler.api.schema.EdgeRelation;
import com.alibaba.graphscope.compiler.api.schema.GraphVertex;

/** Default edge relation */
public class DefaultEdgeRelation implements EdgeRelation {
    private final GraphVertex source;
    private final GraphVertex target;
    private final long tableId;

    public DefaultEdgeRelation(GraphVertex source, GraphVertex target, long tableId) {
        this.source = source;
        this.target = target;
        this.tableId = tableId;
    }

    public DefaultEdgeRelation(GraphVertex source, GraphVertex target) {
        this(source, target, -1L);
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
        return "DefaultEdgeRelation{" +
                "source=" + source +
                ", target=" + target +
                ", tableId=" + tableId +
                '}';
    }

    @Override
    public long getTableId() {
        return this.tableId;
    }
}

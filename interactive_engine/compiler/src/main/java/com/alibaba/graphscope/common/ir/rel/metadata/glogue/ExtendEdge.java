/*
 * Copyright 2024 Alibaba Group Holding Limited.
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

package com.alibaba.graphscope.common.ir.rel.metadata.glogue;

import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.PatternDirection;
import com.alibaba.graphscope.common.ir.rel.metadata.schema.EdgeTypeId;
import com.google.common.collect.ImmutableList;

import java.util.Collections;
import java.util.List;

public class ExtendEdge {
    // the src vertex to extend the edge
    private int srcVertexOrder;
    // the types of the extend edge
    private final List<EdgeTypeId> edgeTypeIds;
    // the direction of the extend edge
    private PatternDirection direction;
    // the weight of the extend edge, which indicates the cost to expand the edge.
    private Double weight;

    public ExtendEdge(int srcVertexOrder, EdgeTypeId edgeTypeId, PatternDirection direction) {
        this.srcVertexOrder = srcVertexOrder;
        this.edgeTypeIds = ImmutableList.of(edgeTypeId);
        this.direction = direction;
    }

    public ExtendEdge(
            int srcVertexOrder, List<EdgeTypeId> edgeTypeIds, PatternDirection direction) {
        this.srcVertexOrder = srcVertexOrder;
        this.edgeTypeIds = edgeTypeIds;
        this.direction = direction;
    }

    public int getSrcVertexOrder() {
        return srcVertexOrder;
    }

    public EdgeTypeId getEdgeTypeId() {
        return edgeTypeIds.get(0);
    }

    public List<EdgeTypeId> getEdgeTypeIds() {
        return Collections.unmodifiableList(edgeTypeIds);
    }

    public PatternDirection getDirection() {
        return direction;
    }

    public void setWeight(Double weight) {
        this.weight = weight;
    }

    public Double getWeight() {
        return weight;
    }

    @Override
    public String toString() {
        return srcVertexOrder
                + (edgeTypeIds.size() == 1 ? edgeTypeIds.get(0).toString() : edgeTypeIds.toString())
                + ": "
                + weight;
    }
}

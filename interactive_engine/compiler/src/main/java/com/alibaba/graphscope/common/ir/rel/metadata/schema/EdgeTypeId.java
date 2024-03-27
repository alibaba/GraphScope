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

package com.alibaba.graphscope.common.ir.rel.metadata.schema;

import org.javatuples.Triplet;

public class EdgeTypeId {
    // srcLabelId, dstLabelId, edgeLabelId
    private final Triplet<Integer, Integer, Integer> edgeType;

    public EdgeTypeId(int srcLabelId, int dstLabelId, int edgeLabelId) {
        this.edgeType = new Triplet<>(srcLabelId, dstLabelId, edgeLabelId);
    }

    public Integer getSrcLabelId() {
        return edgeType.getValue0();
    }

    public Integer getDstLabelId() {
        return edgeType.getValue1();
    }

    public Integer getEdgeLabelId() {
        return edgeType.getValue2();
    }

    public Triplet<Integer, Integer, Integer> getEdgeType() {
        return edgeType;
    }

    @Override
    public String toString() {
        return String.format("[%d-%d->%d]", getSrcLabelId(), getEdgeLabelId(), getDstLabelId());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof EdgeTypeId) {
            return this.edgeType.equals(((EdgeTypeId) obj).edgeType);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return this.edgeType.hashCode();
    }

    @Override
    public EdgeTypeId clone() {
        return new EdgeTypeId(this.getSrcLabelId(), this.getDstLabelId(), this.getEdgeLabelId());
    }

    public int compareTo(EdgeTypeId other) {
        return this.edgeType.compareTo(other.edgeType);
    }
}

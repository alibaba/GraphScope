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

import java.util.List;

public class ExtendStep {
    // The dst vertex (type) that will be added in the pattern expansion.
    private Integer targetVertexType;
    // the dst vertex order that will be added in the pattern expansion.
    private Integer targetVertexOrder;
    // The edges that will be extended in the pattern expansion.
    private List<ExtendEdge> extendEdges;
    // The weight of the extend step, which indicates the cost to expand the step.
    private Double weight;

    public ExtendStep(Integer targetVertexType, List<ExtendEdge> extendEdges) {
        this.targetVertexType = targetVertexType;
        this.extendEdges = extendEdges;
    }

    public Integer getTargetVertexType() {
        return targetVertexType;
    }

    public List<ExtendEdge> getExtendEdges() {
        return extendEdges;
    }

    public void sortExtendEdges() {
        extendEdges.sort(
                (o1, o2) -> {
                    return o1.getWeight().compareTo(o2.getWeight());
                });
    }

    public void setTargetVertexOrder(Integer targetVertexOrder) {
        this.targetVertexOrder = targetVertexOrder;
    }

    public Integer getTargetVertexOrder() {
        return targetVertexOrder;
    }

    public void setWeight(Double weight) {
        this.weight = weight;
    }

    public Double getWeight() {
        return weight;
    }

    @Override
    public String toString() {
        return "ExtendStep{"
                + "targetType="
                + targetVertexType
                + ", targetOrder="
                + targetVertexOrder
                + ", weight="
                + weight
                + ", extendEdges="
                + extendEdges
                + '}';
    }
}

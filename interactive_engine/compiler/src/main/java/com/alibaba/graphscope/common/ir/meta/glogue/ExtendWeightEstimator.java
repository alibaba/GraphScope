/*
 * Copyright 2020 Alibaba Group Holding Limited.
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

package com.alibaba.graphscope.common.ir.meta.glogue;

import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.Pattern;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.PatternEdge;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.PatternVertex;
import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class ExtendWeightEstimator {
    private final CountHandler handler;
    private final EdgeCostEstimator.Extend edgeCostEstimator;

    public ExtendWeightEstimator(CountHandler handler) {
        this.handler = handler;
        this.edgeCostEstimator = new EdgeCostEstimator.Extend(handler);
    }

    /**
     * estimate extend weight for each edge between {@code Pattern} src and {@code Pattern} dst
     * @param edge
     * @return
     */
    public double estimate(PatternEdge edge, PatternVertex target) {
        PatternVertex extendFrom = Utils.getExtendFromVertex(edge, target);
        Pattern vertexPattern = new Pattern(extendFrom);
        Pattern edgePattern = new Pattern();
        edgePattern.addVertex(edge.getSrcVertex());
        edgePattern.addVertex(edge.getDstVertex());
        edgePattern.addEdge(edge.getSrcVertex(), edge.getDstVertex(), edge);
        return handler.handle(edgePattern) / handler.handle(vertexPattern);
    }

    /**
     * estimate total extend weight for all edges in an {@code ExtendStep}, and sort edges by their weight in a heuristic way
     * @param edges
     * @return
     */
    public double estimate(List<PatternEdge> edges, PatternVertex target) {
        // sort edges by their weight in ascending order
        Collections.sort(
                edges, Comparator.comparingDouble((PatternEdge edge) -> estimate(edge, target)));
        Pattern pattern = new Pattern();
        double totalWeight = 0.0d;
        List<PatternVertex> extendFromVertices = Lists.newArrayList();
        for (PatternEdge edge : edges) {
            pattern.addVertex(edge.getSrcVertex());
            pattern.addVertex(edge.getDstVertex());
            pattern.addEdge(edge.getSrcVertex(), edge.getDstVertex(), edge);
            pattern.reordering();
            extendFromVertices.add(Utils.getExtendFromVertex(edge, target));
            double weight =
                    (edges.size() == 1)
                            ? edgeCostEstimator.estimate(null, edges.get(0), target).getExpandRows()
                            : handler.handle(pattern);
            for (PatternVertex vertex : extendFromVertices) {
                weight /= handler.handle(new Pattern(vertex));
            }
            totalWeight += weight;
        }
        return totalWeight;
    }
}

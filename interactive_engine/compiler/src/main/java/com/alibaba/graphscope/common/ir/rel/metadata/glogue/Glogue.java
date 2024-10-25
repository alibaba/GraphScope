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

import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.Pattern;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.PatternVertex;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.SinglePatternVertex;
import com.alibaba.graphscope.common.ir.rel.metadata.schema.GlogueSchema;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.jgrapht.Graph;
import org.jgrapht.graph.DirectedPseudograph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class Glogue {
    // the topology of GLogue graph
    private Graph<Pattern, GlogueEdge> glogueGraph;
    // the cardinality estimation of patterns in Glogue
    private GlogueCardinalityEstimation glogueCardinalityEstimation;
    // the root patterns in Glogue, i.e., those one-vertex patterns.
    private List<Pattern> roots;
    // maxPatternId records the max pattern id in Glogue
    private int maxPatternId;
    private int maxPatternSize;

    private static Logger logger = LoggerFactory.getLogger(Glogue.class);

    protected final GlogueSchema schema;

    public Glogue(GlogueSchema schema, int maxPatternSize) {
        this.schema = schema;
        glogueGraph = new DirectedPseudograph<Pattern, GlogueEdge>(GlogueEdge.class);
        roots = new ArrayList<>();
        maxPatternId = 0;
        create(schema, maxPatternSize);
    }

    // Construct Glogue from a glogue schema with given max pattern size
    private Glogue create(GlogueSchema schema, int maxPatternSize) {
        this.maxPatternSize = maxPatternSize;
        Deque<Pattern> patternQueue = new ArrayDeque<>();
        for (Integer vertexTypeId : schema.getVertexTypes()) {
            PatternVertex vertex = new SinglePatternVertex(vertexTypeId);
            Pattern new_pattern = new Pattern(vertex);
            this.addPattern(new_pattern);
            this.addRoot(new_pattern);
            patternQueue.add(new_pattern);
        }
        while (patternQueue.size() > 0) {
            Pattern pattern = patternQueue.pop();
            if (pattern.getVertexNumber() >= maxPatternSize) {
                continue;
            }
            List<ExtendStep> extendSteps = pattern.getExtendSteps(schema);
            for (ExtendStep extendStep : extendSteps) {
                logger.debug(extendStep.toString());
                Pattern newPattern = pattern.extend(extendStep);
                Optional<Pattern> existingPattern = this.getGlogueVertex(newPattern);
                if (!existingPattern.isPresent()) {
                    this.addPattern(newPattern);
                    Map<Integer, Integer> srcToDstPatternMapping =
                            this.computePatternMapping(pattern, newPattern, extendStep);
                    this.addEdge(pattern, newPattern, extendStep, srcToDstPatternMapping);
                    patternQueue.add(newPattern);
                } else {
                    if (!this.containsEdge(pattern, existingPattern.get())) {
                        // notice that the mapping should be computed based on pattern to
                        // newPattern,
                        // rather than pattern to existingPattern
                        Map<Integer, Integer> srcToDstPatternMapping =
                                this.computePatternMapping(pattern, newPattern, extendStep);
                        this.addEdge(
                                pattern, existingPattern.get(), extendStep, srcToDstPatternMapping);
                    }
                }
            }
        }
        // compute pattern cardinality
        this.glogueCardinalityEstimation = new GlogueBasicCardinalityEstimationImpl(this, schema);
        logger.info(
                "GlogueGraph is created, with {} vertices and {} edges",
                this.glogueGraph.vertexSet().size(),
                this.glogueGraph.edgeSet().size());

        return this;
    }

    public Set<GlogueEdge> getOutEdges(Pattern pattern) {
        Optional<Pattern> vertex = getGlogueVertex(pattern);
        if (vertex.isPresent()) {
            return getGlogueOutEdges(vertex.get());
        } else {
            logger.warn("pattern not found in glogue graph, queried pattern " + pattern);
            return new HashSet<>();
        }
    }

    public Set<GlogueEdge> getInEdges(Pattern pattern) {
        Optional<Pattern> vertex = getGlogueVertex(pattern);
        if (vertex.isPresent()) {
            return getGlogueInEdges(vertex.get());
        } else {
            logger.warn("pattern not found in glogue graph, queried pattern " + pattern);
            return new HashSet<>();
        }
    }

    public Double getRowCount(Pattern pattern) {
        return this.glogueCardinalityEstimation.getCardinality(pattern);
    }

    public @Nullable Double getRowCount(Pattern pattern, boolean allowsNull) {
        return (glogueCardinalityEstimation instanceof GlogueBasicCardinalityEstimationImpl)
                ? ((GlogueBasicCardinalityEstimationImpl) glogueCardinalityEstimation)
                        .getCardinality(pattern, allowsNull)
                : getRowCount(pattern);
    }

    public int getMaxPatternSize() {
        return maxPatternSize;
    }

    protected Set<GlogueEdge> getGlogueOutEdges(Pattern pattern) {
        return glogueGraph.outgoingEdgesOf(pattern);
    }

    protected Set<GlogueEdge> getGlogueInEdges(Pattern pattern) {
        return glogueGraph.incomingEdgesOf(pattern);
    }

    private void addRoot(Pattern pattern) {
        this.roots.add(pattern);
    }

    protected List<Pattern> getRoots() {
        return roots;
    }

    private Optional<Pattern> getGlogueVertex(Pattern pattern) {
        for (Pattern p : this.glogueGraph.vertexSet()) {
            if (p.equals(pattern)) {
                return Optional.of(p);
            }
        }
        return Optional.empty();
    }

    private boolean containsEdge(Pattern srcPattern, Pattern dstPattern) {
        return this.glogueGraph.containsEdge(srcPattern, dstPattern);
    }

    private boolean addPattern(Pattern pattern) {
        pattern.setPatternId(this.maxPatternId++);
        return this.glogueGraph.addVertex(pattern);
    }

    private boolean addEdge(
            Pattern srcPattern,
            Pattern dstPattern,
            ExtendStep edge,
            Map<Integer, Integer> srcToDstIdMapping) {
        GlogueExtendIntersectEdge glogueEdge =
                new GlogueExtendIntersectEdge(srcPattern, dstPattern, edge, srcToDstIdMapping);
        return this.glogueGraph.addEdge(srcPattern, dstPattern, glogueEdge);
    }

    /// Compute the mapping from src pattern to dst pattern.
    /// The mapping preserves srcPatternVertexOrder -> dstPatternVertexOrder,
    /// and the target vertex order will be assigned.
    /// Notice that, the dstPattern should be extended from srcPattern.
    private Map<Integer, Integer> computePatternMapping(
            Pattern srcPattern, Pattern dstPattern, ExtendStep extendStep) {
        Map<Integer, Integer> srcToDstPatternMapping = new HashMap<>();
        for (PatternVertex dstVertex : dstPattern.getVertexSet()) {
            Integer dstVertexOrder = dstPattern.getVertexOrder(dstVertex);
            // srcPattern is extended from dstPattern, so they have the same vertex id.
            PatternVertex srcVertex = srcPattern.getVertexById(dstVertex.getId());
            if (srcVertex == null) {
                // i.e., the dstVertex is a new vertex added in dstPattern
                extendStep.setTargetVertexOrder(dstVertexOrder);
            } else {
                Integer srcVertexOrder = srcPattern.getVertexOrder(srcVertex);
                srcToDstPatternMapping.put(srcVertexOrder, dstVertexOrder);
            }
        }
        return srcToDstPatternMapping;
    }

    @Override
    public String toString() {
        String s = "GlogueVertices:\n";
        for (Pattern p : this.glogueGraph.vertexSet()) {
            s += p.toString() + "\n";
        }
        s += "\nGlogueEdges:\n";
        for (GlogueEdge e : this.glogueGraph.edgeSet()) {
            s += e.toString() + "\n";
        }
        if (this.glogueCardinalityEstimation != null) {
            s += "\nGlogueCardinalityEstimation:\n";
            s += this.glogueCardinalityEstimation.toString();
        }
        return s;
    }
}

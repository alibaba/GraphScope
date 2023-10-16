package com.alibaba.graphscope.common.ir.rel.metadata.glogue.fuzzy;

import com.alibaba.graphscope.common.ir.rel.metadata.glogue.ExtendStep;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.GlogueEdge;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.GlogueExtendIntersectEdge;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.*;
import com.alibaba.graphscope.common.ir.rel.metadata.schema.EdgeTypeId;
import com.alibaba.graphscope.common.ir.rel.metadata.schema.GlogueSchema;

import org.javatuples.Pair;

import java.util.*;

public class FuzzyPatternProcessor {
    private GlogueSchema schema;

    // finally, should return a pattern within Glogue, and attached with fuzzy info
    public class FuzzyInfo {
        private Double fuzzyVerticesWeight;
        private Double fuzzyEdgesWeight;
        private Map<Integer, List<Integer>> fuzzyVertexOrderToTypesMap;
        private Map<Integer, List<Integer>> fuzzyVertexIdToTypesMap;
        private Map<Integer, List<EdgeTypeId>> fuzzyEdgeIdToTypesMap;
        private Map<Integer, Integer> fuzzyToSingleOrderMapping;
        private Map<Integer, Integer> singleToFuzzyOrderMapping;

        private PatternMapping patternMapping;

        private Pattern oriPattern;
        private Pattern newPattern;

        public FuzzyInfo() {
            this.fuzzyVerticesWeight = 1.0;
            this.fuzzyEdgesWeight = 1.0;
            this.fuzzyVertexOrderToTypesMap = new HashMap<>();
            this.fuzzyVertexIdToTypesMap = new HashMap<>();
            this.fuzzyEdgeIdToTypesMap = new HashMap<>();
        }

        public void setOriPattern(Pattern oriPattern) {
            this.oriPattern = oriPattern;
        }

        public void setNewPattern(Pattern newPattern) {
            this.newPattern = newPattern;
        }

        public void setPatternMapping(PatternMapping patternMapping) {
            this.patternMapping = patternMapping;
        }

        public Pattern getOriPattern() {
            return oriPattern;
        }

        public Pattern getNewPattern() {
            return newPattern;
        }

        public void setFuzzyToSingleOrderMapping(Map<Integer, Integer> orderMapping) {
            this.fuzzyToSingleOrderMapping = orderMapping;
        }

        public void setSingleToFuzzyOrderMapping(Map<Integer, Integer> orderMapping) {
            this.singleToFuzzyOrderMapping = orderMapping;
        }

        public Double getFuzzyVerticesWeight() {
            return fuzzyVerticesWeight;
        }

        public Double getFuzzyEdgesWeight() {
            return fuzzyEdgesWeight;
        }

        public Double getFuzzyRatio() {
            return fuzzyVerticesWeight * fuzzyEdgesWeight;
        }

        public void updateFuzzyVerticesWeight(Double fuzzyVertexWeight) {
            this.fuzzyVerticesWeight *= fuzzyVertexWeight;
        }

        public void updateFuzzyEdgesWeight(Double fuzzyEdgeWeight) {
            this.fuzzyEdgesWeight *= fuzzyEdgeWeight;
        }

        public void updateFuzzyVertexOrderToTypeMap(
                Integer fuzzyVertexOrder, List<Integer> singleVertexTypes) {
            this.fuzzyVertexOrderToTypesMap.put(fuzzyVertexOrder, singleVertexTypes);
        }

        public void updateFuzzyVertexIdToTypeMap(
                Integer fuzzyVertexId, List<Integer> singleVertexTypes) {
            this.fuzzyVertexIdToTypesMap.put(fuzzyVertexId, singleVertexTypes);
        }

        public void updateFuzzyEdgeIdToTypeMap(
                Integer fuzzyEdgeId, List<EdgeTypeId> singleEdgeTypes) {
            this.fuzzyEdgeIdToTypesMap.put(fuzzyEdgeId, singleEdgeTypes);
        }
    }

    public FuzzyPatternProcessor(GlogueSchema schema) {
        this.schema = schema;
    }

    /// check if a pattern is fuzzy
    public boolean isFuzzyPattern(Pattern p) {
        boolean isFuzzy = false;
        for (PatternVertex v : p.getVertexSet()) {
            if (!v.isDistinct()) {
                isFuzzy = true;
                break;
            }
        }
        for (PatternEdge e : p.getEdgeSet()) {
            if (!e.isDistinct()) {
                isFuzzy = true;
                break;
            }
        }
        return isFuzzy;
    }

    public Pair<Pattern, FuzzyInfo> processFuzzyPatternAndGetFuzzyInfo(Pattern p) {
        Pattern newPattern = new Pattern();
        FuzzyInfo fuzzyInfo = new FuzzyInfo();
        for (PatternVertex v : p.getVertexSet()) {
            List<Integer> vertexTypeIds = v.getVertexTypeIds();
            Integer singleType = replaceWithSingleType(vertexTypeIds);
            if (vertexTypeIds.size() > 1) {
                fuzzyInfo.updateFuzzyVerticesWeight(computeFuzzyVertexWeight(v, singleType));
                fuzzyInfo.updateFuzzyVertexIdToTypeMap(v.getId(), vertexTypeIds);
                fuzzyInfo.updateFuzzyVertexOrderToTypeMap(p.getVertexOrder(v), vertexTypeIds);
            }
            PatternVertex vertex = new SinglePatternVertex(singleType, v.getId());
            newPattern.addVertex(vertex);
        }
        for (PatternEdge e : p.getEdgeSet()) {
            PatternVertex src = newPattern.getVertexById(e.getSrcVertex().getId());
            PatternVertex dst = newPattern.getVertexById(e.getDstVertex().getId());
            List<EdgeTypeId> edgeTypeIds = e.getEdgeTypeIds();
            EdgeTypeId singleType = replaceWithSingleType(src, dst, edgeTypeIds, schema);
            if (edgeTypeIds.size() > 1) {
                fuzzyInfo.updateFuzzyEdgeIdToTypeMap(e.getId(), edgeTypeIds);
                fuzzyInfo.updateFuzzyEdgesWeight(computeFuzzyEdgeWeight(e, singleType));
            }
            PatternEdge edge = new SinglePatternEdge(src, dst, singleType, e.getId());
            newPattern.addEdge(src, dst, edge);
        }
        newPattern.reordering();
        fuzzyInfo.setOriPattern(p);
        fuzzyInfo.setNewPattern(newPattern);
        computeOrderMapping(p, newPattern, fuzzyInfo);
        return Pair.with(newPattern, fuzzyInfo);
    }

    // replace fuzzy vertex type ids with single type id
    // the logic is to randomly pick a vertex type id.
    private Integer replaceWithSingleType(List<Integer> vertexTypeIds) {
        return vertexTypeIds.get(0);
    }

    // replace fuzzy edge type ids with single type id
    // the logic is, if there is only one edge type id, then return it.
    // otherwise, we need to infer the edge type id from the schema.
    private EdgeTypeId replaceWithSingleType(
            PatternVertex srcVertex,
            PatternVertex dstVertex,
            List<EdgeTypeId> edgeTypeIds,
            GlogueSchema schema) {
        if (edgeTypeIds.size() == 1) {
            return edgeTypeIds.get(0);
        } else {
            Integer srcType = srcVertex.getVertexTypeIds().get(0);
            Integer dstType = dstVertex.getVertexTypeIds().get(0);
            List<Integer> inferredEdgeTypes = new ArrayList<>();
            schema.getEdgeTypes(srcType, dstType)
                    .forEach(edgeType -> inferredEdgeTypes.add(edgeType.getEdgeLabelId()));
            List<Integer> queryEdgeTypes = new ArrayList<>();
            edgeTypeIds.forEach(edgeType -> queryEdgeTypes.add(edgeType.getEdgeLabelId()));
            inferredEdgeTypes.retainAll(queryEdgeTypes);
            if (inferredEdgeTypes.isEmpty()) {
                // TODO: this should not happen.
                // e.g., for a fuzzy pattern (person) - [knows, creates] -> (person, software),
                // if we pick person as the dst vertex type, then the inferred edge type should
                // be knows.
                // if we pick software as the dst vertex type, then the inferred edge type
                // should be creates.
                throw new UnsupportedOperationException(
                        "no edge type found for " + srcType + "->" + dstType);
            }
            return new EdgeTypeId(srcType, dstType, inferredEdgeTypes.get(0));
        }
    }

    private Double computeFuzzyVertexWeight(PatternVertex v, Integer replaceSingleType) {
        Double fuzzyTypeCardinality = 0.0;
        for (Integer vertexTypeId : v.getVertexTypeIds()) {
            fuzzyTypeCardinality += schema.getVertexTypeCardinality(vertexTypeId);
        }
        Double singleTypeCardinality = schema.getVertexTypeCardinality(replaceSingleType);
        return fuzzyTypeCardinality / singleTypeCardinality;
    }

    private Double computeFuzzyEdgeWeight(PatternEdge e, EdgeTypeId replaceSingleType) {
        Double fuzzyTypeCardinality = 0.0;
        for (EdgeTypeId edgeTypeId : e.getEdgeTypeIds()) {
            fuzzyTypeCardinality += schema.getEdgeTypeCardinality(edgeTypeId);
        }
        Double singleTypeCardinality = schema.getEdgeTypeCardinality(replaceSingleType);
        return fuzzyTypeCardinality / singleTypeCardinality;
    }

    public Double estimateCountWithFuzzyInfo(Double count, FuzzyInfo info) {
        System.out.println("fuzzy info: " + info.toString());
        System.out.println("count: " + count);
        // TODO: confirm multiplying the fuzzy ratio? or either fuzzyVerticesWeight or
        // fuzzyEdgesWeight is enough?
        return count * info.getFuzzyRatio();
    }

    private void computeOrderMapping(Pattern oriPattern, Pattern newPattern, FuzzyInfo info) {
        Map<Integer, Integer> oriToNewOrderMapping = new HashMap<>();
        Map<Integer, Integer> newToOriOrderMapping = new HashMap<>();
        for (PatternVertex oriVertex : oriPattern.getVertexSet()) {
            Integer oriOrder = oriPattern.getVertexOrder(oriVertex);
            PatternVertex mappedVertex = newPattern.getVertexById(oriVertex.getId());
            Integer mappedOrder = newPattern.getVertexOrder(mappedVertex);
            oriToNewOrderMapping.put(oriOrder, mappedOrder);
            newToOriOrderMapping.put(mappedOrder, oriOrder);
        }
        info.setFuzzyToSingleOrderMapping(oriToNewOrderMapping);
        info.setSingleToFuzzyOrderMapping(newToOriOrderMapping);
    }

    private void computeOrderMappingAfterIsomorphic(PatternMapping mapping) {}

    // In Glogue Edges
    public Set<GlogueEdge> processGlogueEdgesWithFuzzyInfo(
            Set<GlogueEdge> glogueEdges, FuzzyInfo info, boolean isOut) {
        // TODO: mapping back into fuzzy edges
        for (GlogueEdge glogueEdge : glogueEdges) {
            if (glogueEdge instanceof GlogueExtendIntersectEdge) {
                GlogueExtendIntersectEdge extendIntersectEdge =
                        (GlogueExtendIntersectEdge) glogueEdge;
                // remap the src order to the target order in the ori pattern
                Map<Integer, Integer> glogueSrcToTargetOrderMapping =
                        extendIntersectEdge.getSrcToTargetOrderMapping();
                Map<Integer, Integer> processedSrcToTargetOrderMapping =
                        processGlogueEdgeSrcToTargetOrderMapping(
                                glogueSrcToTargetOrderMapping, info);

                ExtendStep extendStep = extendIntersectEdge.getExtendStep();
            }
        }
        return glogueEdges;
    }

    private Map<Integer, Integer> processGlogueEdgeSrcToTargetOrderMapping(
            Map<Integer, Integer> glogueSrcToTargetOrderMapping, FuzzyInfo info) {
        Map<Integer, Integer> processedSrcToTargetOrderMapping = new HashMap<>();
        for (Map.Entry<Integer, Integer> entry : glogueSrcToTargetOrderMapping.entrySet()) {
            Integer glogueSrcOrder = entry.getKey();
            Integer glogueTargetOrder = entry.getValue();
            Integer mappedOriOrder = info.singleToFuzzyOrderMapping.get(glogueTargetOrder);
            processedSrcToTargetOrderMapping.put(glogueSrcOrder, mappedOriOrder);
        }
        return processedSrcToTargetOrderMapping;
    }

    //    private ExtendStep processGlogueEdgeExtendStep(ExtendStep extendStep, FuzzyInfo info) {
    //        Integer targetOrder = extendStep.getTargetVertexOrder();
    //        Integer mappedTargetOrder = info.singleToFuzzyOrderMapping.get(targetOrder);
    //        for (ExtendEdge extendEdge : extendStep.getExtendEdges()) {
    //            Integer srcVertexOrder = extendEdge.getSrcVertexOrder();
    //            Integer mappedSrcOrder = processedSrcToTargetOrderMapping.get(srcVertexOrder);
    //            extendEdge.setSrcVertexOrder(mappedOriOrder);
    //        }
    //    }
    //
    //    private ExtendEdge processGlogueEdgeExtendEdge(ExtendEdge extendEdge, FuzzyInfo info) {
    //
    //    }
}

package com.alibaba.graphscope.common.ir.planner.rules;

import com.alibaba.graphscope.common.ir.meta.glogue.Utils;
import com.alibaba.graphscope.common.ir.rel.GraphJoinDecomposition;
import com.alibaba.graphscope.common.ir.rel.GraphPattern;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.tools.RelBuilderFactory;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

public class JoinDecompositionRule<C extends JoinDecompositionRule.Config> extends RelRule<C> {

    protected JoinDecompositionRule(C config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall relOptRuleCall) {
        GraphPattern pattern = relOptRuleCall.rel(0);
        if (getMaxVertexNum(pattern.getPattern()) < config.getMinPatternSize()) {
            return;
        }
        List<GraphJoinDecomposition> decompositions = getDecompositions(pattern);
        for (GraphJoinDecomposition decomposition : decompositions) {
            relOptRuleCall.transformTo(decomposition);
        }
    }

    private int getMaxVertexNum(Pattern pattern) {
        int maxVertexNum = pattern.getVertexNumber();
        for (PatternEdge edge : pattern.getEdgeSet()) {
            if (edge.getElementDetails().getRange() != null) {
                PathExpandRange range = edge.getElementDetails().getRange();
                int maxHop = range.getOffset() + range.getFetch() - 1;
                maxVertexNum += (maxHop - 1);
            }
        }
        return maxVertexNum;
    }

    // bfs to get all possible decompositions
    private List<GraphJoinDecomposition> getDecompositions(GraphPattern pattern) {
        List<GraphJoinDecomposition> queues = initDecompositions(pattern);
        int initialSize = queues.size();
        int offset = 0;
        List<PatternVertex> newAddVertices = Lists.newArrayList();
        while (offset < queues.size()) {
            List<GraphJoinDecomposition> newCompositions =
                    getDecompositions(queues.get(offset++), newAddVertices);
            addDedupCompositions(queues, newCompositions);
        }
        return queues.subList(initialSize, queues.size());
    }

    private List<GraphJoinDecomposition> initDecompositions(GraphPattern pattern) {
        Pattern buildPattern = pattern.getPattern();
        buildPattern.reordering();
        List<GraphJoinDecomposition> results = Lists.newArrayList();
        for (PatternVertex vertex : buildPattern.getVertexSet()) {
            Pattern probePattern = new Pattern(vertex);
            probePattern.reordering();
            Map<Integer, Integer> probeOrderMap = Maps.newHashMap();
            probeOrderMap.put(
                    probePattern.getVertexOrder(vertex), buildPattern.getVertexOrder(vertex));
            Map<Integer, Integer> buildOrderMap = Maps.newHashMap();
            buildPattern
                    .getVertexSet()
                    .forEach(
                            v -> {
                                int orderId = buildPattern.getVertexOrder(v);
                                buildOrderMap.put(orderId, orderId);
                            });
            GraphJoinDecomposition decomposition =
                    new GraphJoinDecomposition(
                            pattern.getCluster(),
                            pattern.getTraitSet(),
                            pattern.getPattern(),
                            probePattern,
                            buildPattern,
                            Lists.newArrayList(
                                    new GraphJoinDecomposition.JoinVertexPair(
                                            probePattern.getVertexOrder(vertex),
                                            buildPattern.getVertexOrder(vertex))),
                            new GraphJoinDecomposition.OrderMappings(probeOrderMap, buildOrderMap));
            results.add(decomposition);
        }
        return results;
    }

    private List<GraphJoinDecomposition> getDecompositions(
            GraphJoinDecomposition parent, List<PatternVertex> newAddVertices) {
        List<GraphJoinDecomposition.JoinVertexPair> jointVertices = parent.getJoinVertexPairs();
        List<GraphJoinDecomposition> results = Lists.newArrayList();
        // try to add one joint vertex into disjoint vertices of the probe pattern
        for (GraphJoinDecomposition.JoinVertexPair jointVertex : jointVertices) {
            results.addAll(getDecompositions(parent, jointVertex, jointVertices, newAddVertices));
        }
        return results;
    }

    private List<GraphJoinDecomposition> getDecompositions(
            GraphJoinDecomposition parent,
            GraphJoinDecomposition.JoinVertexPair jointVertex,
            List<GraphJoinDecomposition.JoinVertexPair> jointVertices,
            List<PatternVertex> newAddVertices) {
        Pattern buildPattern = ((GraphPattern) parent.getRight()).getPattern();
        PatternVertex buildJointVertex =
                buildPattern.getVertexByOrder(jointVertex.getRightOrderId());
        if (newAddVertices.contains(buildJointVertex)) {
            return ImmutableList.of();
        }
        // guarantee the joint vertex is not connected to any joint vertices in the build pattern
        if (buildPattern.getEdgesOf(buildJointVertex).stream()
                .anyMatch(
                        edge -> {
                            PatternVertex disjointVertex =
                                    Utils.getExtendFromVertex(edge, buildJointVertex);
                            return jointVertices.stream()
                                    .anyMatch(
                                            v ->
                                                    v.getRightOrderId()
                                                            == buildPattern.getVertexOrder(
                                                                    disjointVertex));
                        })) {
            return ImmutableList.of();
        }
        Pattern buildClone0 = new Pattern(buildPattern);
        // guarantee the build pattern is still connected after removing the joint vertex
        if (buildClone0.removeVertex(buildJointVertex).size() != 1) {
            return ImmutableList.of();
        }
        // find all possible edge decompositions, each edge decomposition contains the edges to be
        // added to the probe pattern and to the build pattern
        List<EdgeDecomposition> edgeDecompositions = Lists.newArrayList();
        getEdgeDecompositions(
                Lists.newArrayList(buildPattern.getEdgesOf(buildJointVertex)),
                0,
                new EdgeDecomposition(Lists.newArrayList(), Lists.newArrayList()),
                edgeDecompositions,
                buildJointVertex,
                newAddVertices);
        return edgeDecompositions.stream()
                .map(
                        k ->
                                createNewJoinDecomposition(
                                        parent, buildClone0, k, jointVertex, jointVertices))
                .filter(k -> k != null)
                .collect(Collectors.toList());
    }

    private @Nullable GraphJoinDecomposition createNewJoinDecomposition(
            GraphJoinDecomposition parent,
            Pattern buildClone0,
            EdgeDecomposition edgeDecomposition,
            GraphJoinDecomposition.JoinVertexPair jointVertex,
            List<GraphJoinDecomposition.JoinVertexPair> jointVertices) {
        Pattern probePattern = ((GraphPattern) parent.getLeft()).getPattern();
        Pattern buildPattern = ((GraphPattern) parent.getRight()).getPattern();
        PatternVertex probeJointVertex =
                probePattern.getVertexByOrder(jointVertex.getLeftOrderId());
        PatternVertex buildJointVertex =
                buildPattern.getVertexByOrder(jointVertex.getRightOrderId());
        Pattern probeClone = new Pattern(probePattern);
        Pattern buildClone = new Pattern(buildClone0);
        // create the new probe pattern
        for (PatternEdge probeEdge : edgeDecomposition.probeEdges) {
            PatternVertex probeNewVertex = Utils.getExtendFromVertex(probeEdge, buildJointVertex);
            probeClone.addVertex(probeNewVertex);
            if (probeEdge.getSrcVertex().equals(buildJointVertex)) {
                probeClone.addEdge(probeJointVertex, probeNewVertex, probeEdge);
            } else {
                probeClone.addEdge(probeNewVertex, probeJointVertex, probeEdge);
            }
        }
        // create the new build pattern
        for (PatternEdge buildEdge : edgeDecomposition.buildEdges) {
            if (!buildClone.containsVertex(buildEdge.getSrcVertex())) {
                buildClone.addVertex(buildEdge.getSrcVertex());
            }
            if (!buildClone.containsVertex(buildEdge.getDstVertex())) {
                buildClone.addVertex(buildEdge.getDstVertex());
            }
            buildClone.addEdge(buildEdge.getSrcVertex(), buildEdge.getDstVertex(), buildEdge);
        }
        if (probeClone.getVertexNumber() > buildClone.getVertexNumber()) {
            return null;
        }
        List<PatternVertex> jointCandidates =
                edgeDecomposition.probeEdges.stream()
                        .map(k -> Utils.getExtendFromVertex(k, buildJointVertex))
                        .filter(k -> buildClone.containsVertex(k))
                        .collect(Collectors.toList());
        probeClone.reordering();
        buildClone.reordering();
        // update the joint vertices for the new probe pattern and build pattern
        List<GraphJoinDecomposition.JoinVertexPair> newJointVertices =
                jointVertices.stream()
                        .filter(v -> v.getRightOrderId() != jointVertex.getRightOrderId())
                        .map(
                                v ->
                                        new GraphJoinDecomposition.JoinVertexPair(
                                                probeClone.getVertexOrder(
                                                        probePattern.getVertexByOrder(
                                                                v.getLeftOrderId())),
                                                buildClone.getVertexOrder(
                                                        buildPattern.getVertexByOrder(
                                                                v.getRightOrderId()))))
                        .collect(Collectors.toList());
        for (PatternVertex jointCandidate : jointCandidates) {
            newJointVertices.add(
                    new GraphJoinDecomposition.JoinVertexPair(
                            probeClone.getVertexOrder(jointCandidate),
                            buildClone.getVertexOrder(jointCandidate)));
        }
        int newJointVertexNum = newJointVertices.size();
        if (newJointVertexNum == 0
                || newJointVertexNum >= probeClone.getVertexNumber()
                || newJointVertexNum >= buildClone.getVertexNumber()) {
            return null;
        }
        JoinRelType joinType = pruneOptional(probeClone, buildClone, newJointVertices);
        if (joinType == null) {
            return null;
        }
        // update the order mappings for the new probe pattern and build pattern
        Map<Integer, Integer> probeOrderMap = parent.getOrderMappings().getLeftToTargetOrderMap();
        Map<Integer, Integer> buildOrderMap = parent.getOrderMappings().getRightToTargetOrderMap();
        Map<Integer, Integer> newProbeOrderMap = Maps.newHashMap();
        probeClone
                .getVertexSet()
                .forEach(
                        v -> {
                            // todo: maintain the order mappings for the new added vertex in probe
                            // pattern
                            if (probePattern.containsVertex(v) || buildPattern.containsVertex(v)) {
                                Integer targetOrderId =
                                        (probePattern.containsVertex(v))
                                                ? probeOrderMap.get(probePattern.getVertexOrder(v))
                                                : buildOrderMap.get(buildPattern.getVertexOrder(v));
                                newProbeOrderMap.put(probeClone.getVertexOrder(v), targetOrderId);
                            }
                        });
        Map<Integer, Integer> newBuildOrderMap = Maps.newHashMap();
        buildClone
                .getVertexSet()
                .forEach(
                        v -> {
                            // todo: maintain the order mappings for the new added vertex in build
                            // pattern
                            if (buildPattern.containsVertex(v)) {
                                newBuildOrderMap.put(
                                        buildClone.getVertexOrder(v),
                                        buildOrderMap.get(buildPattern.getVertexOrder(v)));
                            }
                        });
        return new GraphJoinDecomposition(
                parent.getCluster(),
                parent.getTraitSet(),
                parent.getParentPatten(),
                probeClone,
                buildClone,
                newJointVertices,
                new GraphJoinDecomposition.OrderMappings(newProbeOrderMap, newBuildOrderMap),
                joinType);
    }

    private void getEdgeDecompositions(
            List<PatternEdge> probeEdges,
            int edgeId,
            EdgeDecomposition curDecomposition,
            List<EdgeDecomposition> resultDecompositions,
            PatternVertex jointVertex,
            List<PatternVertex> newAddVertices) {
        if (edgeId == probeEdges.size()) {
            resultDecompositions.add(curDecomposition);
            return;
        }
        PatternEdge probeEdge = probeEdges.get(edgeId);
        PathExpandRange pxdRange = probeEdge.getElementDetails().getRange();
        // try to split the path expand
        if (pxdRange != null) {
            int minHop = pxdRange.getOffset();
            int maxHop = pxdRange.getOffset() + pxdRange.getFetch() - 1;
            if (maxHop >= config.getMinPatternSize() - 1) {
                for (int i = 0; i <= minHop; ++i) {
                    for (int j = 1; j <= maxHop - 1; ++j) {
                        if (i <= j && (minHop - i) <= (maxHop - j)) {
                            // split the path expand into two path expands
                            // probe part: [i, j]
                            // build part: [minHop - i, maxHop - j]
                            PatternVertex anotherVertex =
                                    Utils.getExtendFromVertex(probeEdge, jointVertex);
                            PatternVertex splitVertex = createNewVertex(anotherVertex);
                            newAddVertices.add(splitVertex);
                            PatternVertex probeSrc, probeDst;
                            if (probeEdge.getSrcVertex().equals(jointVertex)) {
                                probeSrc = jointVertex;
                                probeDst = splitVertex;
                            } else {
                                probeSrc = splitVertex;
                                probeDst = jointVertex;
                            }
                            PatternEdge probeSplit =
                                    createNewEdge(
                                            probeEdge,
                                            probeSrc,
                                            probeDst,
                                            new PathExpandRange(i, j - i + 1));
                            PatternVertex buildSrc, buildDst;
                            if (probeEdge.getSrcVertex().equals(jointVertex)) {
                                buildSrc = splitVertex;
                                buildDst = anotherVertex;
                            } else {
                                buildSrc = anotherVertex;
                                buildDst = splitVertex;
                            }
                            PatternEdge buildSplit =
                                    createNewEdge(
                                            probeEdge,
                                            buildSrc,
                                            buildDst,
                                            new PathExpandRange(
                                                    minHop - i, maxHop - j - (minHop - i) + 1));
                            EdgeDecomposition cloneDecomposition = curDecomposition.copy();
                            cloneDecomposition.probeEdges.add(probeSplit);
                            cloneDecomposition.buildEdges.add(buildSplit);
                            getEdgeDecompositions(
                                    probeEdges,
                                    edgeId + 1,
                                    cloneDecomposition,
                                    resultDecompositions,
                                    jointVertex,
                                    newAddVertices);
                        }
                    }
                }
            }
        }
        EdgeDecomposition cloneDecomposition = curDecomposition.copy();
        cloneDecomposition.probeEdges.add(probeEdge);
        getEdgeDecompositions(
                probeEdges,
                edgeId + 1,
                cloneDecomposition,
                resultDecompositions,
                jointVertex,
                newAddVertices);
    }

    private PatternVertex createNewVertex(PatternVertex oldVertex) {
        int randomId = UUID.randomUUID().hashCode();
        return (oldVertex instanceof SinglePatternVertex)
                ? new SinglePatternVertex(
                        oldVertex.getVertexTypeIds().get(0), randomId, new ElementDetails())
                : new FuzzyPatternVertex(
                        oldVertex.getVertexTypeIds(), randomId, new ElementDetails());
    }

    private PatternEdge createNewEdge(
            PatternEdge oldEdge,
            PatternVertex newSrc,
            PatternVertex newDst,
            PathExpandRange newRange) {
        // Here, by setting the ID of the split edge to the same as the original edge,
        // the intention is to enable finding the details info of the previous edge.
        // This way, the final <GraphLogicalPathExpand> operator can include alias and filter
        // details.
        int newEdgeId = oldEdge.getId();
        ElementDetails newDetails =
                new ElementDetails(
                        oldEdge.getElementDetails().getSelectivity(),
                        newRange,
                        oldEdge.getElementDetails().getPxdInnerGetVTypes(),
                        oldEdge.getElementDetails().getResultOpt(),
                        oldEdge.getElementDetails().getPathOpt());
        return (oldEdge instanceof SinglePatternEdge)
                ? new SinglePatternEdge(
                        newSrc,
                        newDst,
                        oldEdge.getEdgeTypeIds().get(0),
                        newEdgeId,
                        oldEdge.isBoth(),
                        newDetails)
                : new FuzzyPatternEdge(
                        newSrc,
                        newDst,
                        oldEdge.getEdgeTypeIds(),
                        newEdgeId,
                        oldEdge.isBoth(),
                        newDetails);
    }

    private void addDedupCompositions(
            List<GraphJoinDecomposition> dedupCompositions,
            List<GraphJoinDecomposition> addCompositions) {
        for (GraphJoinDecomposition addComposition : addCompositions) {
            Pattern probe = ((GraphPattern) addComposition.getLeft()).getPattern();
            Pattern target = ((GraphPattern) addComposition.getRight()).getPattern();
            if (probe.getVertexNumber() > target.getVertexNumber()) {
                continue;
            }
            if (!containsDecomposition(dedupCompositions, addComposition)) {
                dedupCompositions.add(addComposition);
            }
        }
    }

    private boolean containsDecomposition(
            List<GraphJoinDecomposition> decompositions, GraphJoinDecomposition target) {
        Pattern targetProbe = ((GraphPattern) target.getLeft()).getPattern();
        Pattern targetBuild = ((GraphPattern) target.getRight()).getPattern();
        return decompositions.stream()
                .anyMatch(
                        d -> {
                            Pattern dProbe = ((GraphPattern) d.getLeft()).getPattern();
                            Pattern dBuild = ((GraphPattern) d.getRight()).getPattern();
                            return dProbe.isIsomorphicTo(targetProbe)
                                            && dBuild.isIsomorphicTo(targetBuild)
                                    || dProbe.isIsomorphicTo(targetBuild)
                                            && dBuild.isIsomorphicTo(targetProbe);
                        });
    }

    /**
     * Prune unreasonable join combinations. Join combinations that meet any of the following conditions will be retained; otherwise, prune:
     * 1. All edges in the probe pattern are non-optional, and all edges in the build pattern are optional. In this case, the original pattern is probe left join build;
     * 2. All edges in the probe pattern are optional, and all edges in the build pattern are non-optional. In this case, the original pattern is probe right join build;
     * 3. The edges adjacent to the joint point are all non-optional. In this case, the original pattern is probe inner join build.
     * @param probePattern
     * @param buildPattern
     * @param jointVertices
     * @return return null if the join combination is pruned; otherwise, return the join type
     */
    private @Nullable JoinRelType pruneOptional(
            Pattern probePattern,
            Pattern buildPattern,
            List<GraphJoinDecomposition.JoinVertexPair> jointVertices) {
        JoinRelType joinType = null;
        if (allNonOptional(probePattern.getEdgeSet()) && allOptional(buildPattern.getEdgeSet())) {
            buildPattern.getVertexSet().forEach(v -> v.getElementDetails().setOptional(false));
            buildPattern.getEdgeSet().forEach(e -> e.getElementDetails().setOptional(false));
            buildPattern.reordering();
            joinType = JoinRelType.LEFT;
        } else if (allOptional(probePattern.getEdgeSet())
                && allNonOptional(buildPattern.getEdgeSet())) {
            probePattern.getVertexSet().forEach(v -> v.getElementDetails().setOptional(false));
            probePattern.getEdgeSet().forEach(e -> e.getElementDetails().setOptional(false));
            probePattern.reordering();
            joinType = JoinRelType.RIGHT;
        } else {
            Set<PatternEdge> probeJointAdjEdges = Sets.newHashSet();
            Set<PatternEdge> buildJointAdjEdges = Sets.newHashSet();
            jointVertices.forEach(
                    v -> {
                        probeJointAdjEdges.addAll(
                                probePattern.getEdgesOf(
                                        probePattern.getVertexByOrder(v.getLeftOrderId())));
                        buildJointAdjEdges.addAll(
                                buildPattern.getEdgesOf(
                                        buildPattern.getVertexByOrder(v.getRightOrderId())));
                    });
            if (allNonOptional(probeJointAdjEdges) && allNonOptional(buildJointAdjEdges)) {
                joinType = JoinRelType.INNER;
            }
        }
        return joinType;
    }

    private boolean allOptional(Set<PatternEdge> edges) {
        return edges.stream().allMatch(e -> e.getElementDetails().isOptional());
    }

    private boolean allNonOptional(Set<PatternEdge> edges) {
        return edges.stream().allMatch(e -> !e.getElementDetails().isOptional());
    }

    public static class Config implements RelRule.Config {
        public static JoinDecompositionRule.Config DEFAULT =
                new JoinDecompositionRule.Config()
                        .withOperandSupplier(b0 -> b0.operand(GraphPattern.class).anyInputs());

        private RelRule.OperandTransform operandSupplier;
        private @Nullable String description;
        private RelBuilderFactory builderFactory;
        private int minPatternSize;

        @Override
        public RelRule toRule() {
            return new JoinDecompositionRule(this);
        }

        @Override
        public JoinDecompositionRule.Config withRelBuilderFactory(
                RelBuilderFactory relBuilderFactory) {
            this.builderFactory = relBuilderFactory;
            return this;
        }

        @Override
        public JoinDecompositionRule.Config withDescription(
                @org.checkerframework.checker.nullness.qual.Nullable String s) {
            this.description = s;
            return this;
        }

        @Override
        public JoinDecompositionRule.Config withOperandSupplier(OperandTransform operandTransform) {
            this.operandSupplier = operandTransform;
            return this;
        }

        public JoinDecompositionRule.Config withMinPatternSize(int minPatternSize) {
            this.minPatternSize = minPatternSize;
            return this;
        }

        @Override
        public OperandTransform operandSupplier() {
            return this.operandSupplier;
        }

        @Override
        public @org.checkerframework.checker.nullness.qual.Nullable String description() {
            return this.description;
        }

        @Override
        public RelBuilderFactory relBuilderFactory() {
            return this.builderFactory;
        }

        public int getMinPatternSize() {
            return minPatternSize;
        }
    }

    private static class EdgeDecomposition {
        private final List<PatternEdge> probeEdges;
        private final List<PatternEdge> buildEdges;

        public EdgeDecomposition(List<PatternEdge> probeEdges, List<PatternEdge> buildEdges) {
            this.probeEdges = probeEdges;
            this.buildEdges = buildEdges;
        }

        public EdgeDecomposition copy() {
            return new EdgeDecomposition(
                    Lists.newArrayList(probeEdges), Lists.newArrayList(buildEdges));
        }
    }
}

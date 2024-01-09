package com.alibaba.graphscope.common.ir.planner.rules;

import com.alibaba.graphscope.common.ir.meta.glogue.Utils;
import com.alibaba.graphscope.common.ir.rel.GraphJoinDecomposition;
import com.alibaba.graphscope.common.ir.rel.GraphPattern;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.Pattern;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.PatternEdge;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.PatternVertex;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.tools.RelBuilderFactory;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class JoinDecompositionRule<C extends JoinDecompositionRule.Config> extends RelRule<C> {
    protected JoinDecompositionRule(C config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall relOptRuleCall) {
        GraphPattern pattern = relOptRuleCall.rel(0);
        if (pattern.getPattern().getVertexNumber() < config.getMinPatternSize()) {
            return;
        }
        List<GraphJoinDecomposition> decompositions = getDecompositions(pattern);
        for (GraphJoinDecomposition decomposition : decompositions) {
            relOptRuleCall.transformTo(decomposition);
        }
    }

    // bfs to get all possible decompositions
    private List<GraphJoinDecomposition> getDecompositions(GraphPattern pattern) {
        List<GraphJoinDecomposition> queues = initDecompositions(pattern);
        int initialSize = queues.size();
        int offset = 0;
        while (offset < queues.size()) {
            List<GraphJoinDecomposition> newCompositions = getDecompositions(queues.get(offset++));
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

    private List<GraphJoinDecomposition> getDecompositions(GraphJoinDecomposition parent) {
        List<GraphJoinDecomposition.JoinVertexPair> jointVertices = parent.getJoinVertexPairs();
        List<GraphJoinDecomposition> results = Lists.newArrayList();
        // try to add one joint vertex into disjoint vertices of the probe pattern
        for (GraphJoinDecomposition.JoinVertexPair jointVertex : jointVertices) {
            GraphJoinDecomposition decomposition =
                    getDecomposition(parent, jointVertex, jointVertices);
            if (decomposition != null) {
                results.add(decomposition);
            }
        }
        return results;
    }

    private @Nullable GraphJoinDecomposition getDecomposition(
            GraphJoinDecomposition parent,
            GraphJoinDecomposition.JoinVertexPair jointVertex,
            List<GraphJoinDecomposition.JoinVertexPair> jointVertices) {
        Pattern probePattern = ((GraphPattern) parent.getLeft()).getPattern();
        Pattern buildPattern = ((GraphPattern) parent.getRight()).getPattern();
        PatternVertex probeJointVertex =
                probePattern.getVertexByOrder(jointVertex.getLeftOrderId());
        PatternVertex buildJointVertex =
                buildPattern.getVertexByOrder(jointVertex.getRightOrderId());
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
            return null;
        }
        Pattern probeClone = new Pattern(probePattern);
        Pattern buildClone = new Pattern(buildPattern);
        // check if build pattern still connected after removing one joint vertex
        if (buildClone.removeVertex(buildJointVertex).size() != 1) {
            return null;
        }
        List<PatternVertex> jointCandidates = Lists.newArrayList();
        for (PatternEdge edge : buildPattern.getEdgesOf(buildJointVertex)) {
            PatternVertex disjointVertex = Utils.getExtendFromVertex(edge, buildJointVertex);
            // add the vertex and edge to the probe pattern
            probeClone.addVertex(disjointVertex);
            if (probeClone.getVertexNumber() > buildClone.getVertexNumber()) {
                return null;
            }
            // todo: add edges between the disjoint vertex and the joint vertices (including
            // vertices in joint candidates) in the probe pattern
            if (edge.getSrcVertex().equals(buildJointVertex)) {
                probeClone.addEdge(probeJointVertex, disjointVertex, edge);
            } else {
                probeClone.addEdge(disjointVertex, probeJointVertex, edge);
            }
            // the disjoint vertex becomes the new joint vertex if meets the condition
            if (buildClone.containsVertex(disjointVertex)) {
                jointCandidates.add(disjointVertex);
            }
        }
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
        // update the order mappings for the new probe pattern and build pattern
        Map<Integer, Integer> probeOrderMap = parent.getOrderMappings().getLeftToTargetOrderMap();
        Map<Integer, Integer> buildOrderMap = parent.getOrderMappings().getRightToTargetOrderMap();
        Map<Integer, Integer> newProbeOrderMap = Maps.newHashMap();
        probeClone
                .getVertexSet()
                .forEach(
                        v -> {
                            Integer targetOrderId =
                                    (probePattern.containsVertex(v))
                                            ? probeOrderMap.get(probePattern.getVertexOrder(v))
                                            : buildOrderMap.get(buildPattern.getVertexOrder(v));
                            newProbeOrderMap.put(probeClone.getVertexOrder(v), targetOrderId);
                        });
        Map<Integer, Integer> newBuildOrderMap = Maps.newHashMap();
        buildClone
                .getVertexSet()
                .forEach(
                        v -> {
                            newBuildOrderMap.put(
                                    buildClone.getVertexOrder(v),
                                    buildOrderMap.get(buildPattern.getVertexOrder(v)));
                        });
        return new GraphJoinDecomposition(
                parent.getCluster(),
                parent.getTraitSet(),
                probeClone,
                buildClone,
                newJointVertices,
                new GraphJoinDecomposition.OrderMappings(newProbeOrderMap, newBuildOrderMap));
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
                                    && dBuild.isIsomorphicTo(targetBuild);
                        });
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
}

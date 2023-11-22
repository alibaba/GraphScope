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

package com.alibaba.graphscope.common.ir.type;

import com.alibaba.graphscope.common.ir.rel.graph.*;
import com.alibaba.graphscope.common.ir.rel.type.AliasNameWithId;
import com.alibaba.graphscope.common.ir.tools.AliasInference;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.config.*;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.commons.lang3.ObjectUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.*;
import java.util.stream.Collectors;

public class GraphTypeInference {
    private final GraphBuilder builder;

    public GraphTypeInference(GraphBuilder builder) {
        this.builder = builder;
    }

    public RelNode inferTypes(RelNode top) {
        return visitRels(ImmutableList.of(top)).get(0);
    }

    public List<RelNode> inferTypes(List<RelNode> sentences) {
        return visitRels(sentences);
    }

    private List<RelNode> visitRels(List<RelNode> rels) {
        int maxIter = calculateMaxIters(rels);
        List<RelNode> updated = Lists.newArrayList();
        do {
            if (!updated.isEmpty()) {
                updated.clear();
            }
            RelGraph relGraph = new RelGraph(rels);
            rels =
                    relGraph.getRels().stream()
                            .map(k -> dfs(relGraph, k, getType(k), Sets.newHashSet(), updated))
                            .collect(Collectors.toList());
        } while (maxIter-- > 0 && !updated.isEmpty());
        return rels;
    }

    private int calculateMaxIters(List<RelNode> rels) {
        List<RelNode> queue = Lists.newArrayList(rels);
        int maxIters = 0;
        while (!queue.isEmpty()) {
            RelNode cur = queue.remove(0);
            ++maxIters;
            queue.addAll(cur.getInputs());
        }
        return maxIters;
    }

    private RelNode dfs(
            RelGraph relGraph,
            RelNode top,
            RelDataType restriction,
            Set<RelNode> visited,
            List<RelNode> updated) {
        if (visited.contains(top)
                || !(top instanceof AbstractBindableTableScan
                        || top instanceof GraphLogicalPathExpand)) {
            return top;
        }
        visited.add(top);
        List<RelNode> newNeighbors =
                relGraph.getNeighbors(top).stream()
                        .map(
                                k ->
                                        dfs(
                                                relGraph,
                                                k,
                                                restrictChild(k, top, restriction),
                                                visited,
                                                updated))
                        .collect(Collectors.toList());
        RelDataType newType = restrictParent(newNeighbors, top, restriction);
        RelDataType oldType = getType(top);
        if (!newType.equals(oldType)) {
            updated.add(top);
            RelNode newTop = newRel(top, newType);
            RelNode parent = relGraph.getParent(top);
            if (parent != null) {
                parent.replaceInput(0, newTop);
            }
            return newTop;
        }
        return top;
    }

    private RelDataType restrictChild(RelNode child, RelNode parent, RelDataType parentType) {
        if (child instanceof GraphLogicalSource
                        && ((GraphLogicalSource) child).getOpt() == GraphOpt.Source.VERTEX
                || child instanceof GraphLogicalGetV) {
            if (parent instanceof GraphLogicalPathExpand) {
                parent = ((GraphLogicalPathExpand) parent).getExpand();
            }
            if (parent instanceof GraphLogicalExpand) {
                GraphLogicalExpand expand = (GraphLogicalExpand) parent;
                GraphLabelType childLabelType = ((GraphSchemaType) getType(child)).getLabelType();
                GraphLabelType parentLabelType = ((GraphSchemaType) parentType).getLabelType();
                List<GraphLabelType.Entry> commonLabels =
                        commonLabels(childLabelType, parentLabelType, expand.getOpt(), true);
                return new GraphSchemaType(
                        GraphOpt.Source.VERTEX,
                        new GraphLabelType(commonLabels),
                        ImmutableList.of());
            }
            throw new IllegalArgumentException(
                    "graph generic type error: unable to establish an extension relationship"
                            + " between node "
                            + child
                            + " with node "
                            + parent);
        }
        if (child instanceof GraphLogicalSource
                        && ((GraphLogicalSource) child).getOpt() == GraphOpt.Source.EDGE
                || child instanceof GraphLogicalExpand) {
            Preconditions.checkArgument(
                    parent instanceof GraphLogicalGetV,
                    "graph generic type error: unable to establish an extension relationship"
                            + " between node %s with node %s",
                    child,
                    parent);
            GraphLogicalGetV getV = (GraphLogicalGetV) parent;
            GraphLabelType childLabelType = ((GraphSchemaType) getType(child)).getLabelType();
            GraphLabelType parentLabelType = ((GraphSchemaType) parentType).getLabelType();
            List<GraphLabelType.Entry> commonLabels =
                    commonLabels(childLabelType, parentLabelType, getV.getOpt(), true);
            return new GraphSchemaType(
                    GraphOpt.Source.EDGE, new GraphLabelType(commonLabels), ImmutableList.of());
        }
        if (child instanceof GraphLogicalPathExpand) {
            Preconditions.checkArgument(
                    parent instanceof GraphLogicalGetV,
                    "graph generic type error: unable to establish an extension relationship"
                            + " between node %s with node %s",
                    child,
                    parent);
            GraphLogicalPathExpand pxd = (GraphLogicalPathExpand) child;
            RelDataType innerGetVType =
                    ((GraphPathType) getType(pxd)).getComponentType().getGetVType();
            GraphLabelType innerGetVLabelType = ((GraphSchemaType) innerGetVType).getLabelType();
            GraphLabelType outerGetVLabelType = ((GraphSchemaType) parentType).getLabelType();
            List<GraphLabelType.Entry> commonLabels =
                    commonLabels(innerGetVLabelType, outerGetVLabelType, true);
            RelDataType newGetVType =
                    new GraphSchemaType(
                            GraphOpt.Source.VERTEX,
                            new GraphLabelType(commonLabels),
                            ImmutableList.of());
            RelDataType newExpandType = restrictChild(pxd.getExpand(), pxd.getGetV(), newGetVType);
            return new GraphPathType(new GraphPathType.ElementType(newExpandType, newGetVType));
        }
        throw new IllegalArgumentException(
                "graph generic type error: unable to establish an extension relationship between"
                        + " node "
                        + child
                        + " with node "
                        + parent);
    }

    private RelDataType restrictParent(
            List<RelNode> children, RelNode parent, RelDataType parentType) {
        for (RelNode child : children) {
            parentType = restrictParent(child, parent, parentType);
        }
        return parentType;
    }

    private RelDataType restrictParent(RelNode child, RelNode parent, RelDataType parentType) {
        if (child instanceof GraphLogicalSource
                        && ((GraphLogicalSource) child).getOpt() == GraphOpt.Source.VERTEX
                || child instanceof GraphLogicalGetV) {
            if (parent instanceof GraphLogicalExpand) {
                GraphLogicalExpand expand = (GraphLogicalExpand) parent;
                GraphLabelType childLabelType = ((GraphSchemaType) getType(child)).getLabelType();
                GraphLabelType parentLabelType = ((GraphSchemaType) parentType).getLabelType();
                List<GraphLabelType.Entry> commonLabels =
                        commonLabels(childLabelType, parentLabelType, expand.getOpt(), false);
                return new GraphSchemaType(
                        GraphOpt.Source.EDGE, new GraphLabelType(commonLabels), ImmutableList.of());
            }
            if (parent instanceof GraphLogicalPathExpand) {
                GraphLogicalPathExpand pxd = (GraphLogicalPathExpand) parent;
                RelDataType newExpandType =
                        restrictParent(
                                child,
                                pxd.getExpand(),
                                ((GraphPathType) parentType).getComponentType().getExpandType());
                RelNode newExpand = newRel(pxd.getExpand(), newExpandType);
                RelDataType newGetVType =
                        restrictParent(
                                newExpand,
                                pxd.getGetV(),
                                ((GraphPathType) parentType).getComponentType().getGetVType());
                return new GraphPathType(new GraphPathType.ElementType(newExpandType, newGetVType));
            }
            throw new IllegalArgumentException(
                    "graph generic type error: unable to establish an extension relationship"
                            + " between node "
                            + child
                            + " with node "
                            + parent);
        }
        if (child instanceof GraphLogicalSource
                        && ((GraphLogicalSource) child).getOpt() == GraphOpt.Source.EDGE
                || child instanceof GraphLogicalExpand) {
            Preconditions.checkArgument(
                    parent instanceof GraphLogicalGetV,
                    "graph generic type error: unable to establish an extension relationship"
                            + " between node %s with node %s",
                    child,
                    parent);
            GraphLogicalGetV getV = (GraphLogicalGetV) parent;
            GraphLabelType childLabelType = ((GraphSchemaType) getType(child)).getLabelType();
            GraphLabelType parentLabelType = ((GraphSchemaType) parentType).getLabelType();
            List<GraphLabelType.Entry> commonLabels =
                    commonLabels(childLabelType, parentLabelType, getV.getOpt(), false);
            return new GraphSchemaType(
                    GraphOpt.Source.VERTEX, new GraphLabelType(commonLabels), ImmutableList.of());
        }
        if (child instanceof GraphLogicalPathExpand) {
            Preconditions.checkArgument(
                    parent instanceof GraphLogicalGetV,
                    "graph generic type error: unable to establish an extension relationship"
                            + " between node %s with node %s",
                    child,
                    parent);
            GraphLogicalPathExpand pxd = (GraphLogicalPathExpand) child;
            RelDataType innerGetVType =
                    ((GraphPathType) getType(pxd)).getComponentType().getGetVType();
            GraphLabelType innerGetVLabelType = ((GraphSchemaType) innerGetVType).getLabelType();
            GraphLabelType outerGetVLabelType = ((GraphSchemaType) parentType).getLabelType();
            List<GraphLabelType.Entry> commonLabels =
                    commonLabels(innerGetVLabelType, outerGetVLabelType, false);
            return new GraphSchemaType(
                    GraphOpt.Source.VERTEX, new GraphLabelType(commonLabels), ImmutableList.of());
        }
        throw new IllegalArgumentException(
                "graph generic type error: unable to establish an extension relationship between"
                        + " node "
                        + child
                        + " with node "
                        + parent);
    }

    private List<GraphLabelType.Entry> commonLabels(
            GraphLabelType innerGetVType, GraphLabelType outerGetVType, boolean recordInner) {
        List<GraphLabelType.Entry> commonLabels = Lists.newArrayList();
        for (GraphLabelType.Entry entry1 : innerGetVType.getLabelsEntry()) {
            for (GraphLabelType.Entry entry2 : outerGetVType.getLabelsEntry()) {
                if (entry1.getLabel().equals(entry2.getLabel())) {
                    if (recordInner) {
                        commonLabels.add(entry1);
                    } else {
                        commonLabels.add(entry2);
                    }
                }
            }
        }
        Preconditions.checkArgument(
                !commonLabels.isEmpty(),
                "graph schema type error: unable to find common labels between %s and %s",
                innerGetVType,
                outerGetVType);
        return commonLabels;
    }

    private List<GraphLabelType.Entry> commonLabels(
            GraphLabelType expandType,
            GraphLabelType getVType,
            GraphOpt.GetV getVOpt,
            boolean recordExpand) {
        List<GraphLabelType.Entry> commonLabels = Lists.newArrayList();
        for (GraphLabelType.Entry entry1 : expandType.getLabelsEntry()) {
            for (GraphLabelType.Entry entry2 : getVType.getLabelsEntry()) {
                if (getVOpt != GraphOpt.GetV.START) {
                    if (entry1.getDstLabel().equals(entry2.getLabel())) {
                        if (recordExpand) {
                            commonLabels.add(entry1);
                        } else {
                            commonLabels.add(entry2);
                        }
                    }
                }
                if (getVOpt != GraphOpt.GetV.END) {
                    if (entry1.getSrcLabel().equals(entry2.getLabel())) {
                        if (recordExpand) {
                            commonLabels.add(entry1);
                        } else {
                            commonLabels.add(entry2);
                        }
                    }
                }
            }
        }
        Preconditions.checkArgument(
                !commonLabels.isEmpty(),
                "graph schema type error: unable to find common labels between expand type %s and"
                        + " getV type %s",
                expandType,
                getVType);
        return commonLabels;
    }

    private List<GraphLabelType.Entry> commonLabels(
            GraphLabelType getVType,
            GraphLabelType expandType,
            GraphOpt.Expand expandOpt,
            boolean recordGetV) {
        List<GraphLabelType.Entry> commonLabels = Lists.newArrayList();
        for (GraphLabelType.Entry entry1 : getVType.getLabelsEntry()) {
            for (GraphLabelType.Entry entry2 : expandType.getLabelsEntry()) {
                if (expandOpt != GraphOpt.Expand.OUT) {
                    if (entry1.getLabel().equals(entry2.getDstLabel())) {
                        if (recordGetV) {
                            commonLabels.add(entry1);
                        } else {
                            commonLabels.add(entry2);
                        }
                    }
                }
                if (expandOpt != GraphOpt.Expand.IN) {
                    if (entry1.getLabel().equals(entry2.getSrcLabel())) {
                        if (recordGetV) {
                            commonLabels.add(entry1);
                        } else {
                            commonLabels.add(entry2);
                        }
                    }
                }
            }
        }
        Preconditions.checkArgument(
                !commonLabels.isEmpty(),
                "graph schema type error: unable to find common labels between getV type %s and"
                        + " expand type %s",
                getVType,
                expandType);
        return commonLabels;
    }

    private RelNode newRel(RelNode rel, RelDataType newType) {
        if (rel instanceof GraphLogicalSource) {
            GraphLogicalSource source = (GraphLogicalSource) rel;
            return builder.source(
                            new SourceConfig(
                                    source.getOpt(),
                                    getLabelConfig(newType),
                                    source.getAliasName()))
                    .build();
        }
        if (rel instanceof GraphLogicalExpand) {
            GraphLogicalExpand expand = (GraphLogicalExpand) rel;
            GraphLogicalExpand newExpand =
                    (GraphLogicalExpand)
                            builder.push(expand.getInput(0))
                                    .expand(
                                            new ExpandConfig(
                                                    expand.getOpt(),
                                                    getLabelConfig(newType),
                                                    expand.getAliasName(),
                                                    expand.getStartAlias().getAliasName()))
                                    .build();
            newExpand.setRowType((GraphSchemaType) newType);
            return newExpand;
        }
        if (rel instanceof GraphLogicalGetV) {
            GraphLogicalGetV getV = (GraphLogicalGetV) rel;
            return builder.push(getV.getInput(0))
                    .getV(
                            new GetVConfig(
                                    getV.getOpt(),
                                    getLabelConfig(newType),
                                    getV.getAliasName(),
                                    getV.getStartAlias().getAliasName()))
                    .build();
        }
        if (rel instanceof GraphLogicalPathExpand) {
            GraphLogicalPathExpand pxd = (GraphLogicalPathExpand) rel;
            GraphLogicalExpand expand = (GraphLogicalExpand) pxd.getExpand();
            GraphLogicalGetV getV = (GraphLogicalGetV) pxd.getGetV();
            RelNode newExpand =
                    newRel(expand, ((GraphPathType) newType).getComponentType().getExpandType());
            if (ObjectUtils.isNotEmpty(expand.getFilters())) {
                newExpand = builder.push(newExpand).filter(expand.getFilters()).build();
            }
            RelNode newGetV =
                    newRel(getV, ((GraphPathType) newType).getComponentType().getGetVType());
            if (ObjectUtils.isNotEmpty(getV.getFilters())) {
                newGetV = builder.push(newGetV).filter(getV.getFilters()).build();
            }
            return GraphLogicalPathExpand.create(
                    (GraphOptCluster) builder.getCluster(),
                    ImmutableList.of(),
                    pxd.getInput(),
                    newExpand,
                    newGetV,
                    pxd.getOffset(),
                    pxd.getFetch(),
                    pxd.getResultOpt(),
                    pxd.getPathOpt(),
                    pxd.getAliasName(),
                    pxd.getStartAlias());
        }
        return rel;
    }

    private LabelConfig getLabelConfig(RelDataType type) {
        List<String> expectedLabels =
                ((GraphSchemaType) type)
                        .getLabelType().getLabelsEntry().stream()
                                .map(k -> k.getLabel())
                                .collect(Collectors.toList());
        LabelConfig labelConfig = new LabelConfig(false);
        expectedLabels.forEach(k -> labelConfig.addLabel(k));
        return labelConfig;
    }

    private RelDataType getType(RelNode node) {
        if (node instanceof AbstractBindableTableScan || node instanceof GraphLogicalPathExpand) {
            return node.getRowType().getFieldList().get(0).getType();
        }
        return node.getRowType();
    }

    private class RelGraph {
        private Map<String, List<RelNode>> aliasNameToRels;
        private List<RelNode> rels;
        private IdentityHashMap<RelNode, RelNode> relToParent;

        public RelGraph(RelNode rel) {
            initialize(rel);
        }

        public RelGraph(List<RelNode> rels) {
            for (RelNode rel : rels) {
                initialize(rel);
            }
        }

        private void initialize(RelNode rel) {
            rels.add(rel);
            List<RelNode> queue = Lists.newArrayList(rel);
            while (!queue.isEmpty()) {
                RelNode cur = queue.remove(0);
                String alias = getAliasName(cur);
                if (alias != null && alias != AliasInference.DEFAULT_NAME) {
                    aliasNameToRels.computeIfAbsent(alias, k -> Lists.newArrayList()).add(cur);
                }
                for (RelNode input : cur.getInputs()) {
                    relToParent.put(input, cur);
                    queue.add(input);
                }
            }
        }

        public List<RelNode> getNeighbors(RelNode rel) {
            AliasNameWithId startAlias = getStartAlias(rel);
            if (startAlias != null && startAlias.getAliasName() == AliasInference.DEFAULT_NAME) {
                RelNode input = rel.getInputs().isEmpty() ? null : rel.getInput(0);
                if (input != null) {
                    String alias = getAliasName(input);
                    if (alias != null && alias != AliasInference.DEFAULT_NAME) {
                        return aliasNameToRels.get(alias);
                    } else {
                        return Lists.newArrayList(input);
                    }
                } else {
                    return Lists.newArrayList();
                }
            } else if (startAlias != null) {
                return aliasNameToRels.get(startAlias.getAliasName());
            } else {
                return Lists.newArrayList();
            }
        }

        public @Nullable RelNode getParent(RelNode rel) {
            return relToParent.get(rel);
        }

        public List<RelNode> getRels() {
            return Collections.unmodifiableList(rels);
        }

        private @Nullable AliasNameWithId getStartAlias(RelNode rel) {
            if (rel instanceof AbstractBindableTableScan) {
                return ((AbstractBindableTableScan) rel).getStartAlias();
            } else if (rel instanceof GraphLogicalPathExpand) {
                return ((GraphLogicalPathExpand) rel).getStartAlias();
            }
            return null;
        }

        private @Nullable String getAliasName(RelNode rel) {
            if (rel instanceof AbstractBindableTableScan) {
                return ((AbstractBindableTableScan) rel).getAliasName();
            } else if (rel instanceof GraphLogicalPathExpand) {
                return ((GraphLogicalPathExpand) rel).getAliasName();
            }
            return null;
        }
    }
}

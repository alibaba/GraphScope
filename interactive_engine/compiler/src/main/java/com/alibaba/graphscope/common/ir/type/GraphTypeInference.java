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
import com.alibaba.graphscope.common.ir.rel.type.TableConfig;
import com.alibaba.graphscope.common.ir.tools.AliasInference;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.commons.lang3.ObjectUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.*;
import java.util.stream.Collectors;

/**
 * check and infer graph schema type for each graph operator in a {@code Match} node
 */
public class GraphTypeInference {
    private final GraphBuilder builder;

    public GraphTypeInference(GraphBuilder builder) {
        this.builder = builder;
    }

    /**
     * check and infer graph schema type for each graph operator in a single sentence.
     * i.e. for the query 'Match (a)-[:knows]->(b)', types of a and b can be derived from 'knows' as 'person'
     * @param top
     * @return
     */
    public RelNode inferTypes(RelNode top) {
        return visitRels(ImmutableList.of(top)).get(0);
    }

    /**
     * check and infer graph schema type for each graph operator in multiple sentences.
     * i.e. for the query 'Match (a)-[:knows]->(b), (b)-[c]->(d)', firstly types of a and b can be derived from 'knows' as 'person',
     * then types of c and d can be derived from 'b' further, which should be 'knows' and 'person'.
     * @param sentences
     * @return
     */
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
                                                restrictChild(relGraph, k, top, restriction),
                                                visited,
                                                updated))
                        .collect(Collectors.toList());
        RelDataType newType = restrictParent(relGraph, newNeighbors, top, restriction);
        String alias = relGraph.getAliasName(top);
        if (alias != null && alias != AliasInference.DEFAULT_NAME) {
            List<RelNode> relsWithSameAlias = relGraph.aliasNameToRels.get(alias);
            if (ObjectUtils.isNotEmpty(relsWithSameAlias)) {
                newType = restrictShared(relsWithSameAlias, top, newType);
            }
        }
        RelDataType oldType = getType(top);
        if (!typeEquals(oldType, newType)) {
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

    private boolean typeEquals(RelDataType type1, RelDataType type2) {
        if (type1 instanceof GraphSchemaType && type2 instanceof GraphSchemaType) {
            return ((GraphSchemaType) type1)
                    .getLabelType()
                    .equals(((GraphSchemaType) type2).getLabelType());
        } else if (type1 instanceof GraphPathType && type2 instanceof GraphPathType) {
            GraphPathType pathType1 = (GraphPathType) type1;
            GraphPathType pathType2 = (GraphPathType) type2;
            return typeEquals(
                            pathType1.getComponentType().getExpandType(),
                            pathType2.getComponentType().getExpandType())
                    && typeEquals(
                            pathType1.getComponentType().getGetVType(),
                            pathType2.getComponentType().getGetVType());
        }
        return true;
    }

    private RelDataType restrictChild(
            RelGraph relGraph, RelNode child, RelNode parent, RelDataType parentType) {
        if (child instanceof GraphLogicalSource
                        && ((GraphLogicalSource) child).getOpt() == GraphOpt.Source.VERTEX
                || child instanceof GraphLogicalGetV) {
            if (parent instanceof GraphLogicalPathExpand) {
                parent = ((GraphLogicalPathExpand) parent).getExpand();
                parentType = ((GraphPathType) parentType).getComponentType().getExpandType();
            }
            if (parent instanceof GraphLogicalExpand) {
                GraphLogicalExpand expand = (GraphLogicalExpand) parent;
                GraphSchemaType childType = (GraphSchemaType) getType(child);
                GraphLabelType childLabelType = childType.getLabelType();
                GraphLabelType parentLabelType = ((GraphSchemaType) parentType).getLabelType();
                List<GraphLabelType.Entry> commonLabels =
                        commonLabels(childLabelType, parentLabelType, expand.getOpt(), true);
                return createSchemaType(GraphOpt.Source.VERTEX, commonLabels, childType);
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
            GraphSchemaType childType = (GraphSchemaType) getType(child);
            GraphLabelType childLabelType = childType.getLabelType();
            GraphLabelType parentLabelType = ((GraphSchemaType) parentType).getLabelType();
            GraphLabelType otherVLabelType = null;
            if (getV.getOpt() == GraphOpt.GetV.OTHER) {
                RelDataType otherVType = relGraph.getNeighborsType(child);
                Preconditions.checkArgument(
                        otherVType != null && otherVType instanceof GraphSchemaType,
                        "graph generic type error: invalid opt %s in node %s",
                        getV.getOpt(),
                        getV);
                otherVLabelType = ((GraphSchemaType) otherVType).getLabelType();
            }
            List<GraphLabelType.Entry> commonLabels =
                    commonLabels(
                            otherVLabelType, childLabelType, parentLabelType, getV.getOpt(), true);
            return createSchemaType(GraphOpt.Source.EDGE, commonLabels, childType);
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
                    commonLabels(innerGetVLabelType, outerGetVLabelType);
            RelDataType newGetVType =
                    createSchemaType(
                            GraphOpt.Source.VERTEX, commonLabels, (GraphSchemaType) innerGetVType);
            RelDataType newExpandType =
                    restrictChild(
                            relGraph,
                            pxd.getExpand().copy(pxd.getTraitSet(), pxd.getInputs()),
                            pxd.getGetV(),
                            newGetVType);
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
            RelGraph relGraph, List<RelNode> children, RelNode parent, RelDataType parentType) {
        for (RelNode child : children) {
            parentType = restrictParent(relGraph, child, parent, parentType);
        }
        return parentType;
    }

    private RelDataType restrictParent(
            RelGraph relGraph, RelNode child, RelNode parent, RelDataType parentType) {
        if (child instanceof GraphLogicalSource
                        && ((GraphLogicalSource) child).getOpt() == GraphOpt.Source.VERTEX
                || child instanceof GraphLogicalGetV) {
            if (parent instanceof GraphLogicalExpand) {
                GraphLogicalExpand expand = (GraphLogicalExpand) parent;
                GraphLabelType childLabelType = ((GraphSchemaType) getType(child)).getLabelType();
                GraphLabelType parentLabelType = ((GraphSchemaType) parentType).getLabelType();
                List<GraphLabelType.Entry> commonLabels =
                        commonLabels(childLabelType, parentLabelType, expand.getOpt(), false);
                return createSchemaType(
                        GraphOpt.Source.EDGE, commonLabels, (GraphSchemaType) parentType);
            }
            if (parent instanceof GraphLogicalPathExpand) {
                GraphLogicalPathExpand pxd = (GraphLogicalPathExpand) parent;
                RelDataType newExpandType =
                        restrictParent(
                                relGraph,
                                child,
                                pxd.getExpand(),
                                ((GraphPathType) parentType).getComponentType().getExpandType());
                RelNode oldExpand =
                        pxd.getExpand()
                                .copy(pxd.getExpand().getTraitSet(), ImmutableList.of(child));
                RelNode newExpand = newRel(oldExpand, newExpandType);
                RelDataType newGetVType =
                        restrictParent(
                                relGraph,
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
            GraphLabelType otherVLabelType = null;
            if (getV.getOpt() == GraphOpt.GetV.OTHER) {
                RelDataType otherVType = relGraph.getNeighborsType(child);
                Preconditions.checkArgument(
                        otherVType != null && otherVType instanceof GraphSchemaType,
                        "graph generic type error: invalid opt %s in node %s",
                        getV.getOpt(),
                        getV);
                otherVLabelType = ((GraphSchemaType) otherVType).getLabelType();
            }
            List<GraphLabelType.Entry> commonLabels =
                    commonLabels(
                            otherVLabelType, childLabelType, parentLabelType, getV.getOpt(), false);
            return createSchemaType(
                    GraphOpt.Source.VERTEX, commonLabels, (GraphSchemaType) parentType);
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
                    commonLabels(innerGetVLabelType, outerGetVLabelType);
            return createSchemaType(
                    GraphOpt.Source.VERTEX, commonLabels, (GraphSchemaType) parentType);
        }
        throw new IllegalArgumentException(
                "graph generic type error: unable to establish an extension relationship between"
                        + " node "
                        + child
                        + " with node "
                        + parent);
    }

    private RelDataType restrictShared(
            List<RelNode> rels, @Nullable RelNode shared, RelDataType sharedType) {
        if (sharedType instanceof GraphSchemaType) {
            GraphLabelType sharedLabelType = ((GraphSchemaType) sharedType).getLabelType();
            for (RelNode rel : rels) {
                RelDataType relType = getType(rel);
                Preconditions.checkArgument(
                        relType instanceof GraphSchemaType
                                && ((GraphSchemaType) relType).getScanOpt()
                                        == ((GraphSchemaType) sharedType).getScanOpt(),
                        "graph schema type error : rel type %s is not compatible with shared type"
                                + " %s",
                        relType,
                        sharedType);
                GraphLabelType relLabelType = ((GraphSchemaType) relType).getLabelType();
                sharedLabelType = new GraphLabelType(commonLabels(relLabelType, sharedLabelType));
            }
            return createSchemaType(
                    ((GraphSchemaType) sharedType).getScanOpt(),
                    sharedLabelType.getLabelsEntry(),
                    (GraphSchemaType) sharedType);
        }
        if (sharedType instanceof GraphPathType) {
            List<RelNode> expandRels = Lists.newArrayList();
            List<RelNode> getVRels = Lists.newArrayList();
            for (RelNode rel : rels) {
                Preconditions.checkArgument(
                        rel instanceof GraphLogicalPathExpand,
                        "graph schema type error : rel %s is not compatible with shared type %s",
                        rel,
                        sharedType);
                expandRels.add(((GraphLogicalPathExpand) rel).getExpand());
                getVRels.add(((GraphLogicalPathExpand) rel).getGetV());
            }
            RelDataType restrictExpand =
                    restrictShared(
                            expandRels,
                            null,
                            ((GraphPathType) sharedType).getComponentType().getExpandType());
            RelDataType restrictGetV =
                    restrictShared(
                            getVRels,
                            null,
                            ((GraphPathType) sharedType).getComponentType().getGetVType());
            return new GraphPathType(new GraphPathType.ElementType(restrictExpand, restrictGetV));
        }
        throw new IllegalArgumentException(
                "graph schema type error: unable to restrict shared type " + sharedType);
    }

    private List<GraphLabelType.Entry> commonLabels(
            GraphLabelType labelType1, GraphLabelType labelType2) {
        List<GraphLabelType.Entry> commonLabels = Lists.newArrayList(labelType1.getLabelsEntry());
        commonLabels.retainAll(labelType2.getLabelsEntry());
        Preconditions.checkArgument(
                !commonLabels.isEmpty(),
                "graph schema type error: unable to find common labels between %s and %s",
                labelType1,
                labelType2);
        return commonLabels;
    }

    private List<GraphLabelType.Entry> commonLabels(
            @Nullable GraphLabelType otherVType,
            GraphLabelType expandType,
            GraphLabelType getVType,
            GraphOpt.GetV getVOpt,
            boolean recordExpand) {
        List<GraphLabelType.Entry> commonLabels = Lists.newArrayList();
        for (GraphLabelType.Entry entry1 : expandType.getLabelsEntry()) {
            for (GraphLabelType.Entry entry2 : getVType.getLabelsEntry()) {
                if (getVOpt == GraphOpt.GetV.START || getVOpt == GraphOpt.GetV.BOTH) {
                    if (entry1.getSrcLabel().equals(entry2.getLabel())) {
                        if (recordExpand) {
                            commonLabels.add(entry1);
                        } else {
                            commonLabels.add(entry2);
                        }
                    }
                }
                if (getVOpt == GraphOpt.GetV.END || getVOpt == GraphOpt.GetV.BOTH) {
                    if (entry1.getDstLabel().equals(entry2.getLabel())) {
                        if (recordExpand) {
                            commonLabels.add(entry1);
                        } else {
                            commonLabels.add(entry2);
                        }
                    }
                }
                if (getVOpt == GraphOpt.GetV.OTHER && otherVType != null) {
                    for (GraphLabelType.Entry entry3 : otherVType.getLabelsEntry()) {
                        if (entry1.getSrcLabel().equals(entry3.getLabel())
                                        && entry1.getDstLabel().equals(entry2.getLabel())
                                || entry1.getDstLabel().equals(entry3.getLabel())
                                        && entry1.getSrcLabel().equals(entry2.getLabel())) {
                            if (recordExpand) {
                                commonLabels.add(entry1);
                            } else {
                                commonLabels.add(entry2);
                            }
                        }
                    }
                }
            }
        }
        commonLabels = commonLabels.stream().distinct().collect(Collectors.toList());
        String errorMsg;
        if (commonLabels.isEmpty()) {
            switch (getVOpt) {
                case OTHER:
                    errorMsg =
                            String.format(
                                    "graph schema type error: unable to find expand with [type=%s]"
                                            + " between getV with [type=%s] and getV with [opt=%s,"
                                            + " type=%s]",
                                    expandType, otherVType, getVOpt, getVType);
                    break;
                case START:
                case END:
                case BOTH:
                default:
                    errorMsg =
                            String.format(
                                    "graph schema type error: unable to find getV with [opt=%s,"
                                            + " type=%s] from expand with [type=%s]",
                                    getVOpt, getVType, expandType);
            }
            throw new IllegalArgumentException(errorMsg);
        }
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
        commonLabels = commonLabels.stream().distinct().collect(Collectors.toList());
        Preconditions.checkArgument(
                !commonLabels.isEmpty(),
                "graph schema type error: unable to find getV with [type=%s] from expand with"
                        + " [opt=%s, type=%s]",
                getVType,
                expandOpt,
                expandType);
        return commonLabels;
    }

    private RelNode newRel(RelNode rel, RelDataType newType) {
        if (rel instanceof GraphLogicalSource) {
            GraphLogicalSource source = (GraphLogicalSource) rel;
            TableConfig newTableConfig = newTableConfig((GraphLogicalSource) rel, newType);
            if (newTableConfig != source.getTableConfig()) {
                GraphLogicalSource newSource =
                        GraphLogicalSource.create(
                                (GraphOptCluster) builder.getCluster(),
                                ImmutableList.of(),
                                source.getOpt(),
                                newTableConfig,
                                source.getAliasName());
                List<RexNode> filters = Lists.newArrayList();
                if (source.getUniqueKeyFilters() != null) {
                    filters.add(source.getUniqueKeyFilters());
                }
                if (ObjectUtils.isNotEmpty(source.getFilters())) {
                    filters.addAll(source.getFilters());
                }
                if (!filters.isEmpty()) {
                    return builder.push(newSource).filter(filters).build();
                }
                return newSource;
            }
        }
        if (rel instanceof GraphLogicalExpand) {
            GraphLogicalExpand expand = (GraphLogicalExpand) rel;
            TableConfig newTableConfig = newTableConfig((GraphLogicalExpand) rel, newType);
            if (newTableConfig != expand.getTableConfig()) {
                GraphLogicalExpand newExpand =
                        GraphLogicalExpand.create(
                                (GraphOptCluster) builder.getCluster(),
                                ImmutableList.of(),
                                expand.getInputs().isEmpty() ? null : expand.getInput(0),
                                expand.getOpt(),
                                newTableConfig,
                                expand.getAliasName(),
                                expand.getStartAlias());
                newExpand.setRowType((GraphSchemaType) newType);
                if (ObjectUtils.isNotEmpty(expand.getFilters())) {
                    return builder.push(newExpand).filter(expand.getFilters()).build();
                }
                return newExpand;
            } else {
                expand.setRowType((GraphSchemaType) newType);
            }
        }
        if (rel instanceof GraphLogicalGetV) {
            GraphLogicalGetV getV = (GraphLogicalGetV) rel;
            TableConfig newTableConfig = newTableConfig((GraphLogicalGetV) rel, newType);
            if (newTableConfig != getV.getTableConfig()) {
                GraphLogicalGetV newGetV =
                        GraphLogicalGetV.create(
                                (GraphOptCluster) builder.getCluster(),
                                ImmutableList.of(),
                                getV.getInputs().isEmpty() ? null : getV.getInput(0),
                                getV.getOpt(),
                                newTableConfig,
                                getV.getAliasName(),
                                getV.getStartAlias());
                if (ObjectUtils.isNotEmpty(getV.getFilters())) {
                    return builder.push(newGetV).filter(getV.getFilters()).build();
                }
                return newGetV;
            }
        }
        if (rel instanceof GraphLogicalPathExpand) {
            GraphLogicalPathExpand pxd = (GraphLogicalPathExpand) rel;
            RelNode newExpand =
                    newRel(
                            pxd.getExpand(),
                            ((GraphPathType) newType).getComponentType().getExpandType());
            RelNode newGetV =
                    newRel(
                            pxd.getGetV(),
                            ((GraphPathType) newType).getComponentType().getGetVType());
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

    private TableConfig newTableConfig(AbstractBindableTableScan rel, RelDataType newType) {
        List<RelOptTable> oldTables = rel.getTableConfig().getTables();
        List<RelOptTable> newTables = Lists.newArrayList();
        List<String> newLabels =
                ((GraphSchemaType) newType)
                        .getLabelType().getLabelsEntry().stream()
                                .map(k -> k.getLabel())
                                .collect(Collectors.toList());
        oldTables.forEach(
                k -> {
                    if (k.getQualifiedName().size() > 0
                            && newLabels.contains(k.getQualifiedName().get(0))) {
                        newTables.add(k);
                    }
                });
        if (newTables.size() < oldTables.size()) {
            return new TableConfig(newTables);
        }
        return rel.getTableConfig();
    }

    private RelDataType getType(RelNode node) {
        if (node instanceof AbstractBindableTableScan || node instanceof GraphLogicalPathExpand) {
            return node.getRowType().getFieldList().get(0).getType();
        }
        return node.getRowType();
    }

    private GraphSchemaType createSchemaType(
            GraphOpt.Source opt,
            List<GraphLabelType.Entry> newLabels,
            @Nullable GraphSchemaType originalType) {
        boolean isNullable = originalType == null ? false : originalType.isNullable();
        if (newLabels.size() == 1) {
            return new GraphSchemaType(
                    opt, new GraphLabelType(newLabels), ImmutableList.of(), isNullable);
        } else {
            List<GraphSchemaType> fuzzyTypes =
                    newLabels.stream()
                            .map(
                                    k ->
                                            new GraphSchemaType(
                                                    opt,
                                                    new GraphLabelType(ImmutableList.of(k)),
                                                    ImmutableList.of()))
                            .collect(Collectors.toList());
            return GraphSchemaType.create(fuzzyTypes, builder.getTypeFactory(), isNullable);
        }
    }

    private class RelGraph {
        private final Map<String, List<RelNode>> aliasNameToRels;
        private final List<RelNode> rels;
        private final IdentityHashMap<RelNode, RelNode> relToParent;

        public RelGraph(RelNode rel) {
            this(ImmutableList.of(rel));
        }

        public RelGraph(List<RelNode> rels) {
            this.aliasNameToRels = Maps.newHashMap();
            this.rels = Lists.newArrayList();
            this.relToParent = new IdentityHashMap<>();
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

        public @Nullable RelDataType getNeighborsType(RelNode rel) {
            List<RelNode> neighbors = getNeighbors(rel);
            if (!neighbors.isEmpty()) {
                return restrictShared(neighbors, null, getType(neighbors.get(0)));
            }
            return null;
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

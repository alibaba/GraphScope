/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.compiler.cost.statistics;

import com.alibaba.maxgraph.compiler.api.schema.EdgeRelation;
import com.alibaba.maxgraph.compiler.api.schema.GraphElement;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.tree.BaseTreeNode;
import com.alibaba.maxgraph.compiler.tree.EdgeOtherVertexTreeNode;
import com.alibaba.maxgraph.compiler.tree.EdgeTreeNode;
import com.alibaba.maxgraph.compiler.tree.EdgeVertexTreeNode;
import com.alibaba.maxgraph.compiler.tree.TreeNode;
import com.alibaba.maxgraph.compiler.tree.VertexTreeNode;
import com.alibaba.maxgraph.compiler.tree.source.SourceEdgeTreeNode;
import com.alibaba.maxgraph.compiler.tree.source.SourceVertexTreeNode;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class NodeLabelList {
    private Set<String> vertexLabelList = Sets.newHashSet();
    private Set<String> edgeLabelList = Sets.newHashSet();
    private boolean unknownFlag = false;

    public void addVertexLabel(String label) {
        this.vertexLabelList.add(label);
    }

    public void addVertexLabel(Set<String> labelList) {
        this.vertexLabelList.addAll(labelList);
    }

    public void addEdgeLabel(String label) {
        this.edgeLabelList.add(label);
    }

    public void addEdgeLabel(Set<String> labelList) {
        this.edgeLabelList.addAll(labelList);
    }

    public Set<String> getVertexLabelList() {
        return vertexLabelList;
    }

    public Set<String> getEdgeLabelList() {
        return edgeLabelList;
    }

    public void enableUnknown() {
        this.unknownFlag = true;
    }

    public boolean isUnknownFlag() {
        return this.unknownFlag;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("vertexLabelList", vertexLabelList)
                .add("edgeLabelList", edgeLabelList)
                .add("unknownFlag", unknownFlag)
                .toString();
    }

    /**
     * Build label list of current node from parent node, tree node and schema
     *
     * @param parentNodeLabel The given parent node
     * @param treeNode        The given tree node
     * @param graphSchema     The given graph schema
     * @return The result node label list
     */
    public static NodeLabelList buildNodeLabel(NodeLabelList parentNodeLabel,
                                               TreeNode treeNode,
                                               GraphSchema graphSchema) {
        NodeLabelList nodeLabelList = new NodeLabelList();
        if (null == parentNodeLabel) {
            BaseTreeNode baseTreeNode = (BaseTreeNode) treeNode;
            final Set<String> labelList = Sets.newHashSet();
            baseTreeNode.getHasContainerList().forEach(v -> {
                if (StringUtils.equals(v.getKey(), T.label.getAccessor())) {
                    if (v.getBiPredicate() instanceof Compare &&
                            v.getBiPredicate() == Compare.eq) {
                        labelList.add(v.getValue().toString());
                    } else if (v.getBiPredicate() instanceof Contains &&
                            v.getBiPredicate() == Contains.within) {
                        List<String> labelValueList = (List<String>) v.getValue();
                        labelList.addAll(labelValueList);
                    } else {
                        throw new IllegalArgumentException("Not support label compare " + v.toString());
                    }
                }
            });
            if (treeNode instanceof SourceVertexTreeNode) {
                List<String> vertexLabelList = graphSchema.getVertexList()
                        .stream()
                        .map(GraphElement::getLabel)
                        .collect(Collectors.toList());
                Set<String> resultLabelList;
                if (labelList.isEmpty()) {
                    resultLabelList = Sets.newHashSet(vertexLabelList);
                } else {
                    resultLabelList = labelList.stream().filter(vertexLabelList::contains).collect(Collectors.toSet());
                }
                nodeLabelList.addVertexLabel(resultLabelList);
            } else if (treeNode instanceof SourceEdgeTreeNode) {
                List<String> edgeLabelList = graphSchema.getEdgeList()
                        .stream()
                        .map(GraphElement::getLabel)
                        .collect(Collectors.toList());
                Set<String> resultLabelList;
                if (labelList.isEmpty()) {
                    resultLabelList = Sets.newHashSet(edgeLabelList);
                } else {
                    resultLabelList = labelList.stream().filter(edgeLabelList::contains).collect(Collectors.toSet());
                }
                nodeLabelList.addEdgeLabel(resultLabelList);
            } else {
                nodeLabelList.enableUnknown();
            }
        } else {
            Set<String> parentVertexLabelList = Sets.newHashSet();
            Set<String> parentEdgeLabelList = Sets.newHashSet();
            if (parentNodeLabel.isUnknownFlag()) {
                parentVertexLabelList.addAll(graphSchema.getVertexList()
                        .stream()
                        .map(GraphElement::getLabel)
                        .collect(Collectors.toList()));
                parentEdgeLabelList.addAll(graphSchema.getEdgeList()
                        .stream()
                        .map(GraphElement::getLabel)
                        .collect(Collectors.toList()));
            } else {
                parentVertexLabelList.addAll(parentNodeLabel.getVertexLabelList());
                parentEdgeLabelList.addAll(parentNodeLabel.getEdgeLabelList());
            }

            if (treeNode instanceof VertexTreeNode) {
                VertexTreeNode vertexTreeNode = (VertexTreeNode) treeNode;
                Direction direction = vertexTreeNode.getDirection();
                nodeLabelList.addVertexLabel(computeVertexLabelList(parentVertexLabelList, direction, vertexTreeNode.getEdgeLabels(), graphSchema));
            } else if (treeNode instanceof EdgeTreeNode) {
                EdgeTreeNode edgeTreeNode = (EdgeTreeNode) treeNode;
                Direction direction = edgeTreeNode.getDirection();
                nodeLabelList.addEdgeLabel(computeEdgeLabelList(parentVertexLabelList, direction, edgeTreeNode.getEdgeLabels(), graphSchema));
            } else if (treeNode instanceof EdgeVertexTreeNode) {
                EdgeVertexTreeNode edgeVertexTreeNode = (EdgeVertexTreeNode) treeNode;
                Map<String, Pair<Set<String>, Set<String>>> edgeSourceTargetPairList = getEdgeSourceTargetPairList(parentEdgeLabelList.toArray(new String[0]), graphSchema);
                Direction direction = edgeVertexTreeNode.getDirection();
                edgeSourceTargetPairList.forEach((key, value) -> {
                    switch (direction) {
                        case OUT: {
                            nodeLabelList.addVertexLabel(value.getLeft());
                            break;
                        }
                        case IN: {
                            nodeLabelList.addVertexLabel(value.getRight());
                            break;
                        }
                        case BOTH: {
                            nodeLabelList.addVertexLabel(value.getLeft());
                            nodeLabelList.addVertexLabel(value.getRight());
                            break;
                        }
                    }
                });
            } else if (treeNode instanceof EdgeOtherVertexTreeNode) {
                Map<String, Pair<Set<String>, Set<String>>> edgeSourceTargetPairList = getEdgeSourceTargetPairList(parentEdgeLabelList.toArray(new String[0]), graphSchema);
                edgeSourceTargetPairList.forEach((key, value) -> {
                    nodeLabelList.addVertexLabel(value.getLeft());
                    nodeLabelList.addVertexLabel(value.getRight());
                });
            } else {
                nodeLabelList.enableUnknown();
            }
        }

        return nodeLabelList;
    }

    /**
     * Compute result edge list with given paren
     *
     * @param parentVertexLabelList The given parent vertex label list
     * @param direction             The given direction
     * @param edgeLabels            The given edge labels
     * @param schema                The given schema
     * @return The result edge label list
     */
    private static Set<String> computeEdgeLabelList(Set<String> parentVertexLabelList,
                                                    Direction direction,
                                                    String[] edgeLabels,
                                                    GraphSchema schema) {
        Set<String> resultEdgeLabelList = Sets.newHashSet();
        Map<String, Pair<Set<String>, Set<String>>> edgeSourceTargetPairList = getEdgeSourceTargetPairList(edgeLabels, schema);
        switch (direction) {
            case OUT: {
                resultEdgeLabelList.addAll(edgeSourceTargetPairList
                        .entrySet()
                        .stream()
                        .filter(v -> CollectionUtils.containsAny(v.getValue().getLeft(), parentVertexLabelList))
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toSet()));
                break;
            }
            case IN: {
                resultEdgeLabelList.addAll(edgeSourceTargetPairList
                        .entrySet()
                        .stream()
                        .filter(v -> CollectionUtils.containsAny(v.getValue().getRight(), parentVertexLabelList))
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toSet()));
                break;
            }
            case BOTH: {
                resultEdgeLabelList.addAll(edgeSourceTargetPairList
                        .entrySet()
                        .stream()
                        .filter(v -> CollectionUtils.containsAny(v.getValue().getLeft(), parentVertexLabelList)
                                || CollectionUtils.containsAny(v.getValue().getRight(), parentVertexLabelList))
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toSet()));
                break;
            }
        }

        return resultEdgeLabelList;
    }

    private static Map<String, Pair<Set<String>, Set<String>>> getEdgeSourceTargetPairList(String[] edgeLabels, GraphSchema schema) {
        Map<String, Pair<Set<String>, Set<String>>> edgeSourceTargetPairList = Maps.newHashMap();

        if (null == edgeLabels || edgeLabels.length == 0) {
            schema.getEdgeList().forEach(v -> {
                Set<String> sourceVertexList = Sets.newHashSet();
                Set<String> targetVertexList = Sets.newHashSet();
                v.getRelationList().forEach(vv -> {
                    sourceVertexList.add(vv.getSource().getLabel());
                    targetVertexList.add(vv.getTarget().getLabel());
                });
                edgeSourceTargetPairList.put(v.getLabel(), Pair.of(sourceVertexList, targetVertexList));
            });
        } else {
            final Set<String> currEdgeList = Sets.newHashSet(edgeLabels);
            schema.getEdgeList()
                    .stream()
                    .filter(v -> currEdgeList.contains(v.getLabel()))
                    .forEach(v -> {
                        Set<String> sourceVertexList = Sets.newHashSet();
                        Set<String> targetVertexList = Sets.newHashSet();
                        v.getRelationList().forEach(vv -> {
                            sourceVertexList.add(vv.getSource().getLabel());
                            targetVertexList.add(vv.getTarget().getLabel());
                        });
                        edgeSourceTargetPairList.put(v.getLabel(), Pair.of(sourceVertexList, targetVertexList));
                    });
        }
        return edgeSourceTargetPairList;
    }

    /**
     * Compute result vertex label list with given parent
     *
     * @param parentVertexLabelList The given parent vertex label list
     * @param direction             The direction
     * @param edgeLabels            The edge label list
     * @param schema                The given schema
     * @return The result vertex label list
     */
    private static Set<String> computeVertexLabelList(Set<String> parentVertexLabelList,
                                                      Direction direction,
                                                      String[] edgeLabels,
                                                      GraphSchema schema) {
        Set<String> resultVertexLabelList = Sets.newHashSet();
        List<EdgeRelation> relationList = Lists.newArrayList();
        if (null == edgeLabels || edgeLabels.length == 0) {
            schema.getEdgeList().forEach(v -> relationList.addAll(v.getRelationList()));
        } else {
            final Set<String> currEdgeList = Sets.newHashSet(edgeLabels);
            schema.getEdgeList()
                    .stream()
                    .filter(v -> currEdgeList.contains(v.getLabel()))
                    .forEach(v -> relationList.addAll(v.getRelationList()));
        }
        switch (direction) {
            case OUT: {
                resultVertexLabelList.addAll(relationList.stream()
                        .filter(v -> parentVertexLabelList.contains(v.getSource().getLabel()))
                        .map(v -> v.getTarget().getLabel())
                        .collect(Collectors.toSet()));
                break;
            }
            case IN: {
                resultVertexLabelList.addAll(relationList.stream()
                        .filter(v -> parentVertexLabelList.contains(v.getTarget().getLabel()))
                        .map(v -> v.getSource().getLabel())
                        .collect(Collectors.toSet()));
                break;
            }
            case BOTH: {
                resultVertexLabelList.addAll(relationList.stream()
                        .filter(v -> parentVertexLabelList.contains(v.getSource().getLabel()) ||
                                parentVertexLabelList.contains(v.getTarget().getLabel()))
                        .flatMap(v -> Sets.newHashSet(v.getSource().getLabel(), v.getTarget().getLabel()).stream())
                        .collect(Collectors.toSet()));
                break;
            }
        }
        return resultVertexLabelList;
    }
}

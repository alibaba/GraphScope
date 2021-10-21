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
package com.alibaba.maxgraph.compiler.cost;

import com.alibaba.maxgraph.compiler.tree.RepeatTreeNode;
import com.alibaba.maxgraph.compiler.tree.SelectOneTreeNode;
import com.alibaba.maxgraph.compiler.tree.SelectTreeNode;
import com.alibaba.maxgraph.compiler.tree.TreeNode;
import com.alibaba.maxgraph.compiler.tree.TreeNodeLabelManager;
import com.alibaba.maxgraph.compiler.tree.UnaryTreeNode;
import com.alibaba.maxgraph.compiler.tree.UnionTreeNode;
import com.alibaba.maxgraph.compiler.tree.WherePredicateTreeNode;
import com.alibaba.maxgraph.compiler.tree.source.SourceTreeNode;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Map;

public class CostUtils {
    private static final String VALUE_SUFFIX = "@value";

    public static String buildValueName(String label) {
        return label + VALUE_SUFFIX;
    }

    private static List<RowField> buildRowFieldList(String label, boolean labelValueFlag) {
        List<RowField> rowFieldList = Lists.newArrayList();
        rowFieldList.add(new RowField(label));
        if (labelValueFlag) {
            rowFieldList.add(new RowField(buildValueName(label)));
        }

        return rowFieldList;
    }

    private static CostGraph buildLabelCostGraph(String label,
                                                 TreeNode startNode,
                                                 TreeNode selectNode,
                                                 CostMappingManager costMappingManager,
                                                 boolean labelValueFlag) {
        CostGraph costGraph = new CostGraph(costMappingManager);
        TreeNode currNode = selectNode;
        boolean foundFirstFlag = false;
        while (currNode instanceof UnaryTreeNode) {
            if (foundFirstFlag) {
                costGraph.addFirstRow(new CostRow(Lists.newArrayList()));
                currNode = ((UnaryTreeNode) currNode).getInputNode();
                continue;
            }

            if (currNode == selectNode) {
                if (labelValueFlag) {
                    costGraph.addFirstRow(new CostRow(Lists.newArrayList(new RowField(buildValueName(label)))));
                } else {
                    costGraph.addFirstRow(new CostRow(Lists.newArrayList(new RowField(label))));
                }
            } else {
                List<RowField> rowFieldList = currNode.getOutputNode() == selectNode ?
                        Lists.newArrayList(new RowField(buildValueName(label))) : buildRowFieldList(label, labelValueFlag);
                foundFirstFlag = currNode == startNode;
                costGraph.addFirstRow(new CostRow(rowFieldList, foundFirstFlag));
            }

            currNode = ((UnaryTreeNode) currNode).getInputNode();
        }
        if (!foundFirstFlag) {
            List<RowField> rowFieldList = buildRowFieldList(label, labelValueFlag);
            costGraph.addFirstRow(new CostRow(rowFieldList, true));
        } else {
            costGraph.addFirstRow(new CostRow(Lists.newArrayList()));
        }

        return costGraph;
    }

    public static CostGraph buildCostGraph(TreeNode leafNode, TreeNodeLabelManager treeNodeLabelManager) {
        TreeNode currNode = leafNode;
        CostMappingManager costMappingManager = new CostMappingManager();
        CostGraph costGraph = new CostGraph(costMappingManager);
        CostEstimate costEstimate = new CostEstimate();

        while (currNode instanceof UnaryTreeNode) {
            if (currNode instanceof SelectTreeNode) {
                SelectTreeNode selectTreeNode = (SelectTreeNode) currNode;
                Map<String, TreeNode> labelStartNodeList = selectTreeNode.getLabelTreeNodeList();

                List<String> selectKeyList = selectTreeNode.getSelectKeyList();
                Map<String, TreeNode> labelValueNodeList = selectTreeNode.getLabelValueTreeNodeList();
                for (String label : selectKeyList) {
                    TreeNode labelValueNode = labelValueNodeList.get(label);
                    TreeNode labelStartNode = labelStartNodeList.get(label);
                    if (labelStartNode == null) {
                        continue;
                    }

                    double nodeSelfComputeCost = costEstimate.estimateComputeCost(labelStartNode, labelStartNode);
                    double startNodeNetworkCost = costEstimate.estimateNetworkCost(labelStartNode);
                    costMappingManager.addComputeCost(Pair.of(label, label), nodeSelfComputeCost);
                    costMappingManager.addValueNetworkCost(label, startNodeNetworkCost);
                    if (labelValueNode != null && !(labelValueNode instanceof SourceTreeNode)) {
                        costMappingManager.addComputeTree(label, labelValueNode);

                        double nodeValueComputeCost = costEstimate.estimateComputeCost(labelStartNode, labelValueNode);
                        double nodeValueNetworkCost = costEstimate.estimateNetworkCost(labelValueNode);

                        String labelValueTag = CostUtils.buildValueName(label);
                        costMappingManager.addValueParent(labelValueTag, label);
                        costMappingManager.addComputeCost(Pair.of(label, labelValueTag), nodeValueComputeCost);
                        costMappingManager.addValueNetworkCost(labelValueTag, nodeValueNetworkCost);
                    }
                    CostGraph currGraph = buildLabelCostGraph(label,
                            labelStartNode,
                            currNode,
                            costMappingManager,
                            labelValueNode != null && !(labelValueNode instanceof SourceTreeNode));
                    costGraph.mergeCostGraph(currGraph);
                }
            } else if (currNode instanceof SelectOneTreeNode) {
                SelectOneTreeNode selectOneTreeNode = (SelectOneTreeNode) currNode;
                String label = selectOneTreeNode.getSelectLabel();
                TreeNode labelStartNode = selectOneTreeNode.getLabelStartTreeNode();
                TreeNode labelValueNode = selectOneTreeNode.getTraversalTreeNode();
                if (null == labelStartNode || null == labelValueNode) {
                    break;
                }

                double nodeSelfComputeCost = costEstimate.estimateComputeCost(labelStartNode, labelStartNode);
                double startNodeNetworkCost = costEstimate.estimateNetworkCost(labelStartNode);
                costMappingManager.addComputeCost(Pair.of(label, label), nodeSelfComputeCost);
                costMappingManager.addValueNetworkCost(label, startNodeNetworkCost);
                if (labelValueNode != null && !(labelValueNode instanceof SourceTreeNode)) {
                    costMappingManager.addComputeTree(label, labelValueNode);

                    double nodeValueComputeCost = costEstimate.estimateComputeCost(labelStartNode, labelValueNode);
                    double nodeValueNetworkCost = costEstimate.estimateNetworkCost(labelValueNode);

                    String labelValueTag = CostUtils.buildValueName(label);
                    costMappingManager.addValueParent(labelValueTag, label);
                    costMappingManager.addComputeCost(Pair.of(label, labelValueTag), nodeValueComputeCost);
                    costMappingManager.addValueNetworkCost(labelValueTag, nodeValueNetworkCost);
                }
                CostGraph currGraph = buildLabelCostGraph(label,
                        labelStartNode,
                        currNode,
                        costMappingManager,
                        labelValueNode != null && !(labelValueNode instanceof SourceTreeNode));
                costGraph.mergeCostGraph(currGraph);
            } else if (currNode instanceof WherePredicateTreeNode) {
                WherePredicateTreeNode wherePredicateTreeNode = (WherePredicateTreeNode) currNode;
                String startKey = wherePredicateTreeNode.getStartKey();
                if (StringUtils.isNotEmpty(startKey)) {
                    CostGraph startKeyGraph = buildLabelCostGraph(wherePredicateTreeNode.getStartKey(),
                            treeNodeLabelManager.getLastTreeNode(wherePredicateTreeNode.getStartKey()),
                            currNode,
                            costMappingManager,
                            false);
                    costGraph.mergeCostGraph(startKeyGraph);
                }
                String predicateValue = wherePredicateTreeNode.getPredicateValue();
                if (StringUtils.isNotEmpty(predicateValue)) {
                    List<TreeNode> labelNodeList = treeNodeLabelManager.getLabelTreeNodeList(predicateValue);
                    if (null != labelNodeList) {
                        TreeNode predicateNode = labelNodeList.get(labelNodeList.size() - 1);
                        if (null != predicateNode) {
                            CostGraph predicateGraph = buildLabelCostGraph(predicateValue,
                                    predicateNode,
                                    currNode,
                                    costMappingManager,
                                    false);
                            costGraph.mergeCostGraph(predicateGraph);
                        }
                    }
                }
            } else if (currNode instanceof RepeatTreeNode || currNode instanceof UnionTreeNode) {
                costGraph.clear();
                return costGraph;
            }
            currNode = ((UnaryTreeNode) currNode).getInputNode();
        }

        return costGraph;
    }
}

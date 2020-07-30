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

import com.alibaba.maxgraph.compiler.tree.value.VertexValueType;
import org.apache.tinkerpop.gremlin.structure.Direction;

import com.alibaba.maxgraph.compiler.tree.EdgeTreeNode;
import com.alibaba.maxgraph.compiler.tree.FoldTreeNode;
import com.alibaba.maxgraph.compiler.tree.NodeType;
import com.alibaba.maxgraph.compiler.tree.TreeNode;
import com.alibaba.maxgraph.compiler.tree.VertexTreeNode;
import com.alibaba.maxgraph.compiler.tree.addition.PropertyNode;
import com.alibaba.maxgraph.compiler.tree.source.SourceTreeNode;
import com.alibaba.maxgraph.compiler.utils.TreeNodeUtils;

import java.util.List;
import java.util.Set;

public class CostEstimate {
    private static final int PROP_FEW_COUNT = 2;
    private static final int PROP_DEFAULT_COUNT = 10;

    private static final double NET_DEFAULT_COST = 1.0;
    private static final double NET_DEFAULT_SCALE_RATIO = 1.5;
    private static final double NET_FOLD_RATIO = 2.5;

    private static final double CAL_DEFAULT_COST = 1.0;
    private static final double CAL_EDGE_OUT_COST = 2.5;
    private static final double CAL_EDGE_IN_COST = CAL_EDGE_OUT_COST * 1.5;
    private static final double CAL_EDGE_BOTH_COST = CAL_EDGE_OUT_COST + CAL_EDGE_IN_COST;
    private static final double CAL_PROP_FEW_COST = 2.5;
    private static final double CAL_PROP_MANY_COST = CAL_PROP_FEW_COST * 5;

    public double estimateNetworkCost(TreeNode node) {
        if (node instanceof FoldTreeNode) {
            return NET_DEFAULT_COST * NET_DEFAULT_SCALE_RATIO * NET_FOLD_RATIO;
        } else if (node.getNodeType() == NodeType.AGGREGATE) {
            return NET_DEFAULT_COST * NET_DEFAULT_SCALE_RATIO;
        } else if (node instanceof PropertyNode) {
            PropertyNode propertyNode = (PropertyNode) node;
            Set<String> propNameList = propertyNode.getPropKeyList();
            if (propNameList.size() <= 0) {
                return NET_DEFAULT_COST * NET_DEFAULT_SCALE_RATIO * PROP_DEFAULT_COUNT;
            } else {
                return NET_DEFAULT_COST * NET_DEFAULT_SCALE_RATIO * propNameList.size();
            }
        } else if (node.getOutputValueType() instanceof VertexValueType) {
            return NET_DEFAULT_COST;
        } else {
            return NET_DEFAULT_COST * NET_DEFAULT_SCALE_RATIO;
        }
    }

    private double estimateComputeCost(TreeNode node) {
        if (node instanceof SourceTreeNode) {
            return CAL_DEFAULT_COST;
        } else if (node instanceof VertexTreeNode ||
                node instanceof EdgeTreeNode) {
            Direction direction;
            if (node instanceof VertexTreeNode) {
                direction = ((VertexTreeNode) node).getDirection();
            } else {
                direction = ((EdgeTreeNode) node).getDirection();
            }
            switch (direction) {
                case OUT: {
                    return CAL_EDGE_OUT_COST;
                }
                case IN: {
                    return CAL_EDGE_IN_COST;
                }
                case BOTH: {
                    return CAL_EDGE_BOTH_COST;
                }
            }
        } else if (node instanceof PropertyNode) {
            PropertyNode propertyNode = (PropertyNode) node;
            if (propertyNode.getPropKeyList().size() > 0 &&
                    propertyNode.getPropKeyList().size() <= PROP_FEW_COUNT) {
                return CAL_PROP_FEW_COST;
            } else {
                return CAL_PROP_MANY_COST;
            }
        }

        return CAL_DEFAULT_COST;
    }

    public double estimateComputeCost(TreeNode input, TreeNode output) {
        List<TreeNode> nodeList = TreeNodeUtils.buildTreeNodeListFromLeaf(output);
        if (nodeList.size() > 1) {
            double totalCost = 0.0;
            for (int i = 1; i < nodeList.size(); i++) {
                totalCost += estimateComputeCost(nodeList.get(i));
            }
            return totalCost;
        } else {
            return CAL_DEFAULT_COST;
        }
    }
}

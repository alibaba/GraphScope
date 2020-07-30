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
package com.alibaba.maxgraph.compiler.tree;

import com.alibaba.maxgraph.QueryFlowOuterClass;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.logical.edge.EdgeShuffleType;
import com.alibaba.maxgraph.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.compiler.utils.TreeNodeUtils;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.compiler.tree.value.VarietyValueType;
import com.alibaba.maxgraph.compiler.logical.LogicalBinaryVertex;
import com.alibaba.maxgraph.compiler.logical.LogicalEdge;
import com.alibaba.maxgraph.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.compiler.logical.LogicalVertex;
import com.alibaba.maxgraph.compiler.logical.VertexIdManager;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.compiler.tree.source.SourceTreeNode;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;

public class UnionTreeNode extends UnaryTreeNode {
    private List<TreeNode> unionTreeNodeList;

    public UnionTreeNode(TreeNode prev, GraphSchema schema, List<TreeNode> unionTreeNodeList) {
        super(prev, NodeType.FLATMAP, schema);
        this.unionTreeNodeList = unionTreeNodeList;
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        TreeNodeLabelManager labelManager = contextManager.getTreeNodeLabelManager();
        VertexIdManager vertexIdManager = contextManager.getVertexIdManager();
        if (unionTreeNodeList.isEmpty()) {
            throw new IllegalArgumentException("union tree node list is empty");
        } else {
            LogicalSubQueryPlan logicalSubQueryPlan = new LogicalSubQueryPlan(contextManager);
            LogicalVertex delegateSourceVertex = getInputNode().getOutputVertex();
            logicalSubQueryPlan.addLogicalVertex(delegateSourceVertex);

            LogicalVertex unionVertex = null;
            for (TreeNode treeNode : unionTreeNodeList) {
                LogicalSubQueryPlan unionPlan = TreeNodeUtils.buildQueryPlan(treeNode, labelManager, contextManager, vertexIdManager);
                LogicalVertex currentUnionVertex = unionPlan.getOutputVertex();
                logicalSubQueryPlan.mergeLogicalQueryPlan(unionPlan);
                if (null == unionVertex) {
                    unionVertex = currentUnionVertex;
                } else {
                    LogicalBinaryVertex binaryVertex = new LogicalBinaryVertex(
                            vertexIdManager.getId(),
                            new ProcessorFunction(QueryFlowOuterClass.OperatorType.UNION),
                            false,
                            unionVertex,
                            currentUnionVertex);
                    logicalSubQueryPlan.addLogicalVertex(binaryVertex);
                    logicalSubQueryPlan.addLogicalEdge(unionVertex, binaryVertex, new LogicalEdge(EdgeShuffleType.FORWARD));
                    logicalSubQueryPlan.addLogicalEdge(currentUnionVertex, binaryVertex, new LogicalEdge(EdgeShuffleType.FORWARD));
                    unionVertex = binaryVertex;
                }
            }

            LogicalVertex outputVertex = logicalSubQueryPlan.getOutputVertex();
            addUsedLabelAndRequirement(outputVertex, labelManager);
            setFinishVertex(outputVertex, labelManager);

            return logicalSubQueryPlan;
        }
    }

    @Override
    public ValueType getOutputValueType() {
        Set<ValueType> unionValueTypeList = Sets.newHashSet();
        unionTreeNodeList.forEach(v -> unionValueTypeList.add(v.getOutputValueType()));
        if (unionValueTypeList.size() == 1) {
            return unionValueTypeList.iterator().next();
        } else {
            return new VarietyValueType(unionValueTypeList);
        }
    }

    @Override
    public void addPathRequirement() {
        for (TreeNode unionTreeNode : unionTreeNodeList) {
            while (!(unionTreeNode instanceof SourceTreeNode)) {
                if (unionTreeNode.isPathFlag()) {
                    unionTreeNode.addPathRequirement();
                }
                unionTreeNode = UnaryTreeNode.class.cast(unionTreeNode).getInputNode();
            }
        }
    }

    @Override
    public void enableDedupLocal() {
        super.enableDedupLocal();
        for (TreeNode unionNode : this.unionTreeNodeList) {
            List<TreeNode> treeNodeList = TreeNodeUtils.buildTreeNodeListFromLeaf(unionNode);
            for (TreeNode treeNode : treeNodeList) {
                treeNode.enableDedupLocal();
            }
        }
    }

    /**
     * Enable global stop by global limit operator
     */
    @Override
    public void enableGlobalStop() {
        throw new IllegalArgumentException("repeat tree node can't enable global stop");
    }

    /**
     * Enable global filter by global stop
     */
    @Override
    public void enableGlobalFilter() {
        super.enableGlobalFilter();
        for (TreeNode currNode : this.unionTreeNodeList) {
            while (currNode instanceof UnaryTreeNode) {
                currNode.enableGlobalFilter();
                currNode = ((UnaryTreeNode) currNode).getInputNode();
            }
        }
    }
}

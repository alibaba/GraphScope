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

import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.logical.LogicalEdge;
import com.alibaba.maxgraph.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.compiler.utils.TreeNodeUtils;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.compiler.logical.LogicalVertex;
import com.alibaba.maxgraph.compiler.logical.VertexIdManager;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.compiler.tree.source.SourceTreeNode;
import com.alibaba.maxgraph.compiler.utils.CompilerUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;

import java.util.List;
import java.util.Map;

public class SelectOneTreeNode extends UnaryTreeNode {
    private String selectLabel;
    private Pop pop;
    private TreeNode traversalTreeNode;
    private List<TreeNode> selectTreeNodeList;
    private ValueType constantValueType;

    public SelectOneTreeNode(TreeNode input, String selectLabel, Pop pop, List<TreeNode> selectTreeNodeList, GraphSchema schema) {
        super(input, NodeType.MAP, schema);
        this.selectLabel = selectLabel;
        this.pop = pop;
        this.traversalTreeNode = null;
        this.selectTreeNodeList = selectTreeNodeList;
        this.constantValueType = null;
        getUsedLabelList().add(selectLabel);
    }

    public void setConstantValueType(ValueType constantValueType) {
        this.constantValueType = constantValueType;
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        TreeNodeLabelManager labelManager = contextManager.getTreeNodeLabelManager();
        VertexIdManager vertexIdManager = contextManager.getVertexIdManager();
        Map<String, Integer> labelIndexList = labelManager.getLabelIndexList();

        ProcessorFunction selectOneFunction = TreeNodeUtils.createSelectOneFunction(selectLabel, pop, labelIndexList);
        LogicalSubQueryPlan logicalSubQueryPlan = parseSingleUnaryVertex(vertexIdManager,
                labelManager,
                selectOneFunction,
                contextManager,
                LogicalEdge.shuffleByKey(0),
                null == traversalTreeNode);

        if (null != traversalTreeNode &&
                !contextManager.getCostModelManager().processFieldValue(selectLabel)) {
            LogicalSubQueryPlan traversalValuePlan = TreeNodeUtils.buildSubQueryPlan(
                    traversalTreeNode,
                    logicalSubQueryPlan.getOutputVertex(),
                    contextManager);
            logicalSubQueryPlan.mergeLogicalQueryPlan(traversalValuePlan);

            LogicalVertex outputVertex = logicalSubQueryPlan.getOutputVertex();
            addUsedLabelAndRequirement(outputVertex, labelManager);
            setFinishVertex(outputVertex, labelManager);
        }
        return logicalSubQueryPlan;
    }

    @Override
    public ValueType getOutputValueType() {
        return constantValueType != null ? constantValueType :
                (null != traversalTreeNode && !(traversalTreeNode instanceof SourceTreeNode) ?
                        traversalTreeNode.getOutputValueType() :
                        CompilerUtils.parseValueTypeWithPop(selectTreeNodeList, pop));
    }

    public void setTraversalTreeNode(TreeNode traversalTreeNode) {
        this.traversalTreeNode = traversalTreeNode;
    }

    public TreeNode getTraversalTreeNode() {
        return traversalTreeNode;
    }

    public String getSelectLabel() {
        return selectLabel;
    }

    public TreeNode getLabelStartTreeNode() {
        if (null != selectTreeNodeList && this.selectTreeNodeList.size() == 1) {
            return this.selectTreeNodeList.get(0);
        }

        return null;
    }
}

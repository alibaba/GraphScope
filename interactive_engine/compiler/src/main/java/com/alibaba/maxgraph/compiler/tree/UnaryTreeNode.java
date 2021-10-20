/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.compiler.tree;

import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.logical.LogicalEdge;
import com.alibaba.maxgraph.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.compiler.logical.LogicalUnaryVertex;
import com.alibaba.maxgraph.compiler.logical.VertexIdManager;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.compiler.logical.LogicalVertex;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class UnaryTreeNode extends BaseTreeNode {
    private TreeNode input;

    public UnaryTreeNode(TreeNode input, NodeType nodeType, GraphSchema schema) {
        super(nodeType, schema);
        this.input = input;
        if (null != this.input) {
            this.input.setOutputNode(this);
        }
    }

    public TreeNode getInputNode() {
        return input;
    }

    public void setInputNode(TreeNode treeNode) {
        this.input = checkNotNull(treeNode);
        this.input.setOutputNode(this);
    }

    protected LogicalSubQueryPlan parseSingleUnaryVertex(
            VertexIdManager vertexIdManager,
            TreeNodeLabelManager labelManager,
            ProcessorFunction processorFunction,
            ContextManager contextManager) {
        return parseSingleUnaryVertex(
                vertexIdManager,
                labelManager,
                processorFunction,
                contextManager,
                new LogicalEdge());
    }

    protected LogicalSubQueryPlan parseSingleUnaryVertex(
            VertexIdManager vertexIdManager,
            TreeNodeLabelManager labelManager,
            ProcessorFunction processorFunction,
            ContextManager contextManager,
            LogicalEdge logicalEdge) {
        return parseSingleUnaryVertex(
                vertexIdManager,
                labelManager,
                processorFunction,
                contextManager,
                logicalEdge,
                true);
    }

    protected LogicalSubQueryPlan parseSingleUnaryVertex(
            VertexIdManager vertexIdManager,
            TreeNodeLabelManager labelManager,
            ProcessorFunction processorFunction,
            ContextManager contextManager,
            LogicalEdge logicalEdge,
            boolean outputFlag) {
        LogicalSubQueryPlan logicalSubQueryPlan = new LogicalSubQueryPlan(contextManager);
        LogicalVertex sourceVertex = getInputNode().getOutputVertex();
        logicalSubQueryPlan.addLogicalVertex(sourceVertex);

        LogicalUnaryVertex logicalUnaryVertex =
                new LogicalUnaryVertex(
                        vertexIdManager.getId(), processorFunction, false, sourceVertex);
        logicalUnaryVertex.setEarlyStopFlag(super.earlyStopArgument);
        logicalSubQueryPlan.addLogicalVertex(logicalUnaryVertex);
        logicalSubQueryPlan.addLogicalEdge(sourceVertex, logicalUnaryVertex, logicalEdge);

        if (outputFlag) {
            setFinishVertex(logicalUnaryVertex, labelManager);
            addUsedLabelAndRequirement(logicalUnaryVertex, labelManager);
        }
        return logicalSubQueryPlan;
    }

    protected void addUsedLabelAndRequirement(
            LogicalVertex logicalVertex, TreeNodeLabelManager treeNodeLabelManager) {
        getUsedLabelList()
                .forEach(
                        v ->
                                logicalVertex
                                        .getProcessorFunction()
                                        .getUsedLabelList()
                                        .add(treeNodeLabelManager.getLabelIndex(v)));
        logicalVertex
                .getBeforeRequirementList()
                .addAll(buildBeforeRequirementList(treeNodeLabelManager));
        logicalVertex
                .getAfterRequirementList()
                .addAll(buildAfterRequirementList(treeNodeLabelManager));
    }
}

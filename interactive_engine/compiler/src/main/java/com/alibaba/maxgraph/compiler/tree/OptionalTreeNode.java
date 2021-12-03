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

import com.alibaba.maxgraph.QueryFlowOuterClass;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.logical.LogicalQueryPlan;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.compiler.tree.value.VarietyValueType;
import com.alibaba.maxgraph.compiler.logical.LogicalBinaryVertex;
import com.alibaba.maxgraph.compiler.logical.LogicalEdge;
import com.alibaba.maxgraph.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.compiler.logical.LogicalVertex;
import com.alibaba.maxgraph.compiler.logical.edge.EdgeShuffleType;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.compiler.utils.TreeNodeUtils;
import com.google.common.collect.Sets;

import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class OptionalTreeNode extends UnaryTreeNode {
    private TreeNode branchTreeNode;
    private TreeNode trueNode;
    private TreeNode falseNode;

    public OptionalTreeNode(
            TreeNode input,
            GraphSchema schema,
            TreeNode branchTreeNode,
            TreeNode trueNode,
            TreeNode falseNode) {
        super(input, NodeType.FLATMAP, schema);
        this.branchTreeNode = checkNotNull(branchTreeNode);
        this.trueNode = checkNotNull(trueNode);
        this.falseNode = checkNotNull(falseNode);
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        LogicalSubQueryPlan logicalQueryPlan = new LogicalSubQueryPlan(contextManager);
        LogicalVertex sourceVertex = getInputNode().getOutputVertex();
        logicalQueryPlan.addLogicalVertex(sourceVertex);
        LogicalVertex trueVertex =
                TreeNodeUtils.buildFilterTreeNode(
                        this.branchTreeNode,
                        contextManager,
                        logicalQueryPlan,
                        sourceVertex,
                        schema);

        ProcessorFunction joinFilterRightFunction =
                new ProcessorFunction(QueryFlowOuterClass.OperatorType.JOIN_DIRECT_FILTER_NEGATE);
        LogicalEdge leftEdge =
                getInputNode().isPropLocalFlag()
                        ? new LogicalEdge(EdgeShuffleType.FORWARD)
                        : new LogicalEdge(EdgeShuffleType.SHUFFLE_BY_KEY);
        LogicalBinaryVertex falseVertex =
                new LogicalBinaryVertex(
                        contextManager.getVertexIdManager().getId(),
                        joinFilterRightFunction,
                        getInputNode().isPropLocalFlag(),
                        sourceVertex,
                        trueVertex);
        logicalQueryPlan.addLogicalVertex(falseVertex);
        logicalQueryPlan.addLogicalEdge(sourceVertex, falseVertex, leftEdge);
        logicalQueryPlan.addLogicalEdge(
                trueVertex, falseVertex, new LogicalEdge(EdgeShuffleType.SHUFFLE_BY_KEY));

        LogicalQueryPlan truePlan =
                TreeNodeUtils.buildSubQueryPlan(this.trueNode, trueVertex, contextManager);
        LogicalVertex trueOutputVertex = truePlan.getOutputVertex();

        LogicalQueryPlan falsePlan =
                TreeNodeUtils.buildSubQueryPlan(this.falseNode, falseVertex, contextManager);
        LogicalVertex falseOutputVertex = falsePlan.getOutputVertex();

        logicalQueryPlan.mergeLogicalQueryPlan(truePlan);
        logicalQueryPlan.mergeLogicalQueryPlan(falsePlan);

        LogicalBinaryVertex unionVertex =
                new LogicalBinaryVertex(
                        contextManager.getVertexIdManager().getId(),
                        new ProcessorFunction(QueryFlowOuterClass.OperatorType.UNION),
                        false,
                        trueOutputVertex,
                        falseOutputVertex);
        logicalQueryPlan.addLogicalVertex(unionVertex);
        logicalQueryPlan.addLogicalEdge(
                trueOutputVertex, unionVertex, new LogicalEdge(EdgeShuffleType.FORWARD));
        logicalQueryPlan.addLogicalEdge(
                falseOutputVertex, unionVertex, new LogicalEdge(EdgeShuffleType.FORWARD));

        setFinishVertex(unionVertex, contextManager.getTreeNodeLabelManager());
        addUsedLabelAndRequirement(unionVertex, contextManager.getTreeNodeLabelManager());

        return logicalQueryPlan;
    }

    @Override
    public ValueType getOutputValueType() {
        Set<ValueType> outputValueTypeList =
                Sets.newHashSet(
                        branchTreeNode.getOutputValueType(), getInputNode().getOutputValueType());
        if (outputValueTypeList.size() == 1) {
            return outputValueTypeList.iterator().next();
        } else {
            return new VarietyValueType(outputValueTypeList);
        }
    }
}

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

import com.alibaba.maxgraph.Message;
import com.alibaba.maxgraph.QueryFlowOuterClass;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.logical.LogicalBinaryVertex;
import com.alibaba.maxgraph.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.compiler.tree.addition.JoinZeroNode;
import com.alibaba.maxgraph.compiler.tree.source.SourceDelegateNode;
import com.alibaba.maxgraph.compiler.utils.CompilerUtils;
import com.alibaba.maxgraph.compiler.utils.TreeNodeUtils;
import com.alibaba.maxgraph.compiler.tree.value.MapValueType;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.compiler.tree.value.ValueValueType;
import com.alibaba.maxgraph.compiler.logical.LogicalEdge;
import com.alibaba.maxgraph.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.compiler.logical.LogicalUnaryVertex;
import com.alibaba.maxgraph.compiler.logical.LogicalVertex;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFunction;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;

public class GroupCountTreeNode extends UnaryTreeNode {
    private TreeNode keyTreeNode = null;

    public GroupCountTreeNode(TreeNode prev, GraphSchema schema) {
        super(prev, NodeType.AGGREGATE, schema);
    }

    public void setKeyTreeNode(TreeNode keyTreeNode) {
        this.keyTreeNode = keyTreeNode;
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        LogicalSubQueryPlan logicalSubQueryPlan = new LogicalSubQueryPlan(contextManager);
        LogicalVertex sourceVertex = getInputNode().getOutputVertex();
        logicalSubQueryPlan.addLogicalVertex(sourceVertex);
        LogicalVertex outputVertex;

        if (null == keyTreeNode || keyTreeNode instanceof SourceDelegateNode) {
            ProcessorFunction processorFunction =
                    new ProcessorFunction(QueryFlowOuterClass.OperatorType.GROUP_COUNT);
            outputVertex =
                    new LogicalUnaryVertex(
                            contextManager.getVertexIdManager().getId(),
                            processorFunction,
                            false,
                            sourceVertex);
            logicalSubQueryPlan.addLogicalVertex(outputVertex);
            logicalSubQueryPlan.addLogicalEdge(sourceVertex, outputVertex, new LogicalEdge());
        } else {
            TreeNode currentKeyNode = TreeNodeUtils.buildSingleOutputNode(keyTreeNode, schema);
            if (currentKeyNode instanceof JoinZeroNode) {
                ((JoinZeroNode) currentKeyNode).disableJoinZero();
            }
            LogicalSubQueryPlan keyValuePlan =
                    TreeNodeUtils.buildSubQueryPlan(currentKeyNode, sourceVertex, contextManager);
            LogicalVertex groupValueVertex = keyValuePlan.getOutputVertex();
            LogicalVertex enterKeyVertex =
                    TreeNodeUtils.getSourceTreeNode(currentKeyNode).getOutputVertex();
            logicalSubQueryPlan.mergeLogicalQueryPlan(keyValuePlan);
            if (TreeNodeUtils.checkJoinSourceFlag(currentKeyNode)) {
                String valueLabel =
                        contextManager.getTreeNodeLabelManager().createSysLabelStart("val");
                getUsedLabelList().add(valueLabel);
                int valueLabelId =
                        contextManager.getTreeNodeLabelManager().getLabelIndex(valueLabel);
                LogicalBinaryVertex logicalBinaryVertex =
                        new LogicalBinaryVertex(
                                contextManager.getVertexIdManager().getId(),
                                new ProcessorFunction(
                                        CompilerUtils.parseJoinOperatorType(currentKeyNode),
                                        Message.Value.newBuilder().setIntValue(valueLabelId)),
                                false,
                                enterKeyVertex,
                                groupValueVertex);
                logicalSubQueryPlan.addLogicalVertex(logicalBinaryVertex);
                logicalSubQueryPlan.addLogicalEdge(
                        enterKeyVertex, logicalBinaryVertex, new LogicalEdge());
                logicalSubQueryPlan.addLogicalEdge(
                        groupValueVertex, logicalBinaryVertex, new LogicalEdge());

                ProcessorFunction selectValueFunction =
                        TreeNodeUtils.createSelectOneFunction(
                                valueLabel,
                                Pop.first,
                                contextManager.getTreeNodeLabelManager().getLabelIndexList());
                LogicalVertex selectLabelVertex =
                        new LogicalUnaryVertex(
                                contextManager.getVertexIdManager().getId(),
                                selectValueFunction,
                                false,
                                logicalBinaryVertex);
                logicalSubQueryPlan.addLogicalVertex(selectLabelVertex);
                logicalSubQueryPlan.addLogicalEdge(
                        logicalBinaryVertex, selectLabelVertex, LogicalEdge.forwardEdge());
            }

            groupValueVertex = logicalSubQueryPlan.getOutputVertex();
            ProcessorFunction processorFunction =
                    new ProcessorFunction(
                            QueryFlowOuterClass.OperatorType.GROUP_COUNT,
                            Message.Value.newBuilder());
            outputVertex =
                    new LogicalUnaryVertex(
                            contextManager.getVertexIdManager().getId(),
                            processorFunction,
                            false,
                            groupValueVertex);
            logicalSubQueryPlan.addLogicalVertex(outputVertex);
            logicalSubQueryPlan.addLogicalEdge(groupValueVertex, outputVertex, new LogicalEdge());
        }

        ProcessorFunction foldMapFunction =
                new ProcessorFunction(QueryFlowOuterClass.OperatorType.FOLDMAP);
        LogicalVertex foldMapVertex =
                new LogicalUnaryVertex(
                        contextManager.getVertexIdManager().getId(),
                        foldMapFunction,
                        false,
                        outputVertex);
        logicalSubQueryPlan.addLogicalVertex(foldMapVertex);
        logicalSubQueryPlan.addLogicalEdge(outputVertex, foldMapVertex, new LogicalEdge());

        addUsedLabelAndRequirement(foldMapVertex, contextManager.getTreeNodeLabelManager());
        setFinishVertex(foldMapVertex, contextManager.getTreeNodeLabelManager());

        return logicalSubQueryPlan;
    }

    @Override
    public boolean isPropLocalFlag() {
        return false;
    }

    @Override
    public ValueType getOutputValueType() {
        return new MapValueType(
                null == keyTreeNode
                        ? getInputNode().getOutputValueType()
                        : keyTreeNode.getOutputValueType(),
                new ValueValueType(Message.VariantType.VT_LONG));
    }
}

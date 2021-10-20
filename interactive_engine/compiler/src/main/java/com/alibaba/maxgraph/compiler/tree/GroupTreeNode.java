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
import com.alibaba.maxgraph.compiler.tree.value.VertexValueType;
import com.alibaba.maxgraph.compiler.logical.edge.EdgeShuffleType;
import com.alibaba.maxgraph.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.compiler.utils.TreeNodeUtils;
import com.alibaba.maxgraph.compiler.tree.value.MapValueType;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.compiler.logical.LogicalBinaryVertex;
import com.alibaba.maxgraph.compiler.logical.LogicalEdge;
import com.alibaba.maxgraph.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.compiler.logical.LogicalUnaryVertex;
import com.alibaba.maxgraph.compiler.logical.LogicalVertex;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.compiler.tree.source.SourceDelegateNode;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;

import java.util.Optional;
import java.util.Set;

public class GroupTreeNode extends UnaryTreeNode {
    private TreeNode keyTreeNode;
    private TreeNode valueTreeNode;

    public GroupTreeNode(TreeNode prev, GraphSchema schema) {
        super(prev, NodeType.AGGREGATE, schema);
    }

    public void setKeyTreeNode(TreeNode keyTreeNode) {
        this.keyTreeNode = keyTreeNode;
        if (keyTreeNode instanceof UnaryTreeNode) {
            TreeNode parentKeyNode = ((UnaryTreeNode) keyTreeNode).getInputNode();
            if (parentKeyNode instanceof SourceDelegateNode) {
                if (keyTreeNode instanceof SelectOneTreeNode) {
                    this.getUsedLabelList().add(((SelectOneTreeNode) keyTreeNode).getSelectLabel());
                } else if (keyTreeNode instanceof SelectTreeNode) {
                    this.getUsedLabelList()
                            .addAll(((SelectTreeNode) keyTreeNode).getSelectKeyList());
                }
            }
        }
    }

    public void setValueTreeNode(TreeNode valueTreeNode) {
        this.valueTreeNode = valueTreeNode;
        for (TreeNode treeNode : TreeNodeUtils.buildTreeNodeListFromLeaf(this.valueTreeNode)) {
            if (treeNode instanceof OrderGlobalTreeNode) {
                ((OrderGlobalTreeNode) treeNode).enableOrderKeyFlag();
            }
        }
    }

    public void setPropFillValueNode(Set<String> reqPropList) {
        TreeNode currentValueNode = valueTreeNode;
        while (currentValueNode instanceof UnaryTreeNode) {
            UnaryTreeNode currentUnaryNode = UnaryTreeNode.class.cast(currentValueNode);
            TreeNode inputTreeNode = currentUnaryNode.getInputNode();
            if (inputTreeNode instanceof PropFillTreeNode) {
                PropFillTreeNode propFillTreeNode = PropFillTreeNode.class.cast(inputTreeNode);
                propFillTreeNode.getPropKeyList().addAll(reqPropList);
                break;
            } else if (inputTreeNode.getOutputValueType() instanceof VertexValueType) {
                PropFillTreeNode propFillTreeNode =
                        new PropFillTreeNode(inputTreeNode, reqPropList, schema);
                currentUnaryNode.setInputNode(propFillTreeNode);
                break;
            }
            currentValueNode = inputTreeNode;
        }
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        LogicalSubQueryPlan logicalSubQueryPlan = new LogicalSubQueryPlan(contextManager);
        LogicalVertex sourceVertex = getInputNode().getOutputVertex();
        logicalSubQueryPlan.addLogicalVertex(sourceVertex);

        QueryFlowOuterClass.EnterKeyArgumentProto enterKeyArgumentProto = null;
        if (keyTreeNode == null || keyTreeNode instanceof SourceDelegateNode) {
            enterKeyArgumentProto =
                    QueryFlowOuterClass.EnterKeyArgumentProto.newBuilder()
                            .setEnterKeyType(QueryFlowOuterClass.EnterKeyTypeProto.KEY_SELF)
                            .build();
        } else if (UnaryTreeNode.class.cast(keyTreeNode).getInputNode()
                instanceof SourceDelegateNode) {
            Optional<QueryFlowOuterClass.EnterKeyArgumentProto> propLabelId =
                    TreeNodeUtils.parseEnterKeyArgument(
                            keyTreeNode, schema, contextManager.getTreeNodeLabelManager());
            if (propLabelId.isPresent()) {
                enterKeyArgumentProto = propLabelId.get();
            }
        }
        if (null != enterKeyArgumentProto) {
            // use enter key operator to generate key data
            ProcessorFunction enterKeyFunction =
                    new ProcessorFunction(
                            QueryFlowOuterClass.OperatorType.ENTER_KEY,
                            Message.Value.newBuilder()
                                    .setPayload(enterKeyArgumentProto.toByteString()));
            LogicalVertex enterKeyVertex =
                    new LogicalUnaryVertex(
                            contextManager.getVertexIdManager().getId(),
                            enterKeyFunction,
                            false,
                            sourceVertex);
            enterKeyArgumentProto
                    .getPropIdListList()
                    .forEach(
                            v -> {
                                if (v < 0) {
                                    enterKeyFunction.getUsedLabelList().add(v);
                                }
                            });
            if (enterKeyArgumentProto.getPropLabelId() < 0) {
                enterKeyFunction.getUsedLabelList().add(enterKeyArgumentProto.getPropLabelId());
            }
            logicalSubQueryPlan.addLogicalVertex(enterKeyVertex);
            logicalSubQueryPlan.addLogicalEdge(sourceVertex, enterKeyVertex, new LogicalEdge());
        } else {
            TreeNode currentKeyNode = TreeNodeUtils.buildSingleOutputNode(keyTreeNode, schema);
            LogicalSubQueryPlan keyQueryPlan =
                    TreeNodeUtils.buildSubQueryPlan(
                            currentKeyNode, sourceVertex, contextManager, true);
            LogicalVertex keyValueVertex = keyQueryPlan.getOutputVertex();
            logicalSubQueryPlan.mergeLogicalQueryPlan(keyQueryPlan);

            sourceVertex = TreeNodeUtils.getSourceTreeNode(keyTreeNode).getOutputVertex();
            if (sourceVertex.getProcessorFunction().getOperatorType()
                    == QueryFlowOuterClass.OperatorType.ENTER_KEY) {
                ProcessorFunction joinKeyFunction =
                        new ProcessorFunction(
                                QueryFlowOuterClass.OperatorType.JOIN_RIGHT_VALUE_KEY);
                LogicalBinaryVertex joinKeyVertex =
                        new LogicalBinaryVertex(
                                contextManager.getVertexIdManager().getId(),
                                joinKeyFunction,
                                false,
                                sourceVertex,
                                keyValueVertex);
                logicalSubQueryPlan.addLogicalVertex(joinKeyVertex);
                logicalSubQueryPlan.addLogicalEdge(sourceVertex, joinKeyVertex, new LogicalEdge());
                logicalSubQueryPlan.addLogicalEdge(
                        keyValueVertex, joinKeyVertex, new LogicalEdge());
            } else {
                String keyValueLabel =
                        contextManager
                                .getTreeNodeLabelManager()
                                .createSysLabelStart(keyValueVertex, "val");
                String sourceLabel =
                        contextManager
                                .getTreeNodeLabelManager()
                                .createSysLabelStart(sourceVertex, "source");
                ProcessorFunction selectSourceFunction =
                        TreeNodeUtils.createSelectOneFunction(
                                sourceLabel,
                                Pop.first,
                                contextManager.getTreeNodeLabelManager().getLabelIndexList());
                LogicalVertex selectVertex =
                        new LogicalUnaryVertex(
                                contextManager.getVertexIdManager().getId(),
                                selectSourceFunction,
                                false,
                                keyValueVertex);
                logicalSubQueryPlan.addLogicalVertex(selectVertex);
                logicalSubQueryPlan.addLogicalEdge(
                        keyValueVertex, selectVertex, new LogicalEdge(EdgeShuffleType.FORWARD));

                int keyValueLabelId =
                        contextManager.getTreeNodeLabelManager().getLabelIndex(keyValueLabel);
                ProcessorFunction enterKeyFunction =
                        new ProcessorFunction(
                                QueryFlowOuterClass.OperatorType.ENTER_KEY,
                                Message.Value.newBuilder()
                                        .setPayload(
                                                QueryFlowOuterClass.EnterKeyArgumentProto
                                                        .newBuilder()
                                                        .setEnterKeyType(
                                                                QueryFlowOuterClass
                                                                        .EnterKeyTypeProto
                                                                        .KEY_PROP_LABEL)
                                                        .setUniqFlag(false)
                                                        .setPropLabelId(keyValueLabelId)
                                                        .build()
                                                        .toByteString()));
                enterKeyFunction.getUsedLabelList().add(keyValueLabelId);
                LogicalVertex enterKeyVertex =
                        new LogicalUnaryVertex(
                                contextManager.getVertexIdManager().getId(),
                                enterKeyFunction,
                                false,
                                selectVertex);
                logicalSubQueryPlan.addLogicalVertex(enterKeyVertex);
                logicalSubQueryPlan.addLogicalEdge(
                        selectVertex, enterKeyVertex, new LogicalEdge(EdgeShuffleType.FORWARD));
            }
        }

        LogicalVertex outputVertex = logicalSubQueryPlan.getOutputVertex();
        LogicalSubQueryPlan aggregateQueryPlan =
                TreeNodeUtils.buildSubQueryPlan(valueTreeNode, outputVertex, contextManager, false);
        LogicalVertex aggregateVertex = aggregateQueryPlan.getOutputVertex();
        logicalSubQueryPlan.mergeLogicalQueryPlan(aggregateQueryPlan);
        // convert by key value to entry
        ProcessorFunction processorFunction =
                new ProcessorFunction(QueryFlowOuterClass.OperatorType.BYKEY_ENTRY);
        LogicalVertex bykeyEntryVertex =
                new LogicalUnaryVertex(
                        contextManager.getVertexIdManager().getId(),
                        processorFunction,
                        false,
                        aggregateVertex);
        logicalSubQueryPlan.addLogicalVertex(bykeyEntryVertex);
        logicalSubQueryPlan.addLogicalEdge(aggregateVertex, bykeyEntryVertex, new LogicalEdge());

        outputVertex = logicalSubQueryPlan.getOutputVertex();
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
        ValueType keyValueType =
                null == keyTreeNode
                        ? getInputNode().getOutputValueType()
                        : keyTreeNode.getOutputValueType();
        ValueType valueValueType = valueTreeNode.getOutputValueType();
        return new MapValueType(keyValueType, valueValueType);
    }
}

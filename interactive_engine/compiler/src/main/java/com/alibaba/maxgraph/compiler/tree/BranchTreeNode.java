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

import com.alibaba.maxgraph.Message;
import com.alibaba.maxgraph.QueryFlowOuterClass;
import com.alibaba.maxgraph.common.util.SchemaUtils;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFilterFunction;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.compiler.logical.LogicalBinaryVertex;
import com.alibaba.maxgraph.compiler.logical.LogicalEdge;
import com.alibaba.maxgraph.compiler.logical.LogicalQueryPlan;
import com.alibaba.maxgraph.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.compiler.logical.LogicalUnaryVertex;
import com.alibaba.maxgraph.compiler.logical.LogicalVertex;
import com.alibaba.maxgraph.compiler.logical.edge.EdgeShuffleType;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.compiler.tree.addition.JoinZeroNode;
import com.alibaba.maxgraph.compiler.tree.addition.PropertyNode;
import com.alibaba.maxgraph.compiler.tree.source.SourceTreeNode;
import com.alibaba.maxgraph.compiler.utils.CompilerUtils;
import com.alibaba.maxgraph.compiler.utils.ReflectionUtils;
import com.alibaba.maxgraph.compiler.utils.TreeNodeUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.BranchStep;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class BranchTreeNode extends UnaryTreeNode {
    private TreeNode branchTreeNode;
    private TreeNode noneTreeNode;
    private TreeNode anyTreeNode;
    private Map<Object, List<TreeNode>> optionPickList;

    public BranchTreeNode(TreeNode prev, GraphSchema schema, TreeNode branchTreeNode) {
        super(prev, NodeType.FLATMAP, schema);
        this.branchTreeNode = branchTreeNode;
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        LogicalSubQueryPlan logicalSubQueryPlan = new LogicalSubQueryPlan(contextManager);
        LogicalVertex sourceVertex = getInputNode().getOutputVertex();
        logicalSubQueryPlan.addLogicalVertex(sourceVertex);

        int optionLabelId = 0;
        boolean optionJoinFlag = true;
        UnaryTreeNode branchUnaryNode = UnaryTreeNode.class.cast(branchTreeNode);
        if (branchUnaryNode.getInputNode() instanceof SourceTreeNode) {
            if (branchUnaryNode instanceof PropertyNode) {
                String prop = PropertyNode.class.cast(branchUnaryNode).getPropKeyList().iterator().next();
                optionLabelId = SchemaUtils.getPropId(prop, schema);
                optionJoinFlag = false;
            } else if (branchUnaryNode instanceof SelectOneTreeNode) {
                optionLabelId = contextManager.getTreeNodeLabelManager().getLabelIndex(SelectOneTreeNode.class.cast(branchUnaryNode).getSelectLabel());
                optionJoinFlag = false;
            } else if (branchUnaryNode instanceof TokenTreeNode) {
                optionLabelId = contextManager.getTreeNodeLabelManager().getLabelIndex(TokenTreeNode.class.cast(branchUnaryNode).getToken().getAccessor());
                optionJoinFlag = false;
            }
        }
        if (optionJoinFlag) {
            // join the value stream to get value and set it to label
            if (branchUnaryNode instanceof JoinZeroNode) {
                ((JoinZeroNode) branchUnaryNode).disableJoinZero();
            }
            TreeNode currentBranchTreeNode = TreeNodeUtils.buildSingleOutputNode(branchTreeNode, schema);
            LogicalQueryPlan branchValuePlan = TreeNodeUtils.buildSubQueryPlan(
                    currentBranchTreeNode,
                    sourceVertex,
                    contextManager);
            TreeNode branchSourceNode = TreeNodeUtils.getSourceTreeNode(currentBranchTreeNode);
            sourceVertex = branchSourceNode.getOutputVertex();

            LogicalVertex branchValueVertex = branchValuePlan.getOutputVertex();
            logicalSubQueryPlan.mergeLogicalQueryPlan(branchValuePlan);
            String valueLabel = contextManager.getTreeNodeLabelManager().createSysLabelStart("val");
            optionLabelId = contextManager.getTreeNodeLabelManager().getLabelIndex(valueLabel);
            QueryFlowOuterClass.OperatorType joinOperatorType = CompilerUtils.parseJoinOperatorType(branchTreeNode);
            ProcessorFunction joinFunction = new ProcessorFunction(joinOperatorType, Message.Value.newBuilder().setIntValue(optionLabelId));
            LogicalBinaryVertex joinVertex = new LogicalBinaryVertex(
                    contextManager.getVertexIdManager().getId(),
                    joinFunction,
                    false,
                    sourceVertex,
                    branchValueVertex);
            logicalSubQueryPlan.addLogicalVertex(joinVertex);
            logicalSubQueryPlan.addLogicalEdge(sourceVertex, joinVertex, new LogicalEdge());
            logicalSubQueryPlan.addLogicalEdge(branchValueVertex, joinVertex, new LogicalEdge());
        }

        LogicalVertex outputVertex = logicalSubQueryPlan.getOutputVertex();
        if (optionLabelId > 0) {
            ProcessorFunction fillFunction = new ProcessorFunction(QueryFlowOuterClass.OperatorType.PROP_FILL, Message.Value.newBuilder().addIntValueList(optionLabelId));
            LogicalVertex fillPropVertex = new LogicalUnaryVertex(contextManager.getVertexIdManager().getId(), fillFunction, true, outputVertex);
            logicalSubQueryPlan.addLogicalVertex(fillPropVertex);
            logicalSubQueryPlan.addLogicalEdge(outputVertex, fillPropVertex, outputVertex.isPropLocalFlag() ? new LogicalEdge(EdgeShuffleType.FORWARD) : new LogicalEdge());
            outputVertex = fillPropVertex;
        }

        Map<Object, Message.LogicalCompare> pickCompareList = Maps.newHashMap();
        List<Message.LogicalCompare> noneCompareList = Lists.newArrayList();
        for (Map.Entry<Object, List<TreeNode>> pickEntry : optionPickList.entrySet()) {
            Object pick = pickEntry.getKey();
            Object pickValue = pick;
            if (optionLabelId == TreeConstants.LABEL_INDEX) {
                final String pickString = pick.toString();
                pickValue = schema.getElement(pickString).getLabelId();
            } else if (optionLabelId == TreeConstants.ID_INDEX) {
                pickValue = Long.parseLong(pick.toString());
            }

            Message.VariantType variantType = CompilerUtils.parseVariantType(pickValue.getClass(), pickValue);
            Message.Value.Builder valueBuilder = Message.Value.newBuilder().setValueType(variantType);
            switch (variantType) {
                case VT_INT: {
                    if (pickValue instanceof Integer) {
                        valueBuilder.setIntValue((Integer) pickValue);
                    } else {
                        Class<?> pickTokenKeyClazz = BranchStep.class.getDeclaredClasses()[0];
                        Number number = ReflectionUtils.getFieldValue(pickTokenKeyClazz, pickValue, "number");
                        valueBuilder.setIntValue((int) number);
                    }
                    break;
                }
                case VT_LONG: {
                    if (pickValue instanceof Long) {
                        valueBuilder.setLongValue((Long) pickValue);
                    } else {
                        Class<?> pickTokenKeyClazz = BranchStep.class.getDeclaredClasses()[0];
                        Number number = ReflectionUtils.getFieldValue(pickTokenKeyClazz, pickValue, "number");
                        valueBuilder.setLongValue((long) number);
                    }
                    break;
                }
                case VT_STRING: {
                    valueBuilder.setStrValue((String) pickValue).build();
                    break;
                }
                default: {
                    throw new UnsupportedOperationException(pickValue + " for branch option operator");
                }
            }
            Message.Value value = valueBuilder.build();
            pickCompareList.put(pick,
                    Message.LogicalCompare.newBuilder()
                            .setPropId(optionLabelId)
                            .setCompare(Message.CompareType.EQ)
                            .setValue(value)
                            .setType(variantType)
                            .build());
            noneCompareList.add(Message.LogicalCompare.newBuilder()
                    .setPropId(optionLabelId)
                    .setCompare(Message.CompareType.NEQ)
                    .setValue(value)
                    .setType(variantType)
                    .build());
        }

        List<LogicalVertex> unionVertexList = Lists.newArrayList();
        for (Map.Entry<Object, List<TreeNode>> optionEntry : this.optionPickList.entrySet()) {
            List<TreeNode> optionTreeNodeList = optionEntry.getValue();
            Message.LogicalCompare logicalCompare = checkNotNull(pickCompareList.get(optionEntry.getKey()));
            ProcessorFilterFunction filterFunction = new ProcessorFilterFunction(QueryFlowOuterClass.OperatorType.FILTER,
                    createArgumentBuilder().setBoolValue(true));
            filterFunction.getLogicalCompareList().add(logicalCompare);
            LogicalVertex filterVertex = new LogicalUnaryVertex(contextManager.getVertexIdManager().getId(), filterFunction, outputVertex);
            logicalSubQueryPlan.addLogicalVertex(filterVertex);
            logicalSubQueryPlan.addLogicalEdge(outputVertex, filterVertex, LogicalEdge.forwardEdge());

            for (TreeNode treeNode : optionTreeNodeList) {
                LogicalQueryPlan subQueryPlan = TreeNodeUtils.buildQueryPlanWithSource(treeNode,
                        contextManager.getTreeNodeLabelManager(),
                        contextManager,
                        contextManager.getVertexIdManager(),
                        filterVertex);
                unionVertexList.add(subQueryPlan.getOutputVertex());
                logicalSubQueryPlan.mergeLogicalQueryPlan(subQueryPlan);
            }
        }

        if (null != noneTreeNode) {
            ProcessorFilterFunction filterFunction = new ProcessorFilterFunction(QueryFlowOuterClass.OperatorType.FILTER,
                    createArgumentBuilder().setBoolValue(true));
            filterFunction.getLogicalCompareList().addAll(noneCompareList);
            LogicalVertex filterVertex = new LogicalUnaryVertex(contextManager.getVertexIdManager().getId(),
                    filterFunction,
                    outputVertex);
            logicalSubQueryPlan.addLogicalVertex(filterVertex);
            logicalSubQueryPlan.addLogicalEdge(outputVertex, filterVertex, LogicalEdge.forwardEdge());

            LogicalQueryPlan subQueryPlan = TreeNodeUtils.buildQueryPlanWithSource(noneTreeNode,
                    contextManager.getTreeNodeLabelManager(),
                    contextManager,
                    contextManager.getVertexIdManager(),
                    filterVertex);
            unionVertexList.add(subQueryPlan.getOutputVertex());
            logicalSubQueryPlan.mergeLogicalQueryPlan(subQueryPlan);
        }

        if (null != anyTreeNode) {
            LogicalQueryPlan subQueryPlan = TreeNodeUtils.buildQueryPlanWithSource(anyTreeNode,
                    contextManager.getTreeNodeLabelManager(),
                    contextManager,
                    contextManager.getVertexIdManager(),
                    outputVertex);
            unionVertexList.add(subQueryPlan.getOutputVertex());
            logicalSubQueryPlan.mergeLogicalQueryPlan(subQueryPlan);
        }

        LogicalVertex currentUnionVertex = unionVertexList.remove(0);
        for (LogicalVertex logicalVertex : unionVertexList) {
            LogicalVertex unionVertex = new LogicalBinaryVertex(contextManager.getVertexIdManager().getId(),
                    new ProcessorFunction(QueryFlowOuterClass.OperatorType.UNION),
                    false,
                    currentUnionVertex,
                    logicalVertex);
            logicalSubQueryPlan.addLogicalVertex(unionVertex);
            logicalSubQueryPlan.addLogicalEdge(currentUnionVertex, unionVertex, LogicalEdge.forwardEdge());
            logicalSubQueryPlan.addLogicalEdge(logicalVertex, unionVertex, LogicalEdge.forwardEdge());
            currentUnionVertex = unionVertex;
        }

        addUsedLabelAndRequirement(currentUnionVertex, contextManager.getTreeNodeLabelManager());
        setFinishVertex(currentUnionVertex, contextManager.getTreeNodeLabelManager());

        return logicalSubQueryPlan;
    }

    @Override
    public ValueType getOutputValueType() {
        return getInputNode().getOutputValueType();
    }

    void setOptionTreeNodeList(Map<Object, List<TreeNode>> optionPickList) {
        this.optionPickList = optionPickList;
    }

    void setNoneTreeNode(TreeNode noneTreeNode) {
        this.noneTreeNode = noneTreeNode;
    }

    void setAnyTreeNode(TreeNode anyTreeNode) {
        this.anyTreeNode = anyTreeNode;
    }
}

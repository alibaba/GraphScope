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
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.logical.LogicalBinaryVertex;
import com.alibaba.maxgraph.compiler.logical.edge.EdgeShuffleType;
import com.alibaba.maxgraph.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.compiler.tree.addition.JoinZeroNode;
import com.alibaba.maxgraph.compiler.utils.TreeNodeUtils;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.compiler.logical.LogicalEdge;
import com.alibaba.maxgraph.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.compiler.logical.LogicalUnaryVertex;
import com.alibaba.maxgraph.compiler.logical.LogicalVertex;
import com.alibaba.maxgraph.compiler.logical.VertexIdManager;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.compiler.utils.CompilerUtils;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class WherePredicateTreeNode extends UnaryTreeNode {
    private P<?> predicate;
    private String startKey;
    private boolean ringEmptyFlag;
    private TreeNode sourceNode;
    private TreeNode targetNode;

    public WherePredicateTreeNode(TreeNode prev, GraphSchema schema, P<?> predicate, String startKey, boolean ringEmptyFlag) {
        super(prev, NodeType.FILTER, schema);
        this.predicate = predicate;
        if (this.predicate instanceof ConnectiveP) {
            throw new UnsupportedOperationException("Not support and/or predicate yet.");
        }
        this.startKey = startKey;
        this.ringEmptyFlag = ringEmptyFlag;
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        TreeNodeLabelManager labelManager = contextManager.getTreeNodeLabelManager();
        VertexIdManager vertexIdManager = contextManager.getVertexIdManager();
        Map<String, Integer> labelIndexList = labelManager.getLabelIndexList();
        if (ringEmptyFlag) {
            Message.CompareType compareType = CompilerUtils.parseCompareType(predicate, predicate.getBiPredicate());
            if (StringUtils.isEmpty(startKey)) {
                String predicateKey = getPredicateValue();
                if (null != contextManager.getStoreVertex(predicateKey)) {
                    LogicalSubQueryPlan logicalSubQueryPlan = new LogicalSubQueryPlan(contextManager);
                    LogicalVertex storeVertex = contextManager.getStoreVertex(predicateKey);
                    LogicalVertex sourceVertex = getInputNode().getOutputVertex();
                    logicalSubQueryPlan.addLogicalVertex(sourceVertex);

                    ProcessorFunction joinStoreFilterFunction = new ProcessorFunction(
                            QueryFlowOuterClass.OperatorType.JOIN_STORE_FILTER,
                            Message.Value.newBuilder().setIntValue(CompilerUtils.parseCompareType(predicate, predicate.getBiPredicate()).getNumber()));
                    LogicalBinaryVertex logicalBinaryVertex = new LogicalBinaryVertex(vertexIdManager.getId(), joinStoreFilterFunction, getInputNode().isPropLocalFlag(), sourceVertex, storeVertex);
                    logicalSubQueryPlan.addLogicalVertex(logicalBinaryVertex);
                    logicalSubQueryPlan.addLogicalEdge(sourceVertex, logicalBinaryVertex, new LogicalEdge(EdgeShuffleType.FORWARD));

                    setFinishVertex(logicalBinaryVertex, labelManager);
                    addUsedLabelAndRequirement(logicalBinaryVertex, labelManager);

                    return logicalSubQueryPlan;
                } else {
                    int whereLabelId = labelIndexList.getOrDefault(predicateKey, TreeConstants.MAGIC_LABEL_ID);
                    ProcessorFunction processorFunction = new ProcessorFunction(QueryFlowOuterClass.OperatorType.WHERE,
                            Message.Value.newBuilder().addIntValueList(compareType.getNumber())
                                    .addIntValueList(whereLabelId));
                    processorFunction.getUsedLabelList().add(whereLabelId);
                    return parseSingleUnaryVertex(vertexIdManager, labelManager, processorFunction, contextManager);
                }
            } else {
                int startLabelId = labelIndexList.getOrDefault(startKey, TreeConstants.MAGIC_LABEL_ID);

                String predicateKey = getPredicateValue();
                if (null != contextManager.getStoreVertex(predicateKey)) {
                    LogicalSubQueryPlan logicalSubQueryPlan = new LogicalSubQueryPlan(contextManager);
                    LogicalVertex storeVertex = contextManager.getStoreVertex(predicateKey);
                    LogicalVertex sourceVertex = getInputNode().getOutputVertex();
                    logicalSubQueryPlan.addLogicalVertex(sourceVertex);

                    ProcessorFunction joinStoreFilterFunction = new ProcessorFunction(
                            QueryFlowOuterClass.OperatorType.JOIN_STORE_FILTER,
                            Message.Value.newBuilder().setIntValue(startLabelId));
                    LogicalBinaryVertex logicalBinaryVertex = new LogicalBinaryVertex(vertexIdManager.getId(), joinStoreFilterFunction, getInputNode().isPropLocalFlag(), sourceVertex, storeVertex);
                    logicalSubQueryPlan.addLogicalVertex(logicalBinaryVertex);
                    logicalSubQueryPlan.addLogicalEdge(sourceVertex, logicalBinaryVertex, new LogicalEdge(EdgeShuffleType.FORWARD));

                    setFinishVertex(logicalBinaryVertex, labelManager);
                    addUsedLabelAndRequirement(logicalBinaryVertex, labelManager);

                    return logicalSubQueryPlan;
                } else {
                    int whereLabelId = labelIndexList.getOrDefault(predicateKey, TreeConstants.MAGIC_LABEL_ID);
                    ProcessorFunction processorFunction = new ProcessorFunction(QueryFlowOuterClass.OperatorType.WHERE_LABEL,
                            Message.Value.newBuilder().addIntValueList(compareType.getNumber())
                                    .addIntValueList(startLabelId)
                                    .addIntValueList(whereLabelId));
                    processorFunction.getUsedLabelList().addAll(Lists.newArrayList(startLabelId, whereLabelId));
                    if ((startLabelId < 0 && whereLabelId < 0) || getInputNode().isPropLocalFlag()) {
                        return parseSingleUnaryVertex(vertexIdManager, labelManager, processorFunction, contextManager);
                    } else {
                        return parseSingleUnaryVertex(vertexIdManager, labelManager, processorFunction, contextManager);
                    }
                }
            }
        } else {
            LogicalSubQueryPlan logicalSubQueryPlan = new LogicalSubQueryPlan(contextManager);
            LogicalVertex delegateSourceVertex = getInputNode().getOutputVertex();
            logicalSubQueryPlan.addLogicalVertex(delegateSourceVertex);
            String inputLabel = labelManager.createSysLabelStart(getInputNode().getOutputVertex(), "val");

            TreeNode firstRingNode = sourceNode;
            if (TreeNodeUtils.checkMultipleOutput(firstRingNode)) {
                firstRingNode = new RangeGlobalTreeNode(firstRingNode, schema, 0, 1);
            }
            TreeNode secondRingNode = targetNode;
            if (TreeNodeUtils.checkMultipleOutput(secondRingNode)) {
                secondRingNode = new RangeGlobalTreeNode(secondRingNode, schema, 0, 1);
            }

            int sourceLabelId, targetLabelId;
            boolean selfValueFlag = false;
            if (!TreeNodeUtils.checkJoinSourceFlag(firstRingNode)) {
                // first ring is value node
                if (!TreeNodeUtils.checkJoinSourceFlag(secondRingNode)) {
                    // second ring is value node
                    sourceLabelId = processFirstValueNode(
                            contextManager,
                            vertexIdManager,
                            labelManager,
                            logicalSubQueryPlan,
                            firstRingNode);
                    targetLabelId = processSecondValueNode(
                            contextManager,
                            vertexIdManager,
                            labelManager,
                            logicalSubQueryPlan,
                            secondRingNode);
                } else {
                    // second ring is not value node
                    targetLabelId = processComplexNode(contextManager, vertexIdManager, labelManager, logicalSubQueryPlan, delegateSourceVertex, secondRingNode);
                    sourceLabelId = processFirstValueNode(
                            contextManager,
                            vertexIdManager,
                            labelManager,
                            logicalSubQueryPlan,
                            firstRingNode);
                }
            } else {
                // first ring is not value node
                if (!TreeNodeUtils.checkJoinSourceFlag(secondRingNode)) {
                    // second ring is value node
                    sourceLabelId = processComplexNode(contextManager, vertexIdManager, labelManager, logicalSubQueryPlan, logicalSubQueryPlan.getOutputVertex(), firstRingNode);
                    targetLabelId = processSecondValueNode(
                            contextManager,
                            vertexIdManager,
                            labelManager,
                            logicalSubQueryPlan,
                            secondRingNode);
                } else {
                    // second ring is not value node
                    sourceLabelId = processComplexNode(contextManager, vertexIdManager, labelManager, logicalSubQueryPlan, logicalSubQueryPlan.getOutputVertex(), firstRingNode);
                    targetLabelId = processComplexNode(contextManager, vertexIdManager, labelManager, logicalSubQueryPlan, logicalSubQueryPlan.getOutputVertex(), secondRingNode);
                    selfValueFlag = true;
                }
            }

            LogicalVertex currentOutputVertex = logicalSubQueryPlan.getOutputVertex();
            Message.Value.Builder argumentBuilder = Message.Value.newBuilder()
                    .addIntValueList(CompilerUtils.parseCompareType(predicate, predicate.getBiPredicate()).getNumber())
                    .addIntValueList(sourceLabelId)
                    .addIntValueList(targetLabelId);
            ProcessorFunction whereFunction = new ProcessorFunction(QueryFlowOuterClass.OperatorType.WHERE_LABEL, argumentBuilder);
            whereFunction.getUsedLabelList().add(sourceLabelId);
            whereFunction.getUsedLabelList().add(targetLabelId);
            LogicalVertex whereVertex = new LogicalUnaryVertex(vertexIdManager.getId(), whereFunction, getInputNode().isPropLocalFlag(), currentOutputVertex);
            logicalSubQueryPlan.addLogicalVertex(whereVertex);
            logicalSubQueryPlan.addLogicalEdge(currentOutputVertex, whereVertex, new LogicalEdge(EdgeShuffleType.FORWARD));

            if (!selfValueFlag) {
                ProcessorFunction inputValueFunction = TreeNodeUtils.createSelectOneFunction(inputLabel, Pop.first, labelIndexList);
                LogicalVertex logicalVertex = new LogicalUnaryVertex(vertexIdManager.getId(), inputValueFunction, false, whereVertex);
                logicalSubQueryPlan.addLogicalVertex(logicalVertex);
                logicalSubQueryPlan.addLogicalEdge(whereVertex, logicalVertex, new LogicalEdge(EdgeShuffleType.FORWARD));
            }

            LogicalVertex outputVertex = logicalSubQueryPlan.getOutputVertex();
            addUsedLabelAndRequirement(outputVertex, labelManager);
            setFinishVertex(outputVertex, labelManager);

            List<LogicalVertex> logicalVertexList = logicalSubQueryPlan.getLogicalVertexList();
            logicalVertexList.remove(delegateSourceVertex);

            return logicalSubQueryPlan;
        }
    }

    private int processSecondValueNode(ContextManager contextManager,
                                       VertexIdManager vertexIdManager,
                                       TreeNodeLabelManager labelManager,
                                       LogicalSubQueryPlan logicalSubQueryPlan,
                                       TreeNode secondRingNode) {
        LogicalVertex valueVertex = logicalSubQueryPlan.getOutputVertex();
        LogicalSubQueryPlan targetSubQueryPlan = TreeNodeUtils.buildSubQueryPlan(
                secondRingNode,
                valueVertex,
                contextManager);
        logicalSubQueryPlan.mergeLogicalQueryPlan(targetSubQueryPlan);
        LogicalVertex targetValueVertex = targetSubQueryPlan.getOutputVertex();
        String targetLabel = labelManager.createSysLabelStart(targetValueVertex, "val");
        return labelManager.getLabelIndex(targetLabel);
    }

    private int processFirstValueNode(ContextManager contextManager,
                                      VertexIdManager vertexIdManager,
                                      TreeNodeLabelManager labelManager,
                                      LogicalSubQueryPlan logicalSubQueryPlan,
                                      TreeNode firstRingNode) {
        LogicalVertex currentSourceVertex = logicalSubQueryPlan.getOutputVertex();
        LogicalSubQueryPlan subQueryPlan = TreeNodeUtils.buildSubQueryPlan(
                firstRingNode,
                currentSourceVertex,
                contextManager);
        logicalSubQueryPlan.mergeLogicalQueryPlan(subQueryPlan);
        LogicalVertex valueVertex = subQueryPlan.getOutputVertex();
        String sourceLabel = labelManager.createSysLabelStart(valueVertex, "val");

        return labelManager.getLabelIndex(sourceLabel);
    }

    private int processComplexNode(ContextManager contextManager,
                                   VertexIdManager vertexIdManager,
                                   TreeNodeLabelManager labelManager,
                                   LogicalSubQueryPlan logicalSubQueryPlan,
                                   LogicalVertex sourceVertex,
                                   TreeNode ringNode) {
        if (ringNode instanceof JoinZeroNode) {
            ((JoinZeroNode) ringNode).disableJoinZero();
        }
        LogicalSubQueryPlan subQueryPlan = TreeNodeUtils.buildSubQueryPlan(
                ringNode,
                sourceVertex,
                contextManager);
        LogicalVertex secondValueVertex = subQueryPlan.getOutputVertex();
        logicalSubQueryPlan.mergeLogicalQueryPlan(subQueryPlan);
        String secondValueLabel = labelManager.createSysLabelStart("val");
        int labelId = labelManager.getLabelIndex(secondValueLabel);
        QueryFlowOuterClass.OperatorType joinType = CompilerUtils.parseJoinOperatorType(ringNode);
        LogicalBinaryVertex joinVertex = new LogicalBinaryVertex(
                vertexIdManager.getId(),
                new ProcessorFunction(joinType, Message.Value.newBuilder().setIntValue(labelId)),
                false,
                sourceVertex,
                secondValueVertex);
        logicalSubQueryPlan.addLogicalVertex(joinVertex);
        logicalSubQueryPlan.addLogicalEdge(sourceVertex, joinVertex, new LogicalEdge());
        logicalSubQueryPlan.addLogicalEdge(secondValueVertex, joinVertex, new LogicalEdge());
        return labelId;
    }

    @Override
    public ValueType getOutputValueType() {
        return getInputNode().getOutputValueType();
    }

    void setSourceTargetNode(TreeNode sourceNode, TreeNode targetNode) {
        this.sourceNode = sourceNode;
        this.targetNode = targetNode;
    }

    public String getStartKey() {
        return startKey;
    }

    public String getPredicateValue() {
        return predicate.getValue() instanceof Collection ?
                ((Collection) predicate.getValue()).iterator().next().toString() : predicate.getValue().toString();
    }
}

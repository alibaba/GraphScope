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
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.compiler.tree.value.VarietyValueType;
import com.alibaba.maxgraph.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.compiler.optimizer.ContextManager;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Set;

public class CaseWhenTreeNode extends UnaryTreeNode {
    private TreeNode caseTreeNode;
    private List<Pair<TreeNode, TreeNode>> whenThenNodeList;
    private TreeNode elseEndTreeNode;

    public CaseWhenTreeNode(
            TreeNode prev,
            GraphSchema schema,
            TreeNode caseTreeNode,
            List<Pair<TreeNode, TreeNode>> whenThenNodeList,
            TreeNode elseEndTreeNode) {
        super(prev, NodeType.FLATMAP, schema);
        this.caseTreeNode = caseTreeNode;
        this.whenThenNodeList = whenThenNodeList;
        this.elseEndTreeNode = elseEndTreeNode;
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        throw new IllegalArgumentException("not support case when");
        //        LogicalSubQueryPlan caseWhenPlan = new LogicalSubQueryPlan(treeNodeLabelManager,
        // contextManager);
        //        LogicalVertex sourceVertex = getInputNode().getOutputVertex();
        //        caseWhenPlan.addLogicalVertex(sourceVertex);
        //
        //        TreeNode currentCaseTreeNode = caseTreeNode;
        //        if (currentCaseTreeNode instanceof ByKeyTreeNode) {
        //            ((ByKeyTreeNode)
        // currentCaseTreeNode).addByKey(treeNodeLabelManager.createSysLabelStart(sourceVertex,
        // "val"),
        //                    sourceVertex,
        //                    getInputNode().getOutputValueType());
        //        }
        //        if (TreeNodeUtils.checkMultipleOutput(currentCaseTreeNode)) {
        //            currentCaseTreeNode = new RangeGlobalTreeNode(currentCaseTreeNode, schema, 0,
        // 1);
        //            ((RangeGlobalTreeNode)
        // currentCaseTreeNode).addByKey(treeNodeLabelManager.createSysLabelStart(sourceVertex,
        // "val"),
        //                    sourceVertex,
        //                    getInputNode().getOutputValueType());
        //        }
        //        parseSingleResultForSource(
        //                currentCaseTreeNode,
        //                getInputNode().getOutputValueType(),
        //                contextManager,
        //                vertexIdManager,
        //                treeNodeLabelManager,
        //                caseWhenPlan,
        //                sourceVertex);
        //        LogicalVertex caseVertex = caseWhenPlan.getOutputVertex();
        //        if (caseVertex.getProcessorFunction().getOperatorType() ==
        // QueryFlowOuterClass.OperatorType.JOIN_LABEL) {
        //            int labelId =
        // caseVertex.getProcessorFunction().getArgumentBuilder().getIntValue();
        //            String labelName = treeNodeLabelManager.getLabelName(labelId);
        //            ProcessorFunction processorFunction = createSelectOneFunction(labelName,
        // Pop.last, treeNodeLabelManager.getLabelIndexList());
        //            LogicalUnaryVertex selectVertex = new
        // LogicalUnaryVertex(vertexIdManager.getId(), processorFunction, false, caseVertex);
        //            caseWhenPlan.addLogicalVertex(selectVertex);
        //            caseWhenPlan.addLogicalEdge(caseVertex, selectVertex, new
        // LogicalEdge(EdgeShuffleType.FORWARD));
        //            caseVertex = selectVertex;
        //        }
        //
        //        Message.Value.Builder caseWhenArgument = Message.Value.newBuilder();
        //        LogicalVertex caseWhenVertex = new LogicalBinaryVertex(vertexIdManager.getId(),
        //                new ProcessorFunction(QueryFlowOuterClass.OperatorType.JOIN_CASE_WHEN,
        // caseWhenArgument),
        //                false,
        //                sourceVertex,
        //                caseVertex);
        //        caseWhenPlan.addLogicalVertex(caseWhenVertex);
        //        caseWhenPlan.addLogicalEdge(caseVertex, caseWhenVertex, new LogicalEdge());
        //        caseTreeNode.setFinishVertex(caseWhenVertex, treeNodeLabelManager);
        //
        //        List<List<QueryFlowOuterClass.LogicalCompare>> whenCompareList =
        // Lists.newArrayList();
        //        int streamIndex = 0;
        //        for (Pair<TreeNode, TreeNode> whenThenPair : whenThenNodeList) {
        //            TreeNode whenTreeNode = whenThenPair.getLeft();
        //            if (whenTreeNode instanceof UnaryTreeNode) {
        //                if (((UnaryTreeNode) whenTreeNode).getInputNode() instanceof
        // SourceDelegateNode) {
        //                    TreeNode thenTreeNode = whenThenPair.getRight();
        //                    LogicalQueryPlan whenPlan =
        // whenTreeNode.buildLogicalQueryPlan(contextManager, vertexIdManager,
        // treeNodeLabelManager);
        //                    LogicalVertex whenVertex = whenPlan.getOutputVertex();
        //                    if (whenVertex.getProcessorFunction().getOperatorType() ==
        // QueryFlowOuterClass.OperatorType.HAS
        //                            || whenVertex.getProcessorFunction().getOperatorType() ==
        // QueryFlowOuterClass.OperatorType.FILTER) {
        //                        List<QueryFlowOuterClass.LogicalCompare> logicalCompareList =
        // whenVertex.getProcessorFunction().getLogicalCompareList();
        //                        if (0 == streamIndex) {
        //                            parseResultForSource(thenTreeNode, contextManager,
        // vertexIdManager, treeNodeLabelManager, caseWhenPlan, caseWhenVertex, 0);
        //                        } else {
        //                            parseResultForSource(thenTreeNode, contextManager,
        // vertexIdManager, treeNodeLabelManager, caseWhenPlan, caseWhenVertex, streamIndex);
        //                            Pair<LogicalVertex, LogicalVertex> outputPair =
        // caseWhenPlan.getOutputVertexPair();
        //                            LogicalVertex unionVertex = new
        // LogicalBinaryVertex(vertexIdManager.getId(), new
        // ProcessorFunction(QueryFlowOuterClass.OperatorType.UNION, rangeLimit), false,
        // outputPair.getLeft(), outputPair.getRight());
        //                            caseWhenPlan.addLogicalVertex(unionVertex);
        //                            caseWhenPlan.addLogicalEdge(outputPair.getLeft(), unionVertex,
        // new LogicalEdge(EdgeShuffleType.FORWARD));
        //                            caseWhenPlan.addLogicalEdge(outputPair.getRight(),
        // unionVertex, new LogicalEdge(EdgeShuffleType.FORWARD));
        //                        }
        //                        whenCompareList.add(logicalCompareList);
        //                        streamIndex++;
        //                    } else {
        //                        throw new IllegalArgumentException("Only support has operator in
        // when");
        //                    }
        //                } else {
        //                    throw new IllegalArgumentException("Only support has operator in
        // when");
        //                }
        //            } else {
        //                throw new IllegalArgumentException("Only support has operator in when");
        //            }
        //        }
        //        List<QueryFlowOuterClass.LogicalCompareListProto> compareListProtoList =
        // Lists.newArrayList();
        //        for (List<QueryFlowOuterClass.LogicalCompare> compareList : whenCompareList) {
        //
        // compareListProtoList.add(QueryFlowOuterClass.LogicalCompareListProto.newBuilder().addAllLogicalCompareList(compareList).build());
        //        }
        //        QueryFlowOuterClass.WhenCompareListProto.Builder whenCompareBuilder =
        // QueryFlowOuterClass.WhenCompareListProto.newBuilder()
        //                .addAllWhenCompareList(compareListProtoList);
        //
        //        if (elseEndTreeNode != null) {
        //            parseResultForSource(elseEndTreeNode, contextManager, vertexIdManager,
        // treeNodeLabelManager, caseWhenPlan, caseWhenVertex, streamIndex);
        //            Pair<LogicalVertex, LogicalVertex> outputPair =
        // caseWhenPlan.getOutputVertexPair();
        //            LogicalVertex unionVertex = new LogicalBinaryVertex(vertexIdManager.getId(),
        // new ProcessorFunction(QueryFlowOuterClass.OperatorType.UNION, rangeLimit), false,
        // outputPair.getLeft(), outputPair.getRight());
        //            caseWhenPlan.addLogicalVertex(unionVertex);
        //            caseWhenPlan.addLogicalEdge(outputPair.getLeft(), unionVertex, new
        // LogicalEdge(EdgeShuffleType.FORWARD));
        //            caseWhenPlan.addLogicalEdge(outputPair.getRight(), unionVertex, new
        // LogicalEdge(EdgeShuffleType.FORWARD));
        //            whenCompareBuilder.setElseEndFlag(true);
        //        }
        //        caseWhenArgument.setPayload(whenCompareBuilder.build()
        //                .toByteString());
        //
        //        LogicalVertex unionVertex = caseWhenPlan.getOutputVertex();
        //        setFinishVertex(unionVertex, treeNodeLabelManager);
        //        return caseWhenPlan;
    }

    @Override
    public ValueType getOutputValueType() {
        Set<ValueType> valueTypeList = Sets.newHashSet();
        for (Pair<TreeNode, TreeNode> whenThenPair : whenThenNodeList) {
            valueTypeList.add(whenThenPair.getRight().getOutputValueType());
        }
        if (null != elseEndTreeNode) {
            valueTypeList.add(elseEndTreeNode.getOutputValueType());
        }
        if (valueTypeList.size() > 1) {
            return new VarietyValueType(valueTypeList);
        } else {
            return valueTypeList.iterator().next();
        }
    }
}

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
import com.alibaba.maxgraph.compiler.logical.edge.EdgeShuffleType;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorLabelValueFunction;
import com.alibaba.maxgraph.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.compiler.utils.TreeNodeUtils;
import com.alibaba.maxgraph.compiler.tree.value.MapValueType;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.compiler.tree.value.ValueValueType;
import com.alibaba.maxgraph.compiler.tree.value.VarietyValueType;
import com.alibaba.maxgraph.compiler.logical.LogicalEdge;
import com.alibaba.maxgraph.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.compiler.logical.LogicalUnaryVertex;
import com.alibaba.maxgraph.compiler.logical.LogicalVertex;
import com.alibaba.maxgraph.compiler.logical.VertexIdManager;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.compiler.tree.source.SourceDelegateNode;
import com.alibaba.maxgraph.compiler.utils.CompilerUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class SelectTreeNode extends UnaryTreeNode {
    private List<String> selectKeyList;
    private Pop pop;
    private Map<String, List<TreeNode>> labelTreeNodeList;
    private List<TreeNode> traversalTreeNodeList = Lists.newArrayList();

    public SelectTreeNode(
            TreeNode input,
            List<String> selectKeyList,
            Pop pop,
            Map<String, List<TreeNode>> labelTreeNodeList,
            GraphSchema schema) {
        super(input, NodeType.MAP, schema);
        this.selectKeyList = selectKeyList;
        this.pop = pop;
        this.labelTreeNodeList = labelTreeNodeList;
        this.selectKeyList.forEach(v -> getUsedLabelList().add(v));
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        TreeNodeLabelManager labelManager = contextManager.getTreeNodeLabelManager();
        VertexIdManager vertexIdManager = contextManager.getVertexIdManager();
        Set<String> labelStartList =
                (Set<String>)
                        this.beforeRequirementList.get(
                                QueryFlowOuterClass.RequirementType.LABEL_START);
        if (!traversalTreeNodeList.isEmpty()
                && null != labelStartList
                && !labelStartList.isEmpty()) {
            for (int i = 0; i < selectKeyList.size(); i++) {
                String selectKey = selectKeyList.get(i);
                TreeNode selectTreeNode =
                        traversalTreeNodeList.get(i % traversalTreeNodeList.size());
                if (!(selectTreeNode instanceof SourceDelegateNode)) {
                    labelStartList.remove(selectKey);
                }
            }
        }
        if (null == labelStartList || labelStartList.isEmpty()) {
            this.beforeRequirementList.remove(QueryFlowOuterClass.RequirementType.LABEL_START);
        }
        if (!contextManager.getCostModelManager().hasBestPath()) {
            Map<String, Integer> labelIndexList = labelManager.getLabelIndexList();
            if (traversalTreeNodeList.isEmpty()) {
                Set<String> selectKeySet = Sets.newHashSet();
                Message.Value.Builder argumentBuilder =
                        Message.Value.newBuilder()
                                .setBoolValue(true)
                                .setIntValue(
                                        pop == null
                                                ? Message.PopType.POP_EMPTY.getNumber()
                                                : Message.PopType.valueOf(
                                                                StringUtils.upperCase(pop.name()))
                                                        .getNumber());
                List<Integer> selectKeyIdList = Lists.newArrayList();
                for (String selectKey : selectKeyList) {
                    if (selectKeySet.contains(selectKey)) {
                        continue;
                    }
                    selectKeySet.add(selectKey);
                    if (!labelIndexList.containsKey(selectKey)) {
                        argumentBuilder.setBoolValue(false);
                        break;
                    }
                    selectKeyIdList.add(labelIndexList.get(selectKey));
                }
                argumentBuilder
                        .addAllIntValueList(selectKeyIdList)
                        .addAllStrValueList(
                                selectKeyIdList.stream()
                                        .map(v -> labelManager.getLabelName(v))
                                        .collect(Collectors.toList()));
                ProcessorFunction processorFunction =
                        new ProcessorFunction(
                                QueryFlowOuterClass.OperatorType.SELECT, argumentBuilder);
                return parseSingleUnaryVertex(
                        vertexIdManager, labelManager, processorFunction, contextManager);
            } else {
                LogicalSubQueryPlan logicalSubQueryPlan =
                        parseJoinQueryPlan(
                                contextManager, vertexIdManager, labelManager, labelIndexList);
                LogicalVertex outputVertex = logicalSubQueryPlan.getOutputVertex();
                addUsedLabelAndRequirement(outputVertex, labelManager);
                setFinishVertex(outputVertex, labelManager);

                return logicalSubQueryPlan;
            }
        } else {
            Map<String, Integer> labelIndexList = labelManager.getLabelIndexList();
            Set<String> selectKeySet = Sets.newHashSet();
            Message.Value.Builder argumentBuilder =
                    Message.Value.newBuilder()
                            .setBoolValue(true)
                            .setIntValue(
                                    pop == null
                                            ? Message.PopType.POP_EMPTY.getNumber()
                                            : Message.PopType.valueOf(
                                                            StringUtils.upperCase(pop.name()))
                                                    .getNumber());
            List<Integer> selectKeyIdList = Lists.newArrayList();
            for (String selectKey : selectKeyList) {
                if (selectKeySet.contains(selectKey)) {
                    continue;
                }
                selectKeySet.add(selectKey);
                if (!labelIndexList.containsKey(selectKey)) {
                    argumentBuilder.setBoolValue(false);
                    break;
                }
                selectKeyIdList.add(labelIndexList.get(selectKey));
            }
            argumentBuilder
                    .addAllIntValueList(selectKeyIdList)
                    .addAllStrValueList(
                            selectKeyIdList.stream()
                                    .map(
                                            v ->
                                                    contextManager
                                                            .getTreeNodeLabelManager()
                                                            .getLabelName(v))
                                    .collect(Collectors.toList()));
            ProcessorFunction processorFunction =
                    new ProcessorFunction(QueryFlowOuterClass.OperatorType.SELECT, argumentBuilder);
            return parseSingleUnaryVertex(
                    contextManager.getVertexIdManager(),
                    contextManager.getTreeNodeLabelManager(),
                    processorFunction,
                    contextManager);
        }
    }

    private LogicalSubQueryPlan parseJoinQueryPlan(
            ContextManager contextManager,
            VertexIdManager vertexIdManager,
            TreeNodeLabelManager labelManager,
            Map<String, Integer> labelIndexList) {
        LogicalSubQueryPlan logicalSubQueryPlan = new LogicalSubQueryPlan(contextManager);
        LogicalVertex delegateSourceVertex = getInputNode().getOutputVertex();
        logicalSubQueryPlan.addLogicalVertex(delegateSourceVertex);

        Set<String> selectKeySet = Sets.newHashSet();
        List<Integer> keyIdList = Lists.newArrayList();
        List<Integer> valueIdList = Lists.newArrayList();
        for (int i = 0; i < selectKeyList.size(); i++) {
            String selectKey = selectKeyList.get(i);
            if (selectKeySet.contains(selectKey)) {
                continue;
            }
            selectKeySet.add(selectKey);
            TreeNode traversalTreeNode =
                    traversalTreeNodeList.get(i % traversalTreeNodeList.size());
            int labelIndex = labelIndexList.get(selectKey);
            keyIdList.add(labelIndex);
            valueIdList.add(labelIndexList.get(selectKey));
            if (!(traversalTreeNode instanceof SourceDelegateNode)) {
                TreeNode currentTraversalNode =
                        TreeNodeUtils.buildSingleOutputNode(traversalTreeNode, schema);
                LogicalVertex outputVertex = logicalSubQueryPlan.getOutputVertex();
                LogicalSubQueryPlan traversalPlan =
                        TreeNodeUtils.buildSubQueryPlan(
                                currentTraversalNode, outputVertex, contextManager);
                List<LogicalVertex> resultVertexList = traversalPlan.getLogicalVertexList();
                if (resultVertexList.size() == 2) {
                    LogicalVertex fieldValueVertex = resultVertexList.get(1);
                    ProcessorLabelValueFunction labelValueFunction =
                            new ProcessorLabelValueFunction(labelIndex, fieldValueVertex);
                    labelValueFunction.setRequireLabelFlag(true);
                    LogicalVertex labelValueVertex =
                            new LogicalUnaryVertex(
                                    vertexIdManager.getId(), labelValueFunction, outputVertex);
                    logicalSubQueryPlan.addLogicalVertex(labelValueVertex);
                    logicalSubQueryPlan.addLogicalEdge(
                            outputVertex, labelValueVertex, LogicalEdge.shuffleByKey(labelIndex));
                } else {
                    LogicalVertex fieldValueVertex = traversalPlan.getOutputVertex();
                    fieldValueVertex
                            .getAfterRequirementList()
                            .add(
                                    QueryFlowOuterClass.RequirementValue.newBuilder()
                                            .setReqType(
                                                    QueryFlowOuterClass.RequirementType.LABEL_START)
                                            .setReqArgument(
                                                    Message.Value.newBuilder()
                                                            .addIntValueList(labelIndex)));
                    logicalSubQueryPlan.mergeLogicalQueryPlan(traversalPlan);
                    LogicalVertex outputKeyVertex =
                            new LogicalUnaryVertex(
                                    vertexIdManager.getId(),
                                    new ProcessorFunction(
                                            QueryFlowOuterClass.OperatorType.KEY_MESSAGE),
                                    fieldValueVertex);
                    logicalSubQueryPlan.addLogicalVertex(outputKeyVertex);
                    logicalSubQueryPlan.addLogicalEdge(
                            fieldValueVertex, outputKeyVertex, LogicalEdge.forwardEdge());
                }
            }
        }

        LogicalVertex selectInputVertex = logicalSubQueryPlan.getOutputVertex();
        Message.Value.Builder argumentBuilder =
                Message.Value.newBuilder()
                        .setBoolValue(true)
                        .setIntValue(
                                pop == null
                                        ? Message.PopType.POP_EMPTY.getNumber()
                                        : Message.PopType.valueOf(StringUtils.upperCase(pop.name()))
                                                .getNumber())
                        .addAllIntValueList(valueIdList)
                        .addAllStrValueList(
                                keyIdList.stream()
                                        .map(labelManager::getLabelName)
                                        .collect(Collectors.toList()));
        ProcessorFunction processorFunction =
                new ProcessorFunction(QueryFlowOuterClass.OperatorType.SELECT, argumentBuilder);
        processorFunction.getUsedLabelList().addAll(valueIdList);

        LogicalVertex selectVertex =
                new LogicalUnaryVertex(
                        vertexIdManager.getId(), processorFunction, false, selectInputVertex);
        logicalSubQueryPlan.addLogicalVertex(selectVertex);
        logicalSubQueryPlan.addLogicalEdge(
                selectInputVertex, selectVertex, new LogicalEdge(EdgeShuffleType.FORWARD));
        return logicalSubQueryPlan;
    }

    @Override
    public ValueType getOutputValueType() {
        Set<ValueType> labelValueTypeList = Sets.newHashSet();
        if (traversalTreeNodeList.isEmpty()) {
            labelTreeNodeList
                    .values()
                    .forEach(
                            v ->
                                    labelValueTypeList.add(
                                            CompilerUtils.parseValueTypeWithPop(v, pop)));
        } else {
            for (int i = 0; i < selectKeyList.size(); i++) {
                labelValueTypeList.add(
                        traversalTreeNodeList
                                .get(i % traversalTreeNodeList.size())
                                .getOutputValueType());
            }
        }

        return new MapValueType(
                new ValueValueType(Message.VariantType.VT_STRING),
                labelValueTypeList.isEmpty()
                        ? null
                        : (labelValueTypeList.size() > 1
                                ? new VarietyValueType(labelValueTypeList)
                                : Lists.newArrayList(labelValueTypeList).get(0)));
    }

    public void addTraversalTreeNode(TreeNode treeNode) {
        traversalTreeNodeList.add(treeNode);
    }

    public Map<String, TreeNode> getLabelValueTreeNodeList() {
        Map<String, TreeNode> labelValueTreeNodeList = Maps.newHashMap();
        if (!traversalTreeNodeList.isEmpty()) {
            for (int i = 0; i < selectKeyList.size(); i++) {
                TreeNode treeNode = traversalTreeNodeList.get(i % traversalTreeNodeList.size());
                labelValueTreeNodeList.put(selectKeyList.get(i), treeNode);
            }
        }
        return labelValueTreeNodeList;
    }

    public List<String> getSelectKeyList() {
        return selectKeyList;
    }

    public Map<String, TreeNode> getLabelTreeNodeList() {
        Map<String, TreeNode> labelNodeList = Maps.newHashMap();
        for (String labelKey : selectKeyList) {
            List<TreeNode> nodeList = labelTreeNodeList.get(labelKey);
            if (null != nodeList && nodeList.size() == 1) {
                labelNodeList.put(labelKey, nodeList.get(0));
            }
        }

        return labelNodeList;
    }
}

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
package com.alibaba.maxgraph.compiler.logical;

import com.alibaba.maxgraph.Message;
import com.alibaba.maxgraph.QueryFlowOuterClass;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.cost.CostMappingManager;
import com.alibaba.maxgraph.compiler.cost.CostModelManager;
import com.alibaba.maxgraph.compiler.cost.RowField;
import com.alibaba.maxgraph.compiler.cost.RowFieldManager;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorLabelValueFunction;
import com.alibaba.maxgraph.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.compiler.tree.BaseTreeNode;
import com.alibaba.maxgraph.compiler.tree.TreeManager;
import com.alibaba.maxgraph.compiler.tree.TreeNode;
import com.alibaba.maxgraph.compiler.tree.TreeNodeLabelManager;
import com.alibaba.maxgraph.compiler.tree.source.SourceTreeNode;
import com.alibaba.maxgraph.compiler.utils.CompilerUtils;
import com.alibaba.maxgraph.compiler.utils.TreeNodeUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class LogicalPlanBuilder {

    public static LogicalPlanBuilder newBuilder() {
        return new LogicalPlanBuilder();
    }

    /**
     * Build logical plan from tree manager
     *
     * @param treeManager The given tree manager
     * @return The result logical plan
     */
    public LogicalQueryPlan build(TreeManager treeManager) {
        CostModelManager costModelManager = treeManager.optimizeCostModel();
        TreeNodeLabelManager labelManager = treeManager.getLabelManager();
        VertexIdManager vertexIdManager = VertexIdManager.createVertexIdManager();
        ContextManager contextManager =
                new ContextManager(
                        costModelManager,
                        treeManager.getQueryConfig(),
                        vertexIdManager,
                        labelManager);
        TreeNode leafNode = treeManager.getTreeLeaf();

        return buildPlan(leafNode, contextManager, treeManager.getSchema());
    }

    /**
     * Build logical plan from tree directly
     *
     * @param treeNode The given tree node
     * @param contextManager The given context manager
     * @return The result logical plan
     */
    public LogicalQueryPlan buildPlan(
            TreeNode treeNode, ContextManager contextManager, GraphSchema schema) {
        LogicalQueryPlan logicalQueryPlan = new LogicalQueryPlan(contextManager);
        TreeNode currentNode = CompilerUtils.getSourceTreeNode(treeNode);
        Set<Integer> labelIdList = Sets.newHashSet();
        while (currentNode != null) {
            LogicalQueryPlan nodeQueryPlan = currentNode.buildLogicalQueryPlan(contextManager);
            processCostModel(currentNode, nodeQueryPlan, contextManager, schema, labelIdList);
            logicalQueryPlan.mergeLogicalQueryPlan(nodeQueryPlan);
            currentNode = currentNode.getOutputNode();
            contextManager.getCostModelManager().stepNextIndex();
        }
        logicalQueryPlan.setResultValueType(treeNode.getOutputValueType());
        logicalQueryPlan.optimizeLogicalPlan();

        return logicalQueryPlan;
    }

    private void processCostModel(
            TreeNode currentNode,
            LogicalQueryPlan queryPlan,
            ContextManager contextManager,
            GraphSchema schema,
            Set<Integer> labelIdList) {
        CostModelManager costModelManager = contextManager.getCostModelManager();
        TreeNodeLabelManager treeNodeLabelManager = contextManager.getTreeNodeLabelManager();
        VertexIdManager vertexIdManager = contextManager.getVertexIdManager();
        if (costModelManager.hasBestPath()) {
            CostMappingManager costMappingManager = costModelManager.getCostMappingManager();
            RowFieldManager rowFieldManager = costModelManager.getPathFieldManager();
            if (null == rowFieldManager) {
                return;
            }
            RowField rowField = rowFieldManager.getRowField();
            Set<String> birthFieldList = rowFieldManager.getBirthFieldList();
            Set<String> fieldList = rowField.getFieldList();
            for (String field : fieldList) {
                LogicalVertex outputVertex = queryPlan.getOutputVertex();
                if (birthFieldList.contains(field)) {
                    buildLabelValuePlan(
                            queryPlan,
                            contextManager,
                            schema,
                            outputVertex,
                            treeNodeLabelManager,
                            vertexIdManager,
                            costMappingManager,
                            field,
                            currentNode);
                } else {
                    RowFieldManager parentFieldManager = rowFieldManager.getParent();
                    if (!parentFieldManager.getRowField().getFieldList().contains(field)) {
                        buildLabelValuePlan(
                                queryPlan,
                                contextManager,
                                schema,
                                outputVertex,
                                treeNodeLabelManager,
                                vertexIdManager,
                                costMappingManager,
                                field,
                                currentNode);
                    }
                }
            }
            currentNode.setFinishVertex(queryPlan.getOutputVertex(), null);
        }

        List<LogicalVertex> logicalVertexList = queryPlan.getLogicalVertexList();
        for (LogicalVertex vertex : logicalVertexList) {
            if (vertex.getProcessorFunction() != null
                    && vertex.getProcessorFunction().getOperatorType()
                            == QueryFlowOuterClass.OperatorType.LABEL_VALUE) {
                if (vertex.getProcessorFunction().getArgumentBuilder().getIntValue() != 0) {
                    labelIdList.add(
                            vertex.getProcessorFunction().getArgumentBuilder().getIntValue());
                }
            } else {
                vertex.getBeforeRequirementList()
                        .removeIf(
                                vv -> {
                                    if (vv.getReqType()
                                            == QueryFlowOuterClass.RequirementType.LABEL_START) {
                                        List<Integer> intValueList =
                                                Lists.newArrayList(
                                                        vv.getReqArgument().getIntValueListList());
                                        intValueList.removeIf(labelIdList::contains);
                                        if (intValueList.isEmpty()) {
                                            return true;
                                        }
                                        vv.getReqArgumentBuilder()
                                                .clearIntValueList()
                                                .addAllIntValueList(intValueList);
                                    }
                                    return false;
                                });
            }
        }
    }

    private void buildLabelValuePlan(
            LogicalQueryPlan queryPlan,
            ContextManager contextManager,
            GraphSchema schema,
            LogicalVertex outputVertex,
            TreeNodeLabelManager treeNodeLabelManager,
            VertexIdManager vertexIdManager,
            CostMappingManager costMappingManager,
            String field,
            TreeNode currNode) {
        // Create field value here
        String parentField = costMappingManager.getValueParent(field);
        if (!StringUtils.isEmpty(parentField)) {
            BaseTreeNode nextNode = (BaseTreeNode) currNode.getOutputNode();
            nextNode.removeBeforeLabel(parentField);
            // If the field is value field, process it and remove the label requirement in vertex
            int labelIndex = treeNodeLabelManager.getLabelIndex(parentField);
            TreeNode fieldValueTreeNode = costMappingManager.getComputeTreeByValue(parentField);
            if (!(fieldValueTreeNode instanceof SourceTreeNode)) {
                for (QueryFlowOuterClass.RequirementValue.Builder reqValue :
                        outputVertex.getAfterRequirementList()) {
                    if (reqValue.getReqType() == QueryFlowOuterClass.RequirementType.LABEL_START) {
                        List<Integer> labelIndexList =
                                reqValue.getReqArgumentBuilder().getIntValueListList().stream()
                                        .filter(v -> v != labelIndex)
                                        .collect(Collectors.toList());
                        reqValue.getReqArgumentBuilder()
                                .clearIntValueList()
                                .addAllIntValueList(labelIndexList);
                    }
                }
                TreeNode currentFilterTreeNode =
                        TreeNodeUtils.buildSingleOutputNode(fieldValueTreeNode, schema);
                // build filter plan, and use join direct filter vertex to filter left stream
                LogicalSubQueryPlan fieldValuePlan =
                        TreeNodeUtils.buildSubQueryPlan(
                                currentFilterTreeNode, outputVertex, contextManager);
                List<LogicalVertex> fieldValueVertexList = fieldValuePlan.getLogicalVertexList();
                if (fieldValueVertexList.size() == 2) {
                    LogicalVertex fieldValueVertex = fieldValueVertexList.get(1);
                    ProcessorLabelValueFunction labelValueFunction =
                            new ProcessorLabelValueFunction(labelIndex, fieldValueVertex);
                    LogicalVertex labelValueVertex =
                            new LogicalUnaryVertex(
                                    vertexIdManager.getId(), labelValueFunction, outputVertex);
                    queryPlan.addLogicalVertex(labelValueVertex);
                    queryPlan.addLogicalEdge(
                            outputVertex, labelValueVertex, LogicalEdge.shuffleByKey(labelIndex));
                } else {
                    LogicalVertex fieldValueVertex = fieldValuePlan.getOutputVertex();
                    fieldValueVertex
                            .getAfterRequirementList()
                            .add(
                                    QueryFlowOuterClass.RequirementValue.newBuilder()
                                            .setReqType(
                                                    QueryFlowOuterClass.RequirementType.LABEL_START)
                                            .setReqArgument(
                                                    Message.Value.newBuilder()
                                                            .addIntValueList(labelIndex)));
                    queryPlan.mergeLogicalQueryPlan(fieldValuePlan);
                    LogicalVertex outputKeyVertex =
                            new LogicalUnaryVertex(
                                    vertexIdManager.getId(),
                                    new ProcessorFunction(
                                            QueryFlowOuterClass.OperatorType.KEY_MESSAGE),
                                    fieldValueVertex);
                    queryPlan.addLogicalVertex(outputKeyVertex);
                    queryPlan.addLogicalEdge(
                            fieldValueVertex, outputKeyVertex, LogicalEdge.forwardEdge());
                }
            }
        }
    }
}

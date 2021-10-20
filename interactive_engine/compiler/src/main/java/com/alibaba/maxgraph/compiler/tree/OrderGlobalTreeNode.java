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
import com.alibaba.maxgraph.compiler.logical.LogicalQueryPlan;
import com.alibaba.maxgraph.compiler.logical.edge.EdgeShuffleType;
import com.alibaba.maxgraph.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.compiler.tree.addition.AbstractUseKeyNode;
import com.alibaba.maxgraph.compiler.utils.TreeNodeUtils;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.compiler.tree.value.VertexValueType;
import com.alibaba.maxgraph.compiler.logical.LogicalEdge;
import com.alibaba.maxgraph.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.compiler.logical.LogicalUnaryVertex;
import com.alibaba.maxgraph.compiler.logical.LogicalVertex;
import com.alibaba.maxgraph.compiler.logical.VertexIdManager;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.compiler.tree.source.SourceDelegateNode;
import com.alibaba.maxgraph.compiler.utils.CompilerUtils;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Order;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class OrderGlobalTreeNode extends AbstractUseKeyNode {
    private static final long SHUFFLE_THRESHOLD = 0;
    private List<Pair<TreeNode, Order>> treeNodeOrderList;
    private String orderFlagLabel;
    protected boolean orderFlag = false;
    // order in combiner before order in global
    private boolean partitionIdFlag = false;
    private boolean earlyStopFlag = true;
    private boolean orderKeyFlag = false;

    public OrderGlobalTreeNode(
            TreeNode prev, GraphSchema schema, List<Pair<TreeNode, Order>> treeNodeOrderList) {
        super(prev, NodeType.FILTER, schema);
        this.treeNodeOrderList = treeNodeOrderList;
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        LogicalSubQueryPlan logicalSubQueryPlan = new LogicalSubQueryPlan(contextManager);
        LogicalVertex delegateSourceVertex = getInputNode().getOutputVertex();
        logicalSubQueryPlan.addLogicalVertex(delegateSourceVertex);
        TreeNodeLabelManager labelManager = contextManager.getTreeNodeLabelManager();
        VertexIdManager vertexIdManager = contextManager.getVertexIdManager();

        Message.OrderComparatorList.Builder comparatorList =
                Message.OrderComparatorList.newBuilder();
        Set<String> propFillList = Sets.newHashSet();
        Set<Integer> usedLabelIdList = Sets.newHashSet();

        LogicalVertex lastJoinVertex = null;
        LogicalVertex inputVertex = delegateSourceVertex;
        for (int i = 0; i < treeNodeOrderList.size(); i++) {
            Pair<TreeNode, Order> orderPair = treeNodeOrderList.get(i);
            if (orderPair.getLeft() instanceof SourceDelegateNode) {
                comparatorList.addOrderComparator(
                        Message.OrderComparator.newBuilder()
                                .setPropId(0)
                                .setOrderType(
                                        Message.OrderType.valueOf(
                                                StringUtils.upperCase(orderPair.getRight().name())))
                                .build());
            } else {
                UnaryTreeNode unaryTreeNode = UnaryTreeNode.class.cast(orderPair.getLeft());
                if (unaryTreeNode.getInputNode() instanceof SourceDelegateNode
                        && ((unaryTreeNode instanceof SelectOneTreeNode
                                        && SelectOneTreeNode.class
                                                        .cast(unaryTreeNode)
                                                        .getTraversalTreeNode()
                                                == null)
                                || (unaryTreeNode instanceof ElementValueTreeNode
                                        && ElementValueTreeNode.class
                                                        .cast(unaryTreeNode)
                                                        .getByPassTraversal()
                                                == null)
                                || unaryTreeNode instanceof TokenTreeNode
                                || (unaryTreeNode instanceof TraversalMapTreeNode
                                        && ((TraversalMapTreeNode) unaryTreeNode).getTraversalNode()
                                                instanceof ColumnTreeNode
                                        && ((ColumnTreeNode)
                                                                ((TraversalMapTreeNode)
                                                                                unaryTreeNode)
                                                                        .getTraversalNode())
                                                        .getInputNode()
                                                instanceof SourceDelegateNode))) {
                    if (unaryTreeNode instanceof SelectOneTreeNode) {
                        int labelId =
                                labelManager.getLabelIndex(
                                        SelectOneTreeNode.class
                                                .cast(unaryTreeNode)
                                                .getSelectLabel());
                        comparatorList.addOrderComparator(
                                Message.OrderComparator.newBuilder()
                                        .setPropId(labelId)
                                        .setOrderType(
                                                Message.OrderType.valueOf(
                                                        StringUtils.upperCase(
                                                                orderPair.getRight().name())))
                                        .build());
                        usedLabelIdList.add(labelId);
                    } else if (unaryTreeNode instanceof TokenTreeNode) {
                        int labelId =
                                labelManager.getLabelIndex(
                                        TokenTreeNode.class
                                                .cast(unaryTreeNode)
                                                .getToken()
                                                .getAccessor());
                        comparatorList.addOrderComparator(
                                Message.OrderComparator.newBuilder()
                                        .setPropId(labelId)
                                        .setOrderType(
                                                Message.OrderType.valueOf(
                                                        StringUtils.upperCase(
                                                                orderPair.getRight().name())))
                                        .build());
                    } else if (unaryTreeNode instanceof ElementValueTreeNode) {
                        String propKey =
                                ElementValueTreeNode.class
                                        .cast(unaryTreeNode)
                                        .getPropKeyList()
                                        .iterator()
                                        .next();
                        propFillList.add(propKey);
                        comparatorList.addOrderComparator(
                                Message.OrderComparator.newBuilder()
                                        .setPropId(CompilerUtils.getPropertyId(schema, propKey))
                                        .setOrderType(
                                                Message.OrderType.valueOf(
                                                        StringUtils.upperCase(
                                                                orderPair.getRight().name())))
                                        .build());
                    } else {
                        TraversalMapTreeNode traversalMapTreeNode =
                                TraversalMapTreeNode.class.cast(unaryTreeNode);
                        ColumnTreeNode columnTreeNode =
                                (ColumnTreeNode) traversalMapTreeNode.getTraversalNode();
                        comparatorList.addOrderComparator(
                                Message.OrderComparator.newBuilder()
                                        .setPropId(
                                                labelManager.getLabelIndex(
                                                        columnTreeNode.getColumn().name()))
                                        .setOrderType(
                                                Message.OrderType.valueOf(
                                                        StringUtils.upperCase(
                                                                orderPair.getRight().name())))
                                        .build());
                    }
                } else {
                    TreeNode compareTreeNode =
                            TreeNodeUtils.buildSingleOutputNode(orderPair.getLeft(), schema);
                    Pair<LogicalQueryPlan, Integer> planLabelPair =
                            TreeNodeUtils.buildSubQueryWithLabel(
                                    compareTreeNode, inputVertex, contextManager);
                    inputVertex = planLabelPair.getLeft().getOutputVertex();

                    logicalSubQueryPlan.mergeLogicalQueryPlan(planLabelPair.getLeft());
                    comparatorList.addOrderComparator(
                            Message.OrderComparator.newBuilder()
                                    .setPropId(planLabelPair.getRight())
                                    .setOrderType(
                                            Message.OrderType.valueOf(
                                                    StringUtils.upperCase(
                                                            orderPair.getRight().name())))
                                    .build());
                    usedLabelIdList.add(planLabelPair.getRight());
                }
            }
        }

        if (!propFillList.isEmpty()
                && getInputNode().getOutputValueType() instanceof VertexValueType) {
            LogicalVertex propFillInputVertex = logicalSubQueryPlan.getOutputVertex();
            ProcessorFunction propFillFunction =
                    new ProcessorFunction(
                            QueryFlowOuterClass.OperatorType.PROP_FILL,
                            Message.Value.newBuilder()
                                    .addAllIntValueList(
                                            propFillList.stream()
                                                    .map(
                                                            v ->
                                                                    CompilerUtils.getPropertyId(
                                                                            schema, v))
                                                    .collect(Collectors.toSet())));
            LogicalVertex propFillVertex =
                    new LogicalUnaryVertex(
                            vertexIdManager.getId(), propFillFunction, true, propFillInputVertex);
            LogicalEdge logicalEdge;
            if (propFillInputVertex.isPropLocalFlag()) {
                logicalEdge = new LogicalEdge(EdgeShuffleType.FORWARD);
            } else {
                logicalEdge = new LogicalEdge(EdgeShuffleType.SHUFFLE_BY_KEY);
            }
            logicalSubQueryPlan.addLogicalVertex(propFillVertex);
            logicalSubQueryPlan.addLogicalEdge(propFillInputVertex, propFillVertex, logicalEdge);
        }

        Message.Value.Builder argumentBuilder =
                Message.Value.newBuilder().setPayload(comparatorList.build().toByteString());
        if (orderFlag) {
            argumentBuilder
                    .setBoolValue(true)
                    .setIntValue(labelManager.getLabelIndex(orderFlagLabel));
        }
        argumentBuilder
                .setBoolFlag(partitionIdFlag)
                .setLongValue(SHUFFLE_THRESHOLD)
                .setOrderFlag(orderKeyFlag);
        ProcessorFunction orderFunction =
                new ProcessorFunction(
                        QueryFlowOuterClass.OperatorType.ORDER, argumentBuilder, rangeLimit);
        orderFunction.getUsedLabelList().addAll(usedLabelIdList);

        LogicalVertex outputVertex = logicalSubQueryPlan.getOutputVertex();
        LogicalVertex orderVertex =
                new LogicalUnaryVertex(vertexIdManager.getId(), orderFunction, false, outputVertex);
        logicalSubQueryPlan.addLogicalVertex(orderVertex);
        logicalSubQueryPlan.addLogicalEdge(outputVertex, orderVertex, new LogicalEdge());

        addUsedLabelAndRequirement(orderVertex, labelManager);
        setFinishVertex(orderVertex, labelManager);

        return logicalSubQueryPlan;
    }

    public boolean isEmptyOrderNode() {
        return treeNodeOrderList == null
                || treeNodeOrderList.isEmpty()
                || (treeNodeOrderList.size() == 1
                        && treeNodeOrderList.get(0).getLeft() instanceof SourceDelegateNode);
    }

    @Override
    public ValueType getOutputValueType() {
        return getInputNode().getOutputValueType();
    }

    public String enableOrderFlag(TreeNodeLabelManager treeNodeLabelManager) {
        this.orderFlag = true;
        this.orderFlagLabel = treeNodeLabelManager.createSysLabelStart("order");

        return this.orderFlagLabel;
    }

    public void enablePartitionIdFlag() {
        this.partitionIdFlag = true;
    }

    public void enableOrderKeyFlag() {
        this.orderKeyFlag = true;
    }
}

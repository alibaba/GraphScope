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
import com.alibaba.maxgraph.compiler.tree.value.VertexValueType;
import com.alibaba.maxgraph.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.compiler.logical.VertexIdManager;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.compiler.tree.source.SourceTreeNode;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Order;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class OrderLocalTreeNode extends UnaryTreeNode {
    private List<Pair<TreeNode, Order>> orderTreeNodeList;
    private List<Pair<Integer, Message.OrderType>> orderList = Lists.newArrayList();

    public OrderLocalTreeNode(TreeNode prev, GraphSchema schema, List<Pair<TreeNode, Order>> orderTreeNodeList) {
        super(prev, NodeType.MAP, schema);

        Set<String> reqPropList = checkReqPropList(orderTreeNodeList);
        if (reqPropList.size() > 0) {
            TreeNode currentPrev = prev;
            while (currentPrev instanceof UnaryTreeNode) {
                TreeNode prevInput = UnaryTreeNode.class.cast(currentPrev).getInputNode();
                if (prevInput instanceof PropFillTreeNode) {
                    PropFillTreeNode.class.cast(prevInput).getPropKeyList().addAll(reqPropList);
                    break;
                } else if (prevInput.getOutputValueType() instanceof VertexValueType) {
                    PropFillTreeNode propFillTreeNode = new PropFillTreeNode(prevInput, reqPropList, schema);
                    ((UnaryTreeNode) currentPrev).setInputNode(propFillTreeNode);
                    break;
                } else if (prevInput instanceof GroupTreeNode) {
                    GroupTreeNode groupTreeNode = GroupTreeNode.class.cast(prevInput);
                    groupTreeNode.setPropFillValueNode(reqPropList);
                    break;
                }
                currentPrev = prevInput;
            }
        }

        this.orderTreeNodeList = orderTreeNodeList;
    }

    private Set<String> checkReqPropList(List<Pair<TreeNode, Order>> orderTreeNodeList) {
        Set<String> propKeyList = Sets.newHashSet();
        for (Pair<TreeNode, Order> orderPair : orderTreeNodeList) {
            if (orderPair.getLeft() instanceof SourceTreeNode) {
                continue;
            }
            UnaryTreeNode unaryTreeNode = UnaryTreeNode.class.cast(orderPair.getLeft());
            if (unaryTreeNode.getInputNode() instanceof SourceTreeNode) {
                if (unaryTreeNode instanceof ElementValueTreeNode
                        && ((ElementValueTreeNode) unaryTreeNode).getByPassTraversal() == null) {
                    propKeyList.addAll(((ElementValueTreeNode) unaryTreeNode).getPropKeyList());
                }
            } else {
                throw new UnsupportedOperationException("not support sub query in order local yet");
            }
        }
        return propKeyList;
    }

    private int getPropLabelId(TreeNode treeNode, Map<String, Integer> labelIndexList) {
        if (treeNode instanceof SourceTreeNode) {
            return 0;
        } else {
            UnaryTreeNode unaryTreeNode = UnaryTreeNode.class.cast(treeNode);
            if (unaryTreeNode.getInputNode() instanceof SourceTreeNode) {
                if (unaryTreeNode instanceof SelectOneTreeNode) {
                    return labelIndexList.get(((SelectOneTreeNode) unaryTreeNode).getSelectLabel());
                } else if (unaryTreeNode instanceof ColumnTreeNode) {
                    return labelIndexList.get(ColumnTreeNode.class.cast(unaryTreeNode).getColumn().name());
                } else {
                    return SchemaUtils.getPropId(ElementValueTreeNode.class.cast(unaryTreeNode).getPropKeyList().iterator().next(),
                            schema);
                }
            } else {
                throw new IllegalArgumentException();
            }
        }
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        TreeNodeLabelManager labelManager = contextManager.getTreeNodeLabelManager();
        VertexIdManager vertexIdManager = contextManager.getVertexIdManager();

        Message.OrderComparatorList.Builder comparatorList = Message.OrderComparatorList.newBuilder();
        orderTreeNodeList.forEach(v -> {
            comparatorList.addOrderComparator(Message.OrderComparator.newBuilder()
                    .setPropId(getPropLabelId(v.getLeft(), labelManager.getLabelIndexList()))
                    .setOrderType(Message.OrderType.valueOf(StringUtils.upperCase(v.getRight().name()))));
        });
        ProcessorFunction processorFunction = new ProcessorFunction(
                QueryFlowOuterClass.OperatorType.ORDER_LOCAL,
                Message.Value.newBuilder().setPayload(comparatorList.build().toByteString()));
        return parseSingleUnaryVertex(vertexIdManager, labelManager, processorFunction, contextManager);
    }

    @Override
    public ValueType getOutputValueType() {
        return getInputNode().getOutputValueType();
    }
}

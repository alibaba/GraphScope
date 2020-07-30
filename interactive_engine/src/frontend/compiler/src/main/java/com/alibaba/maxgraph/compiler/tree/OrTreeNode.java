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
import com.alibaba.maxgraph.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.compiler.tree.value.VertexValueType;
import com.alibaba.maxgraph.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.compiler.logical.VertexIdManager;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFilterFunction;
import com.alibaba.maxgraph.compiler.tree.source.SourceTreeNode;
import com.alibaba.maxgraph.compiler.utils.CompilerUtils;
import com.google.common.collect.Lists;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;

import java.util.List;

public class OrTreeNode extends UnaryTreeNode {
    private List<TreeNode> orTreeNodeList;

    public OrTreeNode(List<TreeNode> orTreeNodeList, TreeNode prev, GraphSchema schema) {
        super(prev, NodeType.FILTER, schema);
        this.orTreeNodeList = orTreeNodeList;
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        TreeNodeLabelManager labelManager = contextManager.getTreeNodeLabelManager();
        VertexIdManager vertexIdManager = contextManager.getVertexIdManager();
        Message.LogicalCompare.Builder logicalCompareBuilder = Message.LogicalCompare.newBuilder()
                .setCompare(Message.CompareType.OR_RELATION);
        List<TreeNode> otherTreeNodeList = Lists.newArrayList();
        boolean vertexFlag = getInputNode().getOutputValueType() instanceof VertexValueType;
        orTreeNodeList.forEach(v -> {
            if (v instanceof HasTreeNode && ((HasTreeNode) v).getInputNode() instanceof SourceTreeNode) {
                HasTreeNode hasTreeNode = HasTreeNode.class.cast(v);
                List<Message.LogicalCompare> logicalCompareList = Lists.newArrayList();
                for (HasContainer hasContainer : hasTreeNode.getHasContainerList()) {
                    logicalCompareList.add(
                            CompilerUtils.parseLogicalCompare(
                                    hasContainer,
                                    schema,
                                    labelManager.getLabelIndexList(),
                                    vertexFlag));
                }
                if (logicalCompareList.size() == 1) {
                    logicalCompareBuilder.addChildCompareList(logicalCompareList.get(0));
                } else {
                    Message.LogicalCompare andLogicalCompare = Message.LogicalCompare.newBuilder()
                            .setCompare(Message.CompareType.AND_RELATION)
                            .addAllChildCompareList(logicalCompareList)
                            .build();
                    logicalCompareBuilder.addChildCompareList(andLogicalCompare);
                }
            } else {
                otherTreeNodeList.add(v);
            }
        });
        if (otherTreeNodeList.isEmpty()) {
            ProcessorFilterFunction filterFunction = new ProcessorFilterFunction(QueryFlowOuterClass.OperatorType.HAS);
            filterFunction.getLogicalCompareList().addAll(Lists.newArrayList(logicalCompareBuilder.build()));

            return parseSingleUnaryVertex(vertexIdManager, labelManager, filterFunction, contextManager);
        } else {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public ValueType getOutputValueType() {
        return getInputNode().getOutputValueType();
    }
}

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
import com.alibaba.maxgraph.compiler.logical.LogicalEdge;
import com.alibaba.maxgraph.compiler.logical.edge.EdgeShuffleType;
import com.alibaba.maxgraph.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.compiler.tree.addition.ExtendPropLocalNode;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.compiler.tree.value.VertexValueType;
import com.alibaba.maxgraph.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFilterFunction;
import com.alibaba.maxgraph.compiler.utils.CompilerUtils;
import com.google.common.collect.Lists;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class HasTreeNode extends UnaryTreeNode implements ExtendPropLocalNode {
    private List<HasContainer> hasContainerList;

    public HasTreeNode(TreeNode input, List<HasContainer> hasContainerList, GraphSchema schema) {
        super(input, NodeType.FILTER, schema);

        this.hasContainerList = checkNotNull(hasContainerList, "has container can't be null");
    }

    public List<HasContainer> getHasContainerList() {
        return hasContainerList;
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        List<Message.LogicalCompare> logicalCompareList = Lists.newArrayList();
        hasContainerList.forEach(v -> logicalCompareList.add(CompilerUtils.parseLogicalCompare(v, schema, contextManager.getTreeNodeLabelManager().getLabelIndexList(), getInputNode().getOutputValueType() instanceof VertexValueType)));
        ProcessorFilterFunction filterFunction = new ProcessorFilterFunction(
                logicalCompareList.size() == 1 && logicalCompareList.get(0).getPropId() == 0 ? QueryFlowOuterClass.OperatorType.FILTER : QueryFlowOuterClass.OperatorType.HAS);
        filterFunction.getLogicalCompareList().addAll(logicalCompareList);

        boolean filterExchangeFlag = false;
        for (HasContainer hasContainer : this.hasContainerList) {
            if (SchemaUtils.checkPropExist(hasContainer.getKey(), schema)) {
                filterExchangeFlag = true;
                break;
            }
        }
        if (filterExchangeFlag) {
            return parseSingleUnaryVertex(contextManager.getVertexIdManager(), contextManager.getTreeNodeLabelManager(), filterFunction, contextManager);
        } else {
            return parseSingleUnaryVertex(contextManager.getVertexIdManager(), contextManager.getTreeNodeLabelManager(), filterFunction, contextManager, new LogicalEdge(EdgeShuffleType.FORWARD));
        }
    }

    @Override
    public boolean isPropLocalFlag() {
        return true;
    }

    @Override
    public ValueType getOutputValueType() {
        return getInputNode().getOutputValueType();
    }
}

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

import com.alibaba.maxgraph.QueryFlowOuterClass;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.tree.value.ListValueType;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.compiler.logical.LogicalEdge;
import com.alibaba.maxgraph.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.compiler.logical.LogicalUnaryVertex;
import com.alibaba.maxgraph.compiler.logical.LogicalVertex;
import com.alibaba.maxgraph.compiler.logical.VertexIdManager;
import com.alibaba.maxgraph.compiler.logical.edge.EdgeShuffleType;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.compiler.utils.TreeNodeUtils;

public class StoreTreeNode extends UnaryTreeNode {
    private String sideEffectKey;
    private TreeNode storeTreeNode;

    public StoreTreeNode(TreeNode input, GraphSchema schema, String sideEffectKey, TreeNode storeTreeNode) {
        super(input, NodeType.STORE, schema);
        this.sideEffectKey = sideEffectKey;
        this.storeTreeNode = storeTreeNode;
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        TreeNodeLabelManager treeNodeLabelManager = contextManager.getTreeNodeLabelManager();
        VertexIdManager vertexIdManager = contextManager.getVertexIdManager();
        LogicalVertex storeVertex;
        LogicalSubQueryPlan logicalSubQueryPlan;
        if (null == storeTreeNode) {
            ProcessorFunction processorFunction = new ProcessorFunction(QueryFlowOuterClass.OperatorType.FOLD_STORE);
            logicalSubQueryPlan = parseSingleUnaryVertex(vertexIdManager, treeNodeLabelManager, processorFunction, contextManager);
            storeVertex = logicalSubQueryPlan.getOutputVertex();
        } else {
            logicalSubQueryPlan = new LogicalSubQueryPlan(contextManager);
            LogicalVertex sourceVertex = getInputNode().getOutputVertex();
            logicalSubQueryPlan.addLogicalVertex(sourceVertex);

            LogicalSubQueryPlan storeQueryPlan = TreeNodeUtils.buildQueryPlanWithSource(storeTreeNode, treeNodeLabelManager, contextManager, vertexIdManager, sourceVertex);
            logicalSubQueryPlan.mergeLogicalQueryPlan(storeQueryPlan);

            LogicalVertex valueVertex = logicalSubQueryPlan.getOutputVertex();
            ProcessorFunction processorFunction = new ProcessorFunction(QueryFlowOuterClass.OperatorType.FOLD_STORE);
            LogicalVertex foldVertex = new LogicalUnaryVertex(vertexIdManager.getId(), processorFunction, false, valueVertex);
            logicalSubQueryPlan.addLogicalVertex(foldVertex);
            logicalSubQueryPlan.addLogicalEdge(valueVertex, foldVertex, new LogicalEdge(EdgeShuffleType.SHUFFLE_BY_CONST));
            storeVertex = logicalSubQueryPlan.getOutputVertex();
        }
        storeVertex.enableStoreFlag();
        setFinishVertex(getInputNode().getOutputVertex(), treeNodeLabelManager);
        contextManager.addStoreVertex(sideEffectKey, storeVertex);

        return logicalSubQueryPlan;
    }

    @Override
    public ValueType getOutputValueType() {
        if (null == storeTreeNode) {
            return new ListValueType(this.getInputNode().getOutputValueType());
        } else {
            return new ListValueType(storeTreeNode.getOutputValueType());
        }
    }
}

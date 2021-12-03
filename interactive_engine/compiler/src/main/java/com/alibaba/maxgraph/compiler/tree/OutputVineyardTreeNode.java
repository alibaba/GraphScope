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
import com.alibaba.maxgraph.compiler.logical.LogicalEdge;
import com.alibaba.maxgraph.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.compiler.logical.LogicalUnaryVertex;
import com.alibaba.maxgraph.compiler.logical.LogicalVertex;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.compiler.tree.value.ValueValueType;

public class OutputVineyardTreeNode extends UnaryTreeNode {
    private String graphName;

    public OutputVineyardTreeNode(TreeNode input, GraphSchema schema, String graphName) {
        super(input, NodeType.FLATMAP, schema);
        this.graphName = graphName;
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        TreeNode inputNode = this.getInputNode();
        LogicalVertex inputVertex = inputNode.getOutputVertex();
        LogicalSubQueryPlan logicalSubQueryPlan = new LogicalSubQueryPlan(contextManager);
        logicalSubQueryPlan.addLogicalVertex(inputVertex);

        LogicalVertex vineyardVertex = new LogicalUnaryVertex(contextManager.getVertexIdManager().getId(),
                new ProcessorFunction(QueryFlowOuterClass.OperatorType.OUTPUT_VINEYARD_VERTEX,
                        Message.Value.newBuilder().setStrValue(this.graphName)),
                inputVertex);
        logicalSubQueryPlan.addLogicalVertex(vineyardVertex);
        logicalSubQueryPlan.addLogicalEdge(inputVertex, vineyardVertex, LogicalEdge.shuffleByKey(0));

        LogicalVertex vineyardEdge = new LogicalUnaryVertex(contextManager.getVertexIdManager().getId(),
                new ProcessorFunction(QueryFlowOuterClass.OperatorType.OUTPUT_VINEYARD_EDGE,
                        Message.Value.newBuilder().setStrValue(this.graphName)),
                inputVertex);
        logicalSubQueryPlan.addLogicalVertex(vineyardEdge);
        logicalSubQueryPlan.addLogicalEdge(vineyardVertex, vineyardEdge, LogicalEdge.forwardEdge());

        LogicalVertex sumVertex = new LogicalUnaryVertex(contextManager.getVertexIdManager().getId(),
                new ProcessorFunction(QueryFlowOuterClass.OperatorType.SUM,
                        Message.Value.newBuilder().setValueType(Message.VariantType.VT_LONG)),
                vineyardEdge);
        logicalSubQueryPlan.addLogicalVertex(sumVertex);
        logicalSubQueryPlan.addLogicalEdge(vineyardEdge, sumVertex, LogicalEdge.shuffleConstant());

        setFinishVertex(sumVertex, contextManager.getTreeNodeLabelManager());
        addUsedLabelAndRequirement(sumVertex, contextManager.getTreeNodeLabelManager());

        return logicalSubQueryPlan;
    }

    @Override
    public ValueType getOutputValueType() {
        return new ValueValueType(Message.VariantType.VT_LONG);
    }
}

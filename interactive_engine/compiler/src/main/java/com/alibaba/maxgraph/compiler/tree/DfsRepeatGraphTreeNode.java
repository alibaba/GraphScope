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
import com.alibaba.maxgraph.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.compiler.tree.value.VertexValueType;
import com.alibaba.maxgraph.compiler.logical.LogicalQueryPlan;
import com.alibaba.maxgraph.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.compiler.logical.LogicalVertex;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorSourceFunction;
import com.alibaba.maxgraph.compiler.tree.source.SourceTreeNode;

public class DfsRepeatGraphTreeNode extends UnaryTreeNode {
    private SourceTreeNode sourceTreeNode;

    public DfsRepeatGraphTreeNode(
            TreeNode input, SourceTreeNode sourceTreeNode, GraphSchema schema) {
        super(input, NodeType.FLATMAP, schema);
        this.sourceTreeNode = sourceTreeNode;
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {

        LogicalQueryPlan sourceSubPlan = sourceTreeNode.buildLogicalQueryPlan(contextManager);
        LogicalVertex sourceVertex = sourceSubPlan.getOutputVertex();
        ProcessorSourceFunction processorSourceFunction =
                ProcessorSourceFunction.class.cast(sourceVertex.getProcessorFunction());
        Message.Value.Builder argument =
                Message.Value.newBuilder()
                        .mergeFrom(processorSourceFunction.getArgumentBuilder().build())
                        .setBoolValue(
                                sourceTreeNode.getOutputValueType() instanceof VertexValueType);
        ProcessorFunction processorFunction =
                new ProcessorFunction(QueryFlowOuterClass.OperatorType.DFS_REPEAT_GRAPH, argument);
        processorFunction
                .getLogicalCompareList()
                .addAll(processorSourceFunction.getLogicalCompareList());

        return parseSingleUnaryVertex(
                contextManager.getVertexIdManager(),
                contextManager.getTreeNodeLabelManager(),
                processorFunction,
                contextManager);
    }

    @Override
    public ValueType getOutputValueType() {
        return sourceTreeNode.getOutputValueType();
    }
}

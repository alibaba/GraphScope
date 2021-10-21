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
import com.alibaba.maxgraph.QueryFlowOuterClass.OperatorType;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.compiler.logical.VertexIdManager;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.compiler.tree.value.VertexValueType;

import static com.google.common.base.Preconditions.checkArgument;

public class PageRankTreeNode extends UnaryTreeNode {
    private final String outPropId;
    private final double alpha;
    private final int iteration;

    public PageRankTreeNode(TreeNode input, GraphSchema schema, String outPropId, double alpha, int iteration) {
        super(input, NodeType.FLATMAP, schema);
        this.outPropId = outPropId;
        this.alpha = alpha;
        this.iteration = iteration;
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        TreeNodeLabelManager labelManager = contextManager.getTreeNodeLabelManager();
        VertexIdManager vertexIdManager = contextManager.getVertexIdManager();
        ProcessorFunction processorFunction = new ProcessorFunction(OperatorType.PROGRAM_GRAPH_PAGERANK,
                createOperatorArgument());
        return parseSingleUnaryVertex(vertexIdManager, labelManager, processorFunction, contextManager);

    }

    private Message.Value.Builder createOperatorArgument() {
        Message.Value.Builder valueBuilder = Message.Value.newBuilder();
        Message.ProgramPageRankArg.Builder pageRankArgBuilder = Message.ProgramPageRankArg.newBuilder();
        pageRankArgBuilder.setPropertyPrId(schema.getPropertyId(outPropId));
        checkArgument(alpha > 0, "alpha must > 0 for PageRank");
        pageRankArgBuilder.setAlpha(alpha);

        checkArgument(iteration > 0, "iteration must > 0 for PageRank");
        pageRankArgBuilder.setLoopLimit(iteration);
        valueBuilder.setPayload(pageRankArgBuilder.build().toByteString());

        return valueBuilder;
    }

    @Override
    public ValueType getOutputValueType() {
        return new VertexValueType();
    }

}

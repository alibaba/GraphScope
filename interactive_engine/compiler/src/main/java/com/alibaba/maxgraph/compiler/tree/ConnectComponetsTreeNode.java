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
import com.alibaba.maxgraph.QueryFlowOuterClass.OperatorType;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.compiler.tree.value.VertexValueType;
import com.alibaba.maxgraph.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.compiler.optimizer.ContextManager;

import static com.google.common.base.Preconditions.checkArgument;

public class ConnectComponetsTreeNode extends UnaryTreeNode {
    private final String outPropId;
    private final int iteration;

    public ConnectComponetsTreeNode(
            TreeNode input, GraphSchema schema, String outPropId, int iteration) {
        super(input, NodeType.FLATMAP, schema);
        this.outPropId = outPropId;
        this.iteration = iteration;
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        ProcessorFunction processorFunction =
                new ProcessorFunction(OperatorType.PROGRAM_GRAPH_CC, createOperatorArgument());
        return parseSingleUnaryVertex(
                contextManager.getVertexIdManager(),
                contextManager.getTreeNodeLabelManager(),
                processorFunction,
                contextManager);
    }

    private Message.Value.Builder createOperatorArgument() {
        Message.Value.Builder valueBuilder = Message.Value.newBuilder();
        Message.ProgramCCArg.Builder ccArgumentBuilder = Message.ProgramCCArg.newBuilder();

        try {
            ccArgumentBuilder.setPropertyCcId(schema.getPropertyId(outPropId));
        } catch (Exception e) {
            throw new RuntimeException("cant get property by name=>" + outPropId);
        }
        checkArgument(iteration > 0, "iteration must > 0 for ConnectComponets");
        ccArgumentBuilder.setLoopLimit(iteration);
        valueBuilder.setPayload(ccArgumentBuilder.build().toByteString());
        return valueBuilder;
    }

    @Override
    public ValueType getOutputValueType() {
        return new VertexValueType();
    }
}

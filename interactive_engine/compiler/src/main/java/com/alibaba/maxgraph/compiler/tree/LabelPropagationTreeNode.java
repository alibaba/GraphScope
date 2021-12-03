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
import com.alibaba.maxgraph.Message.EdgeDirection;
import com.alibaba.maxgraph.QueryFlowOuterClass.OperatorType;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.compiler.tree.value.VertexValueType;
import com.alibaba.maxgraph.tinkerpop.steps.LabelPropagationStep;

import static com.google.common.base.Preconditions.checkArgument;

public class LabelPropagationTreeNode extends UnaryTreeNode {
    private final LabelPropagationStep step;

    public LabelPropagationTreeNode(TreeNode input, GraphSchema schema, LabelPropagationStep step) {
        super(input, NodeType.FLATMAP, schema);
        this.step = step;
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        ProcessorFunction processorFunction =
                new ProcessorFunction(OperatorType.PROGRAM_GRAPH_LPA, createOperatorArgument());
        return parseSingleUnaryVertex(
                contextManager.getVertexIdManager(),
                contextManager.getTreeNodeLabelManager(),
                processorFunction,
                contextManager);
    }

    private Message.Value.Builder createOperatorArgument() {
        Message.Value.Builder valueBuilder = Message.Value.newBuilder();
        Message.ProgramLPAArg.Builder argBuilder = Message.ProgramLPAArg.newBuilder();
        if ("both".equalsIgnoreCase(step.direction)) {
            argBuilder.setDirection(EdgeDirection.DIR_NONE);
        } else if ("in".equalsIgnoreCase(step.direction)) {
            argBuilder.setDirection(EdgeDirection.DIR_IN);
        } else if ("out".equalsIgnoreCase(step.direction)) {
            argBuilder.setDirection(EdgeDirection.DIR_OUT);
        } else {
            checkArgument(false, "direction must be in/out/both for lpa");
        }
        for (String edgeLabel : step.edgeLabels) {
            argBuilder.addEdgeLabels(schema.getElement(edgeLabel).getLabelId());
        }
        argBuilder.setSeedLabel(schema.getPropertyId(step.seedLabel));
        argBuilder.setTargetLabel(schema.getPropertyId(step.targetLabel));

        checkArgument(step.iteration > 0, "iteration must > 0 for LPA");
        argBuilder.setIteration(step.iteration);

        valueBuilder.setPayload(argBuilder.build().toByteString());
        return valueBuilder;
    }

    @Override
    public ValueType getOutputValueType() {
        return new VertexValueType();
    }
}

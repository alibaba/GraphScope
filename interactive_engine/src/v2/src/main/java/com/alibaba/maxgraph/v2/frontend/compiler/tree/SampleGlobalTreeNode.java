package com.alibaba.maxgraph.v2.frontend.compiler.tree;


import com.alibaba.maxgraph.proto.v2.OperatorType;
import com.alibaba.maxgraph.proto.v2.Value;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.VertexIdManager;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.v2.frontend.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.addition.AbstractUseKeyNode;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.ValueType;

import static com.google.common.base.Preconditions.checkArgument;

public class SampleGlobalTreeNode extends AbstractUseKeyNode {
    private int amountToSample;

    public SampleGlobalTreeNode(TreeNode input, GraphSchema schema, int amountToSample) {
        super(input, NodeType.AGGREGATE, schema);
        this.amountToSample = amountToSample;
        checkArgument(this.amountToSample == 1, "only support amount to sample = 1");
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        TreeNodeLabelManager labelManager = contextManager.getTreeNodeLabelManager();
        VertexIdManager vertexIdManager = contextManager.getVertexIdManager();
        Value.Builder argumentBuilder = createArgumentBuilder();
        argumentBuilder.setIntValue(amountToSample);
        ProcessorFunction processorFunction = new ProcessorFunction(OperatorType.SAMPLE,
                argumentBuilder);
        return parseSingleUnaryVertex(vertexIdManager, labelManager, processorFunction, contextManager);
    }

    @Override
    public ValueType getOutputValueType() {
        return getInputNode().getOutputValueType();
    }
}

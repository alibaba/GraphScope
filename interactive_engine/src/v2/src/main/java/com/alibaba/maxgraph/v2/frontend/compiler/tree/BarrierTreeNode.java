package com.alibaba.maxgraph.v2.frontend.compiler.tree;

import com.alibaba.maxgraph.proto.v2.OperatorType;
import com.alibaba.maxgraph.proto.v2.Value;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.v2.frontend.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.v2.frontend.compiler.utils.ReflectionUtils;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;

public class BarrierTreeNode extends UnaryTreeNode {
    private NoOpBarrierStep barrierStep;

    public BarrierTreeNode(TreeNode input, NoOpBarrierStep barrierStep, GraphSchema schema) {
        super(input, NodeType.BARRIER, schema);
        this.barrierStep = barrierStep;
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        Value.Builder argumentBuilder = Value.newBuilder();
        int maxBarrierSize = ReflectionUtils.getFieldValue(NoOpBarrierStep.class, barrierStep, "maxBarrierSize");
        argumentBuilder.setIntValue(maxBarrierSize);
        ProcessorFunction processorFunction = new ProcessorFunction(OperatorType.BARRIER, argumentBuilder);
        return parseSingleUnaryVertex(contextManager.getVertexIdManager(),
                contextManager.getTreeNodeLabelManager(),
                processorFunction,
                contextManager);
    }

    @Override
    public ValueType getOutputValueType() {
        return getInputNode().getOutputValueType();
    }
}

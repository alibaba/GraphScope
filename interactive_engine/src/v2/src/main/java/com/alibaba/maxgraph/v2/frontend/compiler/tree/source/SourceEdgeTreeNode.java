package com.alibaba.maxgraph.v2.frontend.compiler.tree.source;

import com.alibaba.maxgraph.proto.v2.OperatorType;
import com.alibaba.maxgraph.proto.v2.Value;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.function.ProcessorSourceFunction;
import com.alibaba.maxgraph.v2.frontend.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.addition.CountFlagNode;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.EdgeValueType;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.ValueType;

public class SourceEdgeTreeNode extends SourceTreeNode implements CountFlagNode {
    private boolean countFlag = false;

    public SourceEdgeTreeNode(GraphSchema schema) {
        this(null, schema);
    }

    public SourceEdgeTreeNode(Object[] ids, GraphSchema schema) {
        super(ids, schema);
        setPropLocalFlag(true);
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        LogicalSubQueryPlan logicalQueryPlan = new LogicalSubQueryPlan(contextManager);
        logicalQueryPlan.setDelegateSourceFlag(false);

        Value.Builder argumentBuilder = Value.newBuilder()
                .setBoolValue(true);
        processLabelArgument(argumentBuilder, false);
        processIdArgument(argumentBuilder, false);

        ProcessorSourceFunction processorSourceFunction = new ProcessorSourceFunction(getCountOperator(OperatorType.E), argumentBuilder, rangeLimit);
        return processSourceVertex(contextManager.getVertexIdManager(),
                contextManager.getTreeNodeLabelManager(),
                logicalQueryPlan,
                processorSourceFunction);
    }

    @Override
    public ValueType getOutputValueType() {
        return getCountOutputType(new EdgeValueType());
    }

    @Override
    public boolean checkCountOptimize() {
        return false;
    }

    @Override
    public void enableCountFlag() {
        this.countFlag = true;
    }

    @Override
    public boolean isCountFlag() {
        return countFlag;
    }
}

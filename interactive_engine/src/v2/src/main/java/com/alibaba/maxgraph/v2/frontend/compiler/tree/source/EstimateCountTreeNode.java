package com.alibaba.maxgraph.v2.frontend.compiler.tree.source;

import com.alibaba.maxgraph.proto.v2.OperatorType;
import com.alibaba.maxgraph.proto.v2.Value;
import com.alibaba.maxgraph.proto.v2.VariantType;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.function.ProcessorSourceFunction;
import com.alibaba.maxgraph.v2.frontend.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.ValueValueType;

import java.util.Set;
import java.util.stream.Collectors;

public class EstimateCountTreeNode extends SourceTreeNode {
    private boolean vertexFlag;
    private Set<String> labelList;

    public EstimateCountTreeNode(boolean vertexFlag, Set<String> labelList, GraphSchema schema) {
        super(null, schema);
        this.vertexFlag = vertexFlag;
        this.labelList = labelList;
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        LogicalSubQueryPlan logicalQueryPlan = new LogicalSubQueryPlan(contextManager);
        logicalQueryPlan.setDelegateSourceFlag(false);

        Value.Builder argumentBuilder = super.createArgumentBuilder().setBoolValue(this.vertexFlag);
        if (null != labelList) {
            argumentBuilder.addAllIntValueList(labelList.stream()
                    .map(v -> schema.getSchemaElement(v).getLabelId())
                    .collect(Collectors.toList()));
        }
        ProcessorSourceFunction processorSourceFunction = new ProcessorSourceFunction(OperatorType.ESTIMATE_COUNT, argumentBuilder, null);
        return processSourceVertex(contextManager.getVertexIdManager(), contextManager.getTreeNodeLabelManager(), logicalQueryPlan, processorSourceFunction);
    }

    @Override
    public ValueType getOutputValueType() {
        return new ValueValueType(VariantType.VT_LONG);
    }
}

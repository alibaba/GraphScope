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
package com.alibaba.maxgraph.compiler.tree.source;

import com.alibaba.maxgraph.Message;
import com.alibaba.maxgraph.QueryFlowOuterClass;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorSourceFunction;
import com.alibaba.maxgraph.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.compiler.tree.value.ValueValueType;

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

        Message.Value.Builder argumentBuilder =
                super.createArgumentBuilder().setBoolValue(this.vertexFlag);
        if (null != labelList) {
            argumentBuilder.addAllIntValueList(
                    labelList.stream()
                            .map(v -> schema.getElement(v).getLabelId())
                            .collect(Collectors.toList()));
        }
        ProcessorSourceFunction processorSourceFunction =
                new ProcessorSourceFunction(
                        QueryFlowOuterClass.OperatorType.ESTIMATE_COUNT, argumentBuilder, null);
        return processSourceVertex(
                contextManager.getVertexIdManager(),
                contextManager.getTreeNodeLabelManager(),
                logicalQueryPlan,
                processorSourceFunction);
    }

    @Override
    public ValueType getOutputValueType() {
        return new ValueValueType(Message.VariantType.VT_LONG);
    }
}

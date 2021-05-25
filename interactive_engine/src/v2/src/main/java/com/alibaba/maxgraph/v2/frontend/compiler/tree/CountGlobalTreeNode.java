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
package com.alibaba.maxgraph.v2.frontend.compiler.tree;

import com.alibaba.maxgraph.proto.v2.CountArgumentProto;
import com.alibaba.maxgraph.proto.v2.OperatorType;
import com.alibaba.maxgraph.proto.v2.Value;
import com.alibaba.maxgraph.proto.v2.VariantType;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalEdge;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalUnaryVertex;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalVertex;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.v2.frontend.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.addition.AbstractUseKeyNode;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.addition.JoinZeroNode;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.ValueValueType;

public class CountGlobalTreeNode extends AbstractUseKeyNode implements JoinZeroNode {
    private boolean joinZeroFlag = true;

    /**
     * Convert count to LimitCount operator
     */
    private boolean limitFlag = false;
    private long limitCount = 0;

    public CountGlobalTreeNode(TreeNode input, GraphSchema schema) {
        super(input, NodeType.AGGREGATE, schema);
    }

    public void setLimitCount(long count) {
        this.limitCount = count;
        this.limitFlag = true;
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        LogicalSubQueryPlan logicalSubQueryPlan = new LogicalSubQueryPlan(contextManager);
        LogicalVertex sourceVertex = getInputNode().getOutputVertex();
        logicalSubQueryPlan.addLogicalVertex(sourceVertex);

        OperatorType operatorType = getUseKeyOperator(OperatorType.COUNT);
        CountArgumentProto countArgument = CountArgumentProto.newBuilder()
                .setLimitFlag(limitFlag)
                .setLimitCount(limitCount)
                .build();
        ProcessorFunction countFunction = new ProcessorFunction(
                operatorType,
                Value.newBuilder()
                        .setPayload(countArgument.toByteString()));
        LogicalUnaryVertex countVertex = new LogicalUnaryVertex(
                contextManager.getVertexIdManager().getId(),
                countFunction,
                false,
                sourceVertex);
        logicalSubQueryPlan.addLogicalVertex(countVertex);
        logicalSubQueryPlan.addLogicalEdge(sourceVertex, countVertex, new LogicalEdge());

        LogicalVertex outputVertex = processJoinZeroVertex(
                contextManager.getVertexIdManager(),
                logicalSubQueryPlan,
                countVertex,
                isJoinZeroFlag());
        if (outputVertex == countVertex && operatorType == OperatorType.COUNT && !limitFlag) {
            countFunction.resetOperatorType(OperatorType.COMBINER_COUNT);
            outputVertex = new LogicalUnaryVertex(contextManager.getVertexIdManager().getId(),
                    new ProcessorFunction(OperatorType.SUM, Value.newBuilder().setValueType(VariantType.VT_LONG)),
                    countVertex);
            logicalSubQueryPlan.addLogicalVertex(outputVertex);
            logicalSubQueryPlan.addLogicalEdge(countVertex, outputVertex, new LogicalEdge());

        }

        setFinishVertex(outputVertex, contextManager.getTreeNodeLabelManager());
        return logicalSubQueryPlan;
    }

    @Override
    public ValueType getOutputValueType() {
        return new ValueValueType(VariantType.VT_LONG);
    }

    @Override
    public void disableJoinZero() {
        this.joinZeroFlag = false;
    }

    @Override
    public void enableJoinZero() {
        this.joinZeroFlag = true;
    }

    @Override
    public boolean isJoinZeroFlag() {
        return this.joinZeroFlag;
    }
}

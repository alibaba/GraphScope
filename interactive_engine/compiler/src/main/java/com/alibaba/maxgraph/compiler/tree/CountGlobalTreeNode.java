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
import com.alibaba.maxgraph.compiler.logical.LogicalEdge;
import com.alibaba.maxgraph.compiler.logical.LogicalUnaryVertex;
import com.alibaba.maxgraph.compiler.logical.LogicalVertex;
import com.alibaba.maxgraph.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.compiler.tree.addition.AbstractUseKeyNode;
import com.alibaba.maxgraph.compiler.tree.addition.JoinZeroNode;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.compiler.tree.value.ValueValueType;
import com.alibaba.maxgraph.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFunction;

public class CountGlobalTreeNode extends AbstractUseKeyNode implements JoinZeroNode {
    private boolean joinZeroFlag = true;

    /** Convert count to LimitCount operator */
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

        QueryFlowOuterClass.OperatorType operatorType =
                getUseKeyOperator(QueryFlowOuterClass.OperatorType.COUNT);
        QueryFlowOuterClass.CountArgumentProto countArgument =
                QueryFlowOuterClass.CountArgumentProto.newBuilder()
                        .setLimitFlag(limitFlag)
                        .setLimitCount(limitCount)
                        .build();
        ProcessorFunction countFunction =
                new ProcessorFunction(
                        operatorType,
                        Message.Value.newBuilder().setPayload(countArgument.toByteString()));
        LogicalUnaryVertex countVertex =
                new LogicalUnaryVertex(
                        contextManager.getVertexIdManager().getId(),
                        countFunction,
                        false,
                        sourceVertex);
        logicalSubQueryPlan.addLogicalVertex(countVertex);
        logicalSubQueryPlan.addLogicalEdge(sourceVertex, countVertex, new LogicalEdge());

        LogicalVertex outputVertex =
                processJoinZeroVertex(
                        contextManager.getVertexIdManager(),
                        logicalSubQueryPlan,
                        countVertex,
                        isJoinZeroFlag());

        setFinishVertex(outputVertex, contextManager.getTreeNodeLabelManager());
        return logicalSubQueryPlan;
    }

    @Override
    public ValueType getOutputValueType() {
        return new ValueValueType(Message.VariantType.VT_LONG);
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

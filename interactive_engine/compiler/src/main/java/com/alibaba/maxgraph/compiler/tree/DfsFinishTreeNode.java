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
import com.alibaba.maxgraph.compiler.logical.edge.EdgeShuffleType;
import com.alibaba.maxgraph.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.compiler.tree.value.DfsCmdValueType;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.compiler.logical.LogicalBinaryVertex;
import com.alibaba.maxgraph.compiler.logical.LogicalEdge;
import com.alibaba.maxgraph.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.compiler.logical.LogicalUnaryVertex;
import com.alibaba.maxgraph.compiler.logical.LogicalVertex;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFunction;

public class DfsFinishTreeNode extends UnaryTreeNode {
    private LogicalVertex repeatSourceVertex;
    private long maxRecordCount;

    public DfsFinishTreeNode(TreeNode input, GraphSchema schema, long maxRecordCount) {
        super(input, NodeType.BFSEND, schema);
        this.maxRecordCount = maxRecordCount;
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        LogicalSubQueryPlan logicalSubQueryPlan = new LogicalSubQueryPlan(contextManager);
        LogicalVertex delegateSourceVertex = getInputNode().getOutputVertex();
        logicalSubQueryPlan.addLogicalVertex(delegateSourceVertex);

        LogicalVertex countVertex =
                new LogicalUnaryVertex(
                        contextManager.getVertexIdManager().getId(),
                        new ProcessorFunction(QueryFlowOuterClass.OperatorType.COUNT),
                        false,
                        delegateSourceVertex);
        logicalSubQueryPlan.addLogicalVertex(countVertex);
        logicalSubQueryPlan.addLogicalEdge(delegateSourceVertex, countVertex, new LogicalEdge());

        Message.Value.Builder argument = Message.Value.newBuilder().setLongValue(maxRecordCount);
        LogicalVertex dfsFinishVertex =
                new LogicalBinaryVertex(
                        contextManager.getVertexIdManager().getId(),
                        new ProcessorFunction(
                                QueryFlowOuterClass.OperatorType.DFS_FINISH_JOIN, argument),
                        true,
                        repeatSourceVertex,
                        countVertex);
        logicalSubQueryPlan.addLogicalVertex(dfsFinishVertex);
        logicalSubQueryPlan.addLogicalEdge(
                countVertex, dfsFinishVertex, new LogicalEdge(EdgeShuffleType.SHUFFLE_BY_CONST));

        return logicalSubQueryPlan;
    }

    @Override
    public ValueType getOutputValueType() {
        return new DfsCmdValueType();
    }

    public void setRepeatSourceVertex(LogicalVertex repeatSourceVertex) {
        this.repeatSourceVertex = repeatSourceVertex;
    }
}

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
import com.alibaba.maxgraph.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.compiler.tree.value.VertexValueType;

public class AllPathTreeNode extends UnaryTreeNode {
    public final Long sid;
    public final Long tid;
    public final int khop;
    public final String outPropId;

    public AllPathTreeNode(
            TreeNode input, GraphSchema schema, Long sid, Long tid, int khop, String outProhId) {
        super(input, NodeType.FLATMAP, schema);
        this.sid = sid;
        this.tid = tid;
        this.khop = khop;
        this.outPropId = outProhId;
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        ProcessorFunction processorFunction =
                new ProcessorFunction(
                        QueryFlowOuterClass.OperatorType.PROGRAM_GRAPH_ALLPATH,
                        createOperatorArgument());
        return parseSingleUnaryVertex(
                contextManager.getVertexIdManager(),
                contextManager.getTreeNodeLabelManager(),
                processorFunction,
                contextManager);
    }

    private Message.Value.Builder createOperatorArgument() {
        Message.Value.Builder valueBuilder = Message.Value.newBuilder();
        Message.ProgramAllPathArg.Builder allPathArgBuilder =
                Message.ProgramAllPathArg.newBuilder();
        allPathArgBuilder.setSid(sid);
        allPathArgBuilder.setTid(tid);
        allPathArgBuilder.setLoopLimit(khop);
        allPathArgBuilder.setPropertyPathId(schema.getPropertyId(outPropId));
        allPathArgBuilder.setPropertyIdId(schema.getPropertyId("sid"));
        return valueBuilder;
    }

    @Override
    public ValueType getOutputValueType() {
        return new VertexValueType();
    }
}

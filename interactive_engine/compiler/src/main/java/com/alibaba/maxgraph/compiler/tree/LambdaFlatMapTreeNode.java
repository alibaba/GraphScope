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
package com.alibaba.maxgraph.compiler.tree;

import com.alibaba.maxgraph.Message;
import com.alibaba.maxgraph.QueryFlowOuterClass;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;

public class LambdaFlatMapTreeNode extends UnaryTreeNode {
    private String lambdaIndex;

    public LambdaFlatMapTreeNode(TreeNode input, GraphSchema schema, String lambdaIndex) {
        super(input, NodeType.LAMBDA_FLATMAP, schema);
        this.lambdaIndex = lambdaIndex;
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        Message.Value.Builder argumentBuilder = Message.Value.newBuilder()
                .setStrValue(this.lambdaIndex);

        ProcessorFunction processorFunction = new ProcessorFunction(QueryFlowOuterClass.OperatorType.LAMBDA_FLATMAP, argumentBuilder);
        return parseSingleUnaryVertex(contextManager.getVertexIdManager(), contextManager.getTreeNodeLabelManager(), processorFunction, contextManager);
    }

    @Override
    public ValueType getOutputValueType() {
        return getInputNode().getOutputValueType();
    }
}

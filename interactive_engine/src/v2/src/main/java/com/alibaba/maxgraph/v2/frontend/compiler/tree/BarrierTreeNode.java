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

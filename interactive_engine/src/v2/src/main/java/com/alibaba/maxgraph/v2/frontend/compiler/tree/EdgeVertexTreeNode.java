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
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.v2.frontend.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.VertexValueType;
import org.apache.tinkerpop.gremlin.structure.Direction;

public class EdgeVertexTreeNode extends UnaryTreeNode {
    private Direction direction;

    public EdgeVertexTreeNode(TreeNode input, Direction direction, GraphSchema schema) {
        super(input, direction == Direction.BOTH ? NodeType.FLATMAP : NodeType.MAP, schema);
        this.direction = direction;
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(
            ContextManager contextManager) {
        ProcessorFunction processorFunction = new ProcessorFunction(OperatorType.valueOf(direction.name() + "_V"), rangeLimit);

        return parseSingleUnaryVertex(
                contextManager.getVertexIdManager(),
                contextManager.getTreeNodeLabelManager(),
                processorFunction,
                contextManager);
    }

    @Override
    public ValueType getOutputValueType() {
        return new VertexValueType();
    }

    public Direction getDirection() {
        return direction;
    }
}

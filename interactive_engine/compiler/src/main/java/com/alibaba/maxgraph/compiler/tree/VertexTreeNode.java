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
import com.alibaba.maxgraph.compiler.logical.LogicalEdge;
import com.alibaba.maxgraph.compiler.logical.edge.EdgeShuffleType;
import com.alibaba.maxgraph.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.compiler.tree.value.VertexValueType;
import com.alibaba.maxgraph.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.compiler.logical.VertexIdManager;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.compiler.tree.addition.CountFlagNode;
import com.alibaba.maxgraph.compiler.tree.addition.SampleNode;
import org.apache.tinkerpop.gremlin.structure.Direction;

public class VertexTreeNode extends UnaryTreeNode implements CountFlagNode, SampleNode {
    private Direction direction;
    private String[] edgeLabels;
    private boolean countFlag = false;

    public VertexTreeNode(TreeNode input, Direction direction, String[] edgeLabels, GraphSchema schema) {
        super(input, NodeType.FLATMAP, schema);
        this.direction = direction;
        this.edgeLabels = edgeLabels;
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        TreeNodeLabelManager labelManager = contextManager.getTreeNodeLabelManager();
        VertexIdManager vertexIdManager = contextManager.getVertexIdManager();
        Message.Value.Builder argumentBuilder = createArgumentBuilder();
        if (null != edgeLabels) {
            for (String edgeLabel : edgeLabels) {
                argumentBuilder.addIntValueList(schema.getElement(edgeLabel).getLabelId());
            }
        }
        ProcessorFunction processorFunction = new ProcessorFunction(getCountOperator(QueryFlowOuterClass.OperatorType.valueOf(direction.name())), argumentBuilder, rangeLimit);
        if (direction == Direction.OUT && getInputNode().isPropLocalFlag()) {
            return parseSingleUnaryVertex(vertexIdManager, labelManager, processorFunction, contextManager, new LogicalEdge(EdgeShuffleType.FORWARD));
        } else {
            return parseSingleUnaryVertex(vertexIdManager, labelManager, processorFunction, contextManager);
        }
    }

    @Override
    public ValueType getOutputValueType() {
        return getCountOutputType(new VertexValueType());
    }

    @Override
    public boolean checkCountOptimize() {
        return beforeRequirementList.isEmpty() && afterRequirementList.isEmpty() && null == rangeLimit;
    }

    @Override
    public void enableCountFlag() {
        this.countFlag = true;
        this.nodeType = NodeType.MAP;
    }

    @Override
    public boolean isCountFlag() {
        return countFlag;
    }

    public Direction getDirection() {
        return this.direction;
    }

    @Override
    public void setSample(int amountToSample, String probabilityProperty) {
        throw new UnsupportedOperationException("Only support edge yet");
    }

    public String[] getEdgeLabels() {
        return this.edgeLabels;
    }

    public GraphSchema getSchema() {
        return this.schema;
    }
}

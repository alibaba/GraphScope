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
import com.alibaba.maxgraph.compiler.logical.edge.EdgeShuffleType;
import com.alibaba.maxgraph.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.common.util.SchemaUtils;
import com.alibaba.maxgraph.compiler.tree.value.EdgeValueType;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.compiler.tree.addition.CountFlagNode;
import com.alibaba.maxgraph.compiler.tree.addition.SampleNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.structure.Direction;

public class EdgeTreeNode extends UnaryTreeNode implements CountFlagNode, SampleNode {
    private Direction direction;
    private String[] edgeLabels;
    private boolean countFlag = false;
    // If true, the edge is no need to fetch properties
    private boolean fetchPropFlag = false;
    private int amountToSample = -1;
    private String probabilityProperty = null;

    public EdgeTreeNode(
            TreeNode input, Direction direction, String[] edgeLabels, GraphSchema schema) {
        super(input, NodeType.FLATMAP, schema);
        this.direction = direction;
        this.edgeLabels = edgeLabels;
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        Message.Value.Builder argumentBuilder = createArgumentBuilder();
        if (null != edgeLabels) {
            for (String edgeLabel : edgeLabels) {
                argumentBuilder.addIntValueList(schema.getElement(edgeLabel).getLabelId());
            }
        }
        argumentBuilder.setBoolValue(fetchPropFlag);
        if (null != rangeLimit) {
            argumentBuilder.setBoolFlag(globalRangeFlag);
        }
        if (amountToSample > 0 && StringUtils.isNotEmpty(probabilityProperty)) {
            argumentBuilder
                    .setIntValue(SchemaUtils.getPropId(probabilityProperty, schema))
                    .setLongValue(amountToSample);
        }
        ProcessorFunction processorFunction =
                new ProcessorFunction(
                        isCountFlag()
                                ? QueryFlowOuterClass.OperatorType.valueOf(
                                        direction.name() + "_COUNT")
                                : QueryFlowOuterClass.OperatorType.valueOf(direction.name() + "_E"),
                        argumentBuilder,
                        rangeLimit);
        if (direction == Direction.OUT && getInputNode().isPropLocalFlag()) {
            return parseSingleUnaryVertex(
                    contextManager.getVertexIdManager(),
                    contextManager.getTreeNodeLabelManager(),
                    processorFunction,
                    contextManager,
                    new LogicalEdge(EdgeShuffleType.FORWARD));
        } else {
            return parseSingleUnaryVertex(
                    contextManager.getVertexIdManager(),
                    contextManager.getTreeNodeLabelManager(),
                    processorFunction,
                    contextManager);
        }
    }

    @Override
    public ValueType getOutputValueType() {
        return getCountOutputType(new EdgeValueType());
    }

    @Override
    public boolean checkCountOptimize() {
        return beforeRequirementList.isEmpty()
                && afterRequirementList.isEmpty()
                && null == rangeLimit;
    }

    @Override
    public void enableCountFlag() {
        this.countFlag = true;
        this.nodeType = NodeType.MAP;
    }

    public void setFetchPropFlag(boolean fetchPropFlag) {
        this.fetchPropFlag = fetchPropFlag;
    }

    public boolean isFetchPropFlag() {
        return this.fetchPropFlag;
    }

    @Override
    public boolean isPropLocalFlag() {
        return true;
    }

    @Override
    public boolean isCountFlag() {
        return this.countFlag;
    }

    @Override
    public void setSample(int amountToSample, String probabilityProperty) {
        this.amountToSample = amountToSample;
        this.probabilityProperty = probabilityProperty;
    }

    public Direction getDirection() {
        return direction;
    }

    public String[] getEdgeLabels() {
        return edgeLabels;
    }
}

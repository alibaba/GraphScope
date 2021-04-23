package com.alibaba.maxgraph.v2.frontend.compiler.tree;

import com.alibaba.maxgraph.proto.v2.OperatorType;
import com.alibaba.maxgraph.proto.v2.Value;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalEdge;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.edge.EdgeShuffleType;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.v2.frontend.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.addition.CountFlagNode;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.addition.SampleNode;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.EdgeValueType;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.v2.frontend.compiler.utils.SchemaUtils;
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

    public EdgeTreeNode(TreeNode input, Direction direction, String[] edgeLabels, GraphSchema schema) {
        super(input, NodeType.FLATMAP, schema);
        this.direction = direction;
        this.edgeLabels = edgeLabels;
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        Value.Builder argumentBuilder = createArgumentBuilder();
        if (null != edgeLabels) {
            for (String edgeLabel : edgeLabels) {
                argumentBuilder.addIntValueList(schema.getSchemaElement(edgeLabel).getLabelId());
            }
        }
        argumentBuilder.setBoolValue(fetchPropFlag);
        if (null != rangeLimit) {
            argumentBuilder.setBoolFlag(globalRangeFlag);
        }
        if (amountToSample > 0 && StringUtils.isNotEmpty(probabilityProperty)) {
            argumentBuilder.setIntValue(SchemaUtils.getPropId(probabilityProperty, schema))
                    .setLongValue(amountToSample);
        }
        ProcessorFunction processorFunction = new ProcessorFunction(
                isCountFlag() ? OperatorType.valueOf(direction.name() + "_COUNT") :
                        OperatorType.valueOf(direction.name() + "_E"), argumentBuilder, rangeLimit);
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
        return beforeRequirementList.isEmpty() && afterRequirementList.isEmpty() && null == rangeLimit;
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

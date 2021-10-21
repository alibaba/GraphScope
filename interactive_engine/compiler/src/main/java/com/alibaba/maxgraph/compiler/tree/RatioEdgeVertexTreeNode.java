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
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.compiler.tree.value.VertexValueType;
import com.alibaba.maxgraph.compiler.logical.LogicalBinaryVertex;
import com.alibaba.maxgraph.compiler.logical.LogicalEdge;
import com.alibaba.maxgraph.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.compiler.logical.LogicalUnaryVertex;
import com.alibaba.maxgraph.compiler.logical.LogicalVertex;
import com.alibaba.maxgraph.compiler.logical.VertexIdManager;
import com.alibaba.maxgraph.compiler.logical.edge.EdgeShuffleType;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.compiler.utils.CompilerUtils;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.Direction;

public class RatioEdgeVertexTreeNode extends UnaryTreeNode {
    private Direction direction;
    private P predicate;

    public RatioEdgeVertexTreeNode(TreeNode input, GraphSchema schema, Direction direction, P predicate) {
        super(input, NodeType.FLATMAP, schema);
        this.direction = direction;
        this.predicate = predicate;
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        TreeNodeLabelManager labelManager = contextManager.getTreeNodeLabelManager();
        VertexIdManager vertexIdManager = contextManager.getVertexIdManager();
        TreeNode inputNode = this.getInputNode();
        LogicalVertex inputVertex = inputNode.getOutputVertex();

        TreeNode sourceNode = UnaryTreeNode.class.cast(inputNode).getInputNode();
        LogicalVertex sourceVertex = sourceNode.getOutputVertex();

        LogicalSubQueryPlan logicalSubQueryPlan = new LogicalSubQueryPlan(contextManager);
        logicalSubQueryPlan.addLogicalVertex(inputVertex);
        logicalSubQueryPlan.addLogicalVertex(sourceVertex);

        ProcessorFunction countFunction = new ProcessorFunction(QueryFlowOuterClass.OperatorType.COUNT);
        LogicalUnaryVertex countVertex = new LogicalUnaryVertex(vertexIdManager.getId(), countFunction, false, sourceVertex);
        logicalSubQueryPlan.addLogicalVertex(countVertex);
        LogicalEdge countEdge = new LogicalEdge(EdgeShuffleType.SHUFFLE_BY_CONST);
        countEdge.setStreamIndex(1);
        logicalSubQueryPlan.addLogicalEdge(sourceVertex, countVertex, countEdge);

        ProcessorFunction enterKeyFunction = new ProcessorFunction(QueryFlowOuterClass.OperatorType.ENTER_KEY,
                Message.Value.newBuilder().setPayload(QueryFlowOuterClass.EnterKeyArgumentProto.newBuilder()
                        .setEnterKeyType(QueryFlowOuterClass.EnterKeyTypeProto.KEY_SELF)
                        .setUniqFlag(true)
                        .build().toByteString()));
        LogicalVertex enterKeyVertex = new LogicalUnaryVertex(vertexIdManager.getId(), enterKeyFunction, false, sourceVertex);
        logicalSubQueryPlan.addLogicalVertex(enterKeyVertex);
        logicalSubQueryPlan.addLogicalEdge(sourceVertex, enterKeyVertex, new LogicalEdge(EdgeShuffleType.FORWARD));

        ProcessorFunction valueFunction = new ProcessorFunction(QueryFlowOuterClass.OperatorType.valueOf(direction.name() + "_V"));
        LogicalUnaryVertex valueVertex = new LogicalUnaryVertex(vertexIdManager.getId(), valueFunction, false, enterKeyVertex);
        logicalSubQueryPlan.addLogicalVertex(valueVertex);
        LogicalEdge valueEdge = new LogicalEdge(EdgeShuffleType.SHUFFLE_BY_KEY);
        logicalSubQueryPlan.addLogicalEdge(enterKeyVertex, valueVertex, valueEdge);

        ProcessorFunction countByKeyFunction = new ProcessorFunction(QueryFlowOuterClass.OperatorType.COUNT_BY_KEY, Message.Value.newBuilder().addIntValueList(0));
        LogicalUnaryVertex countByKeyVertex = new LogicalUnaryVertex(vertexIdManager.getId(), countByKeyFunction, false, valueVertex);
        logicalSubQueryPlan.addLogicalVertex(countByKeyVertex);
        logicalSubQueryPlan.addLogicalEdge(valueVertex, countByKeyVertex, new LogicalEdge(EdgeShuffleType.SHUFFLE_BY_KEY));

        ProcessorFunction ratioFunction = new ProcessorFunction(
                QueryFlowOuterClass.OperatorType.JOIN_RATIO,
                Message.Value.newBuilder()
                        .setIntValue(CompilerUtils.parseCompareType(predicate, predicate.getBiPredicate()).getNumber())
                        .setDoubleValue(Double.parseDouble(predicate.getValue().toString())));
        LogicalBinaryVertex logicalBinaryVertex = new LogicalBinaryVertex(vertexIdManager.getId(), ratioFunction, true, countVertex, countByKeyVertex);
        logicalSubQueryPlan.addLogicalVertex(logicalBinaryVertex);
        logicalSubQueryPlan.addLogicalEdge(countVertex, logicalBinaryVertex, new LogicalEdge(EdgeShuffleType.BROADCAST));
        logicalSubQueryPlan.addLogicalEdge(countByKeyVertex, logicalBinaryVertex, new LogicalEdge(EdgeShuffleType.FORWARD));

        setFinishVertex(logicalBinaryVertex, labelManager);
        addUsedLabelAndRequirement(logicalBinaryVertex, labelManager);

        return logicalSubQueryPlan;
    }

    @Override
    public ValueType getOutputValueType() {
        return new VertexValueType();
    }
}

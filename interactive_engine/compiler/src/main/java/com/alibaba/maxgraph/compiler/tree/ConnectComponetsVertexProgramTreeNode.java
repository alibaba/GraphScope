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
import com.alibaba.maxgraph.QueryFlowOuterClass.OperatorType;
import com.alibaba.maxgraph.compiler.api.schema.GraphEdge;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.compiler.tree.value.VertexValueType;
import com.alibaba.maxgraph.compiler.utils.ReflectionUtils;
import com.alibaba.maxgraph.common.util.SchemaUtils;
import org.apache.tinkerpop.gremlin.process.computer.clustering.connected.ConnectedComponentVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.ConnectedComponentVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.util.PureTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class ConnectComponetsVertexProgramTreeNode extends UnaryTreeNode {
    public static final String TIMES = "times";
    public static final String COMPONENT = "component";
    private ConnectedComponentVertexProgramStep step;

    public ConnectComponetsVertexProgramTreeNode(
            TreeNode input, GraphSchema schema, ConnectedComponentVertexProgramStep step) {
        super(input, NodeType.FLATMAP, schema);
        this.step = step;
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        ProcessorFunction processorFunction =
                new ProcessorFunction(OperatorType.PROGRAM_GRAPH_CC, createOperatorArgument());
        return parseSingleUnaryVertex(
                contextManager.getVertexIdManager(),
                contextManager.getTreeNodeLabelManager(),
                processorFunction,
                contextManager);
    }

    private Message.Value.Builder createOperatorArgument() {
        Message.Value.Builder valueBuilder = Message.Value.newBuilder();
        Message.ProgramCCArg.Builder ccArgumentBuilder = Message.ProgramCCArg.newBuilder();
        String clusterProperty =
                ReflectionUtils.getFieldValue(
                        ConnectedComponentVertexProgramStep.class, step, "clusterProperty");
        PureTraversal<Vertex, Edge> edgeTraversal =
                ReflectionUtils.getFieldValue(
                        ConnectedComponentVertexProgramStep.class, step, "edgeTraversal");
        Parameters parameters =
                ReflectionUtils.getFieldValue(
                        ConnectedComponentVertexProgramStep.class, step, "parameters");

        if (parameters.contains(TIMES)) {
            ArrayList<Object> times = (ArrayList<Object>) parameters.remove(TIMES);
            ccArgumentBuilder.setLoopLimit((int) times.get(0));
        } else {
            ccArgumentBuilder.setLoopLimit(20);
        }
        if (clusterProperty.equals(ConnectedComponentVertexProgram.COMPONENT)) {
            clusterProperty = COMPONENT;
        }
        ccArgumentBuilder.setPropertyCcId(SchemaUtils.getPropId(clusterProperty, schema));

        List<Step> edgeTraversalSteps = edgeTraversal.getPure().getSteps();
        VertexStep vstep = (VertexStep) edgeTraversalSteps.get(0);
        for (String edgeLabel : vstep.getEdgeLabels()) {
            GraphEdge edgeType = (GraphEdge) schema.getElement(edgeLabel);
            ccArgumentBuilder.addEdgeLabels(edgeType.getLabelId());
        }
        if (Direction.BOTH.equals(vstep.getDirection())) {
            ccArgumentBuilder.setDirection(Message.EdgeDirection.DIR_NONE);
        } else if (Direction.IN.equals(vstep.getDirection())) {
            ccArgumentBuilder.setDirection(Message.EdgeDirection.DIR_IN);
        } else if (Direction.OUT.equals(vstep.getDirection())) {
            ccArgumentBuilder.setDirection(Message.EdgeDirection.DIR_OUT);
        } else {
            checkArgument(false, "direction must be in/out/both for shortest path");
        }
        for (int i = 1; i < edgeTraversalSteps.size(); ++i) {
            Step hasStep = edgeTraversalSteps.get(i);
            if (hasStep instanceof HasStep) {
                HasContainer hasContainer =
                        (HasContainer) ((HasStep) hasStep).getHasContainers().get(0);
                Object value = hasContainer.getPredicate().getValue();
                GraphEdge edgeType = (GraphEdge) schema.getElement(value.toString());
                ccArgumentBuilder.addEdgeLabels(edgeType.getLabelId());
            }
        }
        valueBuilder.setPayload(ccArgumentBuilder.build().toByteString());
        return valueBuilder;
    }

    @Override
    public ValueType getOutputValueType() {
        return new VertexValueType();
    }
}

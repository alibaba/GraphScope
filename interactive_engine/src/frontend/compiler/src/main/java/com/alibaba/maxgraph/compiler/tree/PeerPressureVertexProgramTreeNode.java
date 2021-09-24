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
import com.alibaba.maxgraph.compiler.api.schema.GraphEdge;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.compiler.logical.VertexIdManager;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.compiler.tree.value.VertexValueType;
import com.alibaba.maxgraph.compiler.utils.ReflectionUtils;
import com.alibaba.maxgraph.common.util.SchemaUtils;
import org.apache.tinkerpop.gremlin.process.computer.clustering.peerpressure.PeerPressureVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.PeerPressureVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.PureTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class PeerPressureVertexProgramTreeNode extends UnaryTreeNode {
    public static final String PEER_PRESSURE = "cluster";
    private final PeerPressureVertexProgramStep step;

    public PeerPressureVertexProgramTreeNode(TreeNode input, GraphSchema schema, PeerPressureVertexProgramStep step) {
        super(input, NodeType.FLATMAP, schema);
        this.step = step;
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        TreeNodeLabelManager labelManager = contextManager.getTreeNodeLabelManager();
        VertexIdManager vertexIdManager = contextManager.getVertexIdManager();
        ProcessorFunction processorFunction = new ProcessorFunction(QueryFlowOuterClass.OperatorType. PROGRAM_GRAPH_PEERPRESSURE,
            createOperatorArgument());
        return parseSingleUnaryVertex(vertexIdManager, labelManager, processorFunction, contextManager);
    }

    private Message.Value.Builder createOperatorArgument() {
        Message.Value.Builder valueBuilder = Message.Value.newBuilder();
        Message.ProgramPeerPressureArg.Builder peerPressureArgBuilder = Message.ProgramPeerPressureArg.newBuilder();
        String clusterProperty = ReflectionUtils.getFieldValue(PeerPressureVertexProgramStep.class, step, "clusterProperty");
        int times = ReflectionUtils.getFieldValue(PeerPressureVertexProgramStep.class, step, "times");
        PureTraversal<Vertex, Edge> edgeTraversal = ReflectionUtils.getFieldValue(PeerPressureVertexProgramStep.class, step, "edgeTraversal");

        if(clusterProperty.equals(PeerPressureVertexProgram.CLUSTER)){
            clusterProperty = PEER_PRESSURE;
        }
        peerPressureArgBuilder.setPropertyPpId(SchemaUtils.getPropId(clusterProperty,schema));
        checkArgument(times > 0, "iteration must > 0 for PeerPressure");
        peerPressureArgBuilder.setLoopLimit(times);

        List<Step> edgeTraversalSteps = edgeTraversal.getPure().getSteps();
        VertexStep vstep = (VertexStep)edgeTraversalSteps.get(0);
        for (String edgeLabel : vstep.getEdgeLabels()) {
            GraphEdge edgeType = (GraphEdge)schema.getElement(edgeLabel);
            peerPressureArgBuilder.addEdgeLabels(edgeType.getLabelId());
        }
        if (Direction.BOTH.equals(vstep.getDirection())) {
            peerPressureArgBuilder.setDirection(Message.EdgeDirection.DIR_NONE);
        } else if (Direction.IN.equals(vstep.getDirection())) {
            peerPressureArgBuilder.setDirection(Message.EdgeDirection.DIR_IN);
        } else if (Direction.OUT.equals(vstep.getDirection())) {
            peerPressureArgBuilder.setDirection(Message.EdgeDirection.DIR_OUT);
        } else {
            checkArgument(false, "direction must be in/out/both for shortest path");
        }
        for (int i = 1; i < edgeTraversalSteps.size(); ++i) {
            Step hasStep = edgeTraversalSteps.get(i);
            if (hasStep instanceof HasStep) {
                HasContainer hasContainer = (HasContainer)((HasStep)hasStep).getHasContainers().get(0);
                Object value = hasContainer.getPredicate().getValue();
                GraphEdge edgeType = (GraphEdge)schema.getElement(value.toString());
                peerPressureArgBuilder.addEdgeLabels(edgeType.getLabelId());
            }
        }

        valueBuilder.setPayload(peerPressureArgBuilder.build().toByteString());
        return valueBuilder;
    }

    @Override
    public ValueType getOutputValueType() {
        return new VertexValueType();
    }
}


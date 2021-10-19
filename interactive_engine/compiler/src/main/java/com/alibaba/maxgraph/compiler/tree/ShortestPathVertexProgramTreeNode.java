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

import com.alibaba.maxgraph.compiler.utils.MaxGraphUtils;
import com.alibaba.maxgraph.compiler.utils.ReflectionUtils;
import com.alibaba.maxgraph.common.util.SchemaUtils;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.ShortestPathVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ConstantStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class ShortestPathVertexProgramTreeNode extends UnaryTreeNode {
    private final ShortestPathVertexProgramStep step;


    public ShortestPathVertexProgramTreeNode(TreeNode input, GraphSchema schema, ShortestPathVertexProgramStep step) {
        super(input, NodeType.FLATMAP, schema);
        this.step = step;
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        TreeNodeLabelManager labelManager = contextManager.getTreeNodeLabelManager();
        VertexIdManager vertexIdManager = contextManager.getVertexIdManager();
        ProcessorFunction processorFunction = new ProcessorFunction(
                QueryFlowOuterClass.OperatorType.PROGRAM_GRAPH_SHORTESTPATH,
                createOperatorArgument());
        return parseSingleUnaryVertex(vertexIdManager, labelManager, processorFunction, contextManager);
    }

    private Message.Value.Builder createOperatorArgument() {
        Message.Value.Builder valueBuilder = Message.Value.newBuilder();
        Message.VertexProgramShortestPathArg.Builder shortestPathArgBuilder = Message.VertexProgramShortestPathArg
                .newBuilder();

        List<Traversal.Admin<?, ?>> traversalSteps = step.getLocalChildren();
        Traversal.Admin<?, ?> targetVertexFilter = traversalSteps.get(0);
        Traversal.Admin<?, ?> edgeTraversal = traversalSteps.get(1);
        Traversal.Admin<?, ?> distanceTraversal = traversalSteps.get(2);

        Number maxDistance = ReflectionUtils.getFieldValue(ShortestPathVertexProgramStep.class, step, "maxDistance");
        boolean includeEdges = ReflectionUtils.getFieldValue(ShortestPathVertexProgramStep.class, step, "includeEdges");

        // targetVertexFilter
        // only support .with(ShortestPath.target,__.has('name','peter')) or default case
        Step targetVertexStep = targetVertexFilter.getSteps().get(0);
        if (targetVertexStep instanceof HasStep) {
            HasStep hasStep = (HasStep) targetVertexStep;
            List<HasContainer> hasContainerList = hasStep.getHasContainers();
            for (HasContainer hasContainer : hasContainerList) {
                String key = hasContainer.getKey();
                Object value = hasContainer.getValue();

                Message.Value.Builder propertyValueBuilder = Message.Value.newBuilder();
                Message.VariantType variantType = MaxGraphUtils.parsePropertyDataType(key, schema);
                propertyValueBuilder.setIndex(schema.getPropertyId(key));

                propertyValueBuilder.setValueType(variantType);
                switch (variantType) {
                    case VT_INT:
                        propertyValueBuilder.setIntValue(Integer.parseInt(value.toString()));
                        break;
                    case VT_LONG:
                        propertyValueBuilder.setLongValue(Long.parseLong(value.toString()));
                        break;
                    case VT_DOUBLE:
                        propertyValueBuilder.setDoubleValue(Double.parseDouble(value.toString()));
                        break;
                    case VT_STRING:
                        propertyValueBuilder.setStrValue(value.toString());
                        break;
                    default:
                        throw new IllegalArgumentException("value in with-step is not supported yet => " + hasStep
                                .toString());
                }
                shortestPathArgBuilder.setTarget(propertyValueBuilder);
                shortestPathArgBuilder.setHasTarget(true);
            }
        } else if (targetVertexStep instanceof IdentityStep) {
            shortestPathArgBuilder.setHasTarget(false);
        } else {
            throw new IllegalArgumentException(
                    "step for targetVertexFilter in shortest path is not supported yet => " + step);
        }

        // distanceTraversal supports ".with(ShortestPath.distance, 'weight')" and default case
        Step distanceStep = distanceTraversal.getSteps().get(0);
        if (distanceStep instanceof PropertiesStep) {
            String weight = ((PropertiesStep) distanceStep).getPropertyKeys()[0];
            shortestPathArgBuilder.setPropertyEdgeWeightId(SchemaUtils.getPropId(weight, schema));
            shortestPathArgBuilder.setWeightFlag(true);
        } else if (distanceStep instanceof ConstantStep) {
            shortestPathArgBuilder.setPropertyEdgeWeightId(-1);
            shortestPathArgBuilder.setWeightFlag(false);
        } else {
            throw new IllegalArgumentException(
                    "step for distanceTraversal in shortest path is not supported yet => " + step);
        }

        if (maxDistance == null) {
            maxDistance = 10;
        }
        checkArgument((int) maxDistance > 0, "iteration must > 0 for shortest path");
        shortestPathArgBuilder.setLoopLimit((int) maxDistance);

        // edgeTraversal supports ".with(ShortestPath.edges, Direction.IN)", ".with(ShortestPath.edges, outE('edge'))
        // ", default as OUT
        List<Step> edgeTraversalSteps = edgeTraversal.getSteps();
        shortestPathArgBuilder.setWeightLb(0);
        VertexStep vstep = (VertexStep) edgeTraversalSteps.get(0);
        for (String edgeLabel : vstep.getEdgeLabels()) {
            GraphEdge edgeType = (GraphEdge) schema.getElement(edgeLabel);
            shortestPathArgBuilder.addEdgeLabels(edgeType.getLabelId());
        }
        if (Direction.BOTH.equals(vstep.getDirection())) {
            shortestPathArgBuilder.setDirection(Message.EdgeDirection.DIR_NONE);
        } else if (Direction.IN.equals(vstep.getDirection())) {
            shortestPathArgBuilder.setDirection(Message.EdgeDirection.DIR_IN);
        } else if (Direction.OUT.equals(vstep.getDirection())) {
            shortestPathArgBuilder.setDirection(Message.EdgeDirection.DIR_OUT);
        } else {
            checkArgument(false, "direction must be in/out/both for shortest path");
        }
        for (int i = 1; i < edgeTraversalSteps.size(); ++i) {
            Step hasStep = edgeTraversalSteps.get(i);
            if (hasStep instanceof HasStep) {
                HasContainer hasContainer = (HasContainer) ((HasStep) hasStep).getHasContainers().get(0);
                String key = hasContainer.getKey();
                Object value = hasContainer.getPredicate().getValue();
                if (hasContainer.getPredicate().getBiPredicate().toString().equals("gt") && (value instanceof Number)) {
                    shortestPathArgBuilder.setWeightLb(((Number) value).doubleValue());
                    shortestPathArgBuilder.setPropertyEdgeWeightId(SchemaUtils.getPropId(key, schema));
                } else {
                    GraphEdge edgeType = (GraphEdge) schema.getElement(value.toString());
                    shortestPathArgBuilder.addEdgeLabels(edgeType.getLabelId());
                }
            }
        }

        valueBuilder.setPayload(shortestPathArgBuilder.build().toByteString());
        return valueBuilder;
    }

    @Override
    public ValueType getOutputValueType() {
        return new VertexValueType();
    }
}

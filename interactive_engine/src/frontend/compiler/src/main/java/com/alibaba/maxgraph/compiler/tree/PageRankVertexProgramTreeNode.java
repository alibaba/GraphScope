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
import org.apache.tinkerpop.gremlin.process.computer.ranking.pagerank.PageRankVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.PageRankVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class PageRankVertexProgramTreeNode extends UnaryTreeNode {
    public static final String PAGERANK = "pageRank";
    PageRankVertexProgramStep pageRankVertexProgramStep;

    public PageRankVertexProgramTreeNode(TreeNode input, GraphSchema schema,
                                         PageRankVertexProgramStep pageRankVertexProgramStep) {
        super(input, NodeType.FLATMAP, schema);
        this.pageRankVertexProgramStep = pageRankVertexProgramStep;
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        TreeNodeLabelManager labelManager = contextManager.getTreeNodeLabelManager();
        VertexIdManager vertexIdManager = contextManager.getVertexIdManager();
        ProcessorFunction processorFunction = new ProcessorFunction(
            QueryFlowOuterClass.OperatorType.PROGRAM_GRAPH_PAGERANK,
            createOperatorArgument());
        return parseSingleUnaryVertex(vertexIdManager, labelManager, processorFunction, contextManager);

    }

    private Message.Value.Builder createOperatorArgument() {
        Message.Value.Builder valueBuilder = Message.Value.newBuilder();
        Message.ProgramPageRankArg.Builder pageRankArgBuilder = Message.ProgramPageRankArg.newBuilder();

        String pageRankProperty = ReflectionUtils.getFieldValue(PageRankVertexProgramStep.class,
            pageRankVertexProgramStep, "pageRankProperty");
        double alpha = ReflectionUtils.getFieldValue(PageRankVertexProgramStep.class, pageRankVertexProgramStep,
            "alpha");
        int times = ReflectionUtils.getFieldValue(PageRankVertexProgramStep.class, pageRankVertexProgramStep, "times");
        Traversal.Admin<Vertex, Edge> edgeTraversal = pageRankVertexProgramStep.getLocalChildren().get(0);

        if(pageRankProperty.equals(PageRankVertexProgram.PAGE_RANK)) {
            pageRankProperty = PAGERANK;
        }
        pageRankArgBuilder.setPropertyPrId(SchemaUtils.getPropId(pageRankProperty, schema));
        checkArgument(alpha > 0 && alpha < 1, "alpha must > 0 and < 1 for PageRank");
        pageRankArgBuilder.setAlpha(alpha);
        checkArgument(times > 0, "iteration must > 0 for PageRank");
        pageRankArgBuilder.setLoopLimit(times);

        List<Step> steps = edgeTraversal.getSteps();

        // supports with(PageRank.edges,outE('knows'))
        VertexStep vstep = (VertexStep)steps.get(0);
        String[] labels = vstep.getEdgeLabels();
        for (String edgeLabel : labels) {
            GraphEdge edgeType = (GraphEdge)schema.getElement(edgeLabel);
            pageRankArgBuilder.addEdgeLabels(edgeType.getLabelId());
        }
        if (Direction.BOTH.equals(vstep.getDirection())) {
            pageRankArgBuilder.setDirection(Message.EdgeDirection.DIR_NONE);
        } else if (Direction.IN.equals(vstep.getDirection())) {
            pageRankArgBuilder.setDirection(Message.EdgeDirection.DIR_IN);
        } else if (Direction.OUT.equals(vstep.getDirection())) {
            pageRankArgBuilder.setDirection(Message.EdgeDirection.DIR_OUT);
        } else {
            checkArgument(false, "direction must be in/out/both for pagerank");
        }
        for (int i = 1; i < steps.size(); ++i) {
            Step step = steps.get(i);
            if (step instanceof HasStep) {
                HasContainer hasContainer = (HasContainer)((HasStep)step).getHasContainers().get(0);
                Object value = hasContainer.getPredicate().getValue();
                GraphEdge edgeType = (GraphEdge)schema.getElement(value.toString());
                pageRankArgBuilder.addEdgeLabels(edgeType.getLabelId());
            }
        }
        valueBuilder.setPayload(pageRankArgBuilder.build().toByteString());
        return valueBuilder;
    }

    @Override
    public ValueType getOutputValueType() {
        return new VertexValueType();
    }
}

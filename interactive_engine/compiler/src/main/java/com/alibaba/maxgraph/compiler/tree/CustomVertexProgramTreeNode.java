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
import com.alibaba.maxgraph.common.util.SchemaUtils;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.tree.value.MapEntryValueType;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.compiler.tree.value.ValueValueType;
import com.alibaba.maxgraph.compiler.tree.value.VertexValueType;
import com.alibaba.maxgraph.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.program.*;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class CustomVertexProgramTreeNode extends UnaryTreeNode {
    private VertexProgram customProgram;

    public CustomVertexProgramTreeNode(TreeNode input, GraphSchema schema, VertexProgram customProgram) {
        super(input, NodeType.FLATMAP, schema);
        this.customProgram = customProgram;
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        ProcessorFunction processorFunction = new ProcessorFunction(getOperatorType(customProgram), createOperatorArgument(customProgram));
        return parseSingleUnaryVertex(contextManager.getVertexIdManager(), contextManager.getTreeNodeLabelManager(), processorFunction, contextManager);
    }

    private Message.Value.Builder createOperatorArgument(VertexProgram customProgram) {
        Message.Value.Builder valueBuilder = Message.Value.newBuilder();
        if (customProgram instanceof GraphConnectedComponentVertexProgram) {
            Message.ProgramCCArg.Builder ccArgumentBuilder = Message.ProgramCCArg.newBuilder();
            List<String> outputList = ((GraphConnectedComponentVertexProgram) customProgram).getOutputList();
            outputList.forEach(v -> {
                ccArgumentBuilder.setPropertyCcId(SchemaUtils.getPropId(v, schema));
            });
            switch (((GraphConnectedComponentVertexProgram) customProgram).getDirection()) {
                case IN:
                    ccArgumentBuilder.setDirection(Message.EdgeDirection.DIR_IN);
                    break;
                case BOTH:
                    ccArgumentBuilder.setDirection(Message.EdgeDirection.DIR_NONE);
                    break;
                default:
                    ccArgumentBuilder.setDirection(Message.EdgeDirection.DIR_OUT);
            }
            int iteration = ((GraphConnectedComponentVertexProgram) customProgram).getIteration();
            checkArgument(iteration > 0, "iteration must > 0 for graph cc");
            ccArgumentBuilder.setLoopLimit(iteration);
            valueBuilder.setPayload(ccArgumentBuilder.build().toByteString());
        } else if (customProgram instanceof GraphPageRankVertexProgram) {
            Message.ProgramPageRankArg.Builder pageRankArgBuilder = Message.ProgramPageRankArg.newBuilder();
            pageRankArgBuilder.setPropertyPrId(SchemaUtils.getPropId(((GraphPageRankVertexProgram) customProgram).getProperty(), schema));
            checkArgument(((GraphPageRankVertexProgram) customProgram).getAlpha() > 0, "alpha must > 0 for PageRank");
            checkArgument(((GraphPageRankVertexProgram) customProgram).getAlpha() < 1, "alpha must < 1 for PageRank");
            pageRankArgBuilder.setAlpha(((GraphPageRankVertexProgram) customProgram).getAlpha());
            switch (((GraphPageRankVertexProgram) customProgram).getDirection()) {
                case IN:
                    pageRankArgBuilder.setDirection(Message.EdgeDirection.DIR_IN);
                    break;
                case BOTH:
                    pageRankArgBuilder.setDirection(Message.EdgeDirection.DIR_NONE);
                    break;
                default:
                    pageRankArgBuilder.setDirection(Message.EdgeDirection.DIR_OUT);
            }
            for (String edgeLabel : ((GraphPageRankVertexProgram) customProgram).getEdgeLabels()) {
                pageRankArgBuilder.addEdgeLabels(schema.getElement(edgeLabel).getLabelId());
            }
            checkArgument(((GraphPageRankVertexProgram) customProgram).getMaxIterations() > 0, "iteration must > 0 for PageRank");
            pageRankArgBuilder.setLoopLimit(((GraphPageRankVertexProgram) customProgram).getMaxIterations());
            valueBuilder.setPayload(pageRankArgBuilder.build().toByteString());
        } else if (customProgram instanceof GraphHitsVertexProgram) {
            Message.ProgramGraphHITSArg.Builder hitsArgBuilder = Message.ProgramGraphHITSArg.newBuilder();
            hitsArgBuilder.setPropertyAuthId(SchemaUtils.getPropId(((GraphHitsVertexProgram) customProgram).getAuthProp(), schema));
            hitsArgBuilder.setPropertyHubId(SchemaUtils.getPropId(((GraphHitsVertexProgram) customProgram).getHubProp(), schema));
            for (String edgeLabel : ((GraphHitsVertexProgram) customProgram).getEdgeLabels()) {
                hitsArgBuilder.addEdgeLabels(schema.getElement(edgeLabel).getLabelId());
            }
            checkArgument(((GraphHitsVertexProgram) customProgram).getMaxIterations() > 0, "iteration must > 0 for HITS");
            hitsArgBuilder.setLoopLimit(((GraphHitsVertexProgram) customProgram).getMaxIterations());
            valueBuilder.setPayload(hitsArgBuilder.build().toByteString());
        } else if (customProgram instanceof GraphLpaVertexProgram) {
            Message.ProgramLPAArg.Builder lpaArgBuilder = Message.ProgramLPAArg.newBuilder();
            lpaArgBuilder.setSeedLabel(SchemaUtils.getPropId(((GraphLpaVertexProgram) customProgram).getSeed(), schema));
            lpaArgBuilder.setTargetLabel(SchemaUtils.getPropId(((GraphLpaVertexProgram) customProgram).getProperty(), schema));
            for (String edgeLabel : ((GraphLpaVertexProgram) customProgram).getEdgeLabels()) {
                lpaArgBuilder.addEdgeLabels(schema.getElement(edgeLabel).getLabelId());
            }
            lpaArgBuilder.setIteration(((GraphLpaVertexProgram) customProgram).getMaxIterations());
            switch (((GraphLpaVertexProgram) customProgram).getDirection()) {
                case IN:
                    lpaArgBuilder.setDirection(Message.EdgeDirection.DIR_IN);
                    break;
                case BOTH:
                    lpaArgBuilder.setDirection(Message.EdgeDirection.DIR_NONE);
                    break;
                default:
                    lpaArgBuilder.setDirection(Message.EdgeDirection.DIR_OUT);
            }
            valueBuilder.setPayload(lpaArgBuilder.build().toByteString());
        }
        return valueBuilder;
    }

    private QueryFlowOuterClass.OperatorType getOperatorType(VertexProgram customProgram) {
        if (customProgram instanceof ConnectedComponentVertexProgram) {
            return QueryFlowOuterClass.OperatorType.PROGRAM_CC;
        } else if (customProgram instanceof GraphConnectedComponentVertexProgram) {
            return QueryFlowOuterClass.OperatorType.PROGRAM_GRAPH_CC;
        } else if (customProgram instanceof GraphPageRankVertexProgram) {
            return QueryFlowOuterClass.OperatorType.PROGRAM_GRAPH_PAGERANK;
        } else if (customProgram instanceof GraphHitsVertexProgram) {
            return QueryFlowOuterClass.OperatorType.PROGRAM_GRAPH_HITS;
        } else if (customProgram instanceof GraphLpaVertexProgram) {
            return QueryFlowOuterClass.OperatorType.PROGRAM_GRAPH_LPA;
        } else {
            throw new UnsupportedOperationException();
        }
    }

    private ValueType getValueType(VertexProgram customProgram) {
        if (customProgram instanceof ConnectedComponentVertexProgram) {
            return new MapEntryValueType(new VertexValueType(), new ValueValueType(Message.VariantType.VT_INT));
        } else if (customProgram instanceof GraphConnectedComponentVertexProgram) {
            return new VertexValueType();
        } else if (customProgram instanceof GraphPageRankVertexProgram) {
            return new VertexValueType();
        } else if (customProgram instanceof GraphHitsVertexProgram) {
            return new VertexValueType();
        } else if (customProgram instanceof GraphLpaVertexProgram) {
            return new VertexValueType();
        } else {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public ValueType getOutputValueType() {
        return getValueType(customProgram);
    }
}

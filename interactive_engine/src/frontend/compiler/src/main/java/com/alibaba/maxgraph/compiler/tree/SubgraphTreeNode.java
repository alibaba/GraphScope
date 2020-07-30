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
import com.alibaba.maxgraph.compiler.api.schema.EdgeRelation;
import com.alibaba.maxgraph.compiler.api.schema.GraphEdge;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.compiler.tree.value.VertexValueType;
import com.alibaba.maxgraph.compiler.logical.LogicalEdge;
import com.alibaba.maxgraph.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.compiler.logical.LogicalUnaryVertex;
import com.alibaba.maxgraph.compiler.logical.LogicalVertex;
import com.alibaba.maxgraph.compiler.logical.VertexIdManager;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.compiler.optimizer.ContextManager;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;

public class SubgraphTreeNode extends UnaryTreeNode {
    private boolean vertexFlag = false;

    public SubgraphTreeNode(TreeNode input, GraphSchema schema) {
        super(input, NodeType.GRAPH, schema);
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        TreeNodeLabelManager treeNodeLabelManager = contextManager.getTreeNodeLabelManager();
        VertexIdManager vertexIdManager = contextManager.getVertexIdManager();
        LogicalSubQueryPlan logicalSubQueryPlan = new LogicalSubQueryPlan(contextManager);
        LogicalVertex sourceDelegateVertex = getInputNode().getOutputVertex();
        logicalSubQueryPlan.addLogicalVertex(sourceDelegateVertex);

        List<Integer> edgeLabelList = Lists.newArrayList();
        if (sourceDelegateVertex.getProcessorFunction().getOperatorType() == QueryFlowOuterClass.OperatorType.E) {
            Message.Value.Builder argumentBuilder = sourceDelegateVertex.getProcessorFunction().getArgumentBuilder();
            edgeLabelList.addAll(argumentBuilder.getIntValueListList());
        }
        Set<Integer> sourceVertexList = Sets.newHashSet();
        Set<Integer> targetVertexList = Sets.newHashSet();
        Message.SubgraphVertexList.Builder subgraphBuilder = Message.SubgraphVertexList.newBuilder();
        for (Integer edgeLabel : edgeLabelList) {
            GraphEdge edgeType = (GraphEdge) schema.getElement(edgeLabel);
            for (EdgeRelation relationShip : edgeType.getRelationList()) {
                sourceVertexList.add(relationShip.getSource().getLabelId());
                targetVertexList.add(relationShip.getTarget().getLabelId());
            }
        }
        if (sourceVertexList.isEmpty()) {
            schema.getVertexList().forEach(v -> {
                sourceVertexList.add(v.getLabelId());
                targetVertexList.add(v.getLabelId());
            });
        }
        subgraphBuilder.addAllSourceVertexList(sourceVertexList)
                .addAllTargetVertexList(targetVertexList);

        ProcessorFunction processorFunction = new ProcessorFunction(
                QueryFlowOuterClass.OperatorType.SUBGRAPH,
                Message.Value.newBuilder()
                        .setBoolFlag(this.vertexFlag)
                        .setPayload(subgraphBuilder.build().toByteString()));
        LogicalVertex graphVertex = new LogicalUnaryVertex(vertexIdManager.getId(), processorFunction, false, sourceDelegateVertex);
        logicalSubQueryPlan.addLogicalVertex(graphVertex);
        logicalSubQueryPlan.addLogicalEdge(sourceDelegateVertex, graphVertex, new LogicalEdge());

        setFinishVertex(graphVertex, treeNodeLabelManager);
        addUsedLabelAndRequirement(graphVertex, treeNodeLabelManager);

        return logicalSubQueryPlan;
    }

    @Override
    public ValueType getOutputValueType() {
        return new VertexValueType();
    }

    public void enableVertexFlag() {
        this.vertexFlag = true;
    }
}

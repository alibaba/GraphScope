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
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.tree.value.MapValueType;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.compiler.tree.value.ValueValueType;
import com.alibaba.maxgraph.compiler.tree.value.VarietyValueType;
import com.alibaba.maxgraph.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.compiler.optimizer.ContextManager;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

public class AggregationListTreeNode extends UnaryTreeNode {
    private List<String> aggNameList;
    private List<TreeNode> aggNodeList;

    public AggregationListTreeNode(
            TreeNode input,
            GraphSchema schema,
            List<String> aggNameList,
            List<TreeNode> aggNodeList) {
        super(input, NodeType.AGGREGATE_LIST, schema);
        this.aggNameList = aggNameList;
        this.aggNodeList = aggNodeList;
        checkArgument(
                aggNameList.size() == aggNodeList.size(),
                "agg name list must equal to agg node list");
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        throw new IllegalArgumentException("not support aggregation yet.");
        //        LogicalSubQueryPlan logicalSubQueryPlan = new
        // LogicalSubQueryPlan(treeNodeLabelManager, contextManager);
        //        LogicalVertex sourceVertex = getInputNode().getOutputVertex();
        //        ValueType sourceValueType = getInputNode().getOutputValueType();
        //        logicalSubQueryPlan.addLogicalVertex(sourceVertex);
        //
        //        String keyLabel = treeNodeLabelManager.createSysLabelStart(sourceVertex, "key");
        //        int index = 0;
        //        LogicalVertex dedupSourceVertex = new LogicalUnaryVertex(vertexIdManager.getId(),
        // new ProcessorFunction(QueryFlowOuterClass.OperatorType.DEDUP), false, sourceVertex);
        //        logicalSubQueryPlan.addLogicalVertex(dedupSourceVertex);
        //        logicalSubQueryPlan.addLogicalEdge(sourceVertex, dedupSourceVertex, new
        // LogicalEdge());
        //
        //        LogicalVertex lastJoinVertex = dedupSourceVertex;
        //        for (TreeNode aggNode : aggNodeList) {
        //            TreeNode currentTreeNode = CompilerUtils.getSourceTreeNode(aggNode);
        //            currentTreeNode.setFinishVertex(lastJoinVertex, treeNodeLabelManager);
        //            while (currentTreeNode != null) {
        //                if (currentTreeNode instanceof ByKeyTreeNode) {
        //                    ByKeyTreeNode.class.cast(currentTreeNode).addByKey(keyLabel,
        // lastJoinVertex, sourceValueType);
        //                }
        //                currentTreeNode = currentTreeNode.getOutputNode();
        //            }
        //            if (aggNode instanceof ByKeyTreeNode) {
        //                ByKeyTreeNode.class.cast(aggNode).setJoinValueFlag(false);
        //            }
        //            parseSingleResultForSource(
        //                    aggNode,
        //                    sourceValueType,
        //                    contextManager,
        //                    vertexIdManager,
        //                    treeNodeLabelManager,
        //                    logicalSubQueryPlan,
        //                    sourceVertex,
        //                    false);
        //            List<LogicalVertex> outputVertexList =
        // logicalSubQueryPlan.getOutputVertexList();
        //            outputVertexList.remove(lastJoinVertex);
        //            checkArgument(outputVertexList.size() == 1);
        //
        //            QueryFlowOuterClass.OperatorType joinType =
        // CompilerUtils.parseJoinOperatorType(aggNode);
        //            String valueLabel = treeNodeLabelManager.createSysLabelStart("val");
        //            treeNodeLabelManager.replaceLabelName(valueLabel, aggNameList.get(index));
        //            int valueLabelId = treeNodeLabelManager.getLabelIndex(aggNameList.get(index));
        //            ProcessorFunction joinFunction = new ProcessorFunction(joinType,
        // Message.Value.newBuilder().setIntValue(valueLabelId));
        //            LogicalVertex outputVertex = outputVertexList.get(0);
        //            LogicalBinaryVertex joinVertex = new
        // LogicalBinaryVertex(vertexIdManager.getId(), joinFunction, false, lastJoinVertex,
        // outputVertex);
        //            logicalSubQueryPlan.addLogicalVertex(joinVertex);
        //            logicalSubQueryPlan.addLogicalEdge(lastJoinVertex, joinVertex, new
        // LogicalEdge());
        //            logicalSubQueryPlan.addLogicalEdge(outputVertex, joinVertex, new
        // LogicalEdge());
        //            lastJoinVertex = joinVertex;
        //            index++;
        //        }
        //
        //        LogicalVertex outputVertex = logicalSubQueryPlan.getOutputVertex();
        //        List<Integer> labelIdList = Lists.newArrayList();
        //        aggNameList.forEach(v -> labelIdList.add(treeNodeLabelManager.getLabelIndex(v)));
        //        ProcessorFunction selectFunction = new ProcessorFunction(
        //                QueryFlowOuterClass.OperatorType.SELECT,
        //                Message.Value.newBuilder()
        //                        .addAllIntValueList(labelIdList)
        //                        .addAllStrValueList(aggNameList)
        //                        .setIntValue(Message.PopType.FIRST.getNumber())
        //                        .setBoolValue(true));
        //        LogicalVertex selectVertex = new LogicalUnaryVertex(vertexIdManager.getId(),
        // selectFunction, false, outputVertex);
        //        logicalSubQueryPlan.addLogicalVertex(selectVertex);
        //        logicalSubQueryPlan.addLogicalEdge(outputVertex, selectVertex, new LogicalEdge());
        //
        //        addUsedLabelAndRequirement(selectVertex, treeNodeLabelManager);
        //        setFinishVertex(selectVertex, treeNodeLabelManager);
        //
        //        return logicalSubQueryPlan;
    }

    @Override
    public ValueType getOutputValueType() {
        Set<ValueType> aggValueList = Sets.newHashSet();
        aggNodeList.forEach(v -> aggValueList.add(v.getOutputValueType()));
        return new MapValueType(
                new ValueValueType(Message.VariantType.VT_STRING),
                aggValueList.size() > 1
                        ? new VarietyValueType(aggValueList)
                        : aggValueList.iterator().next());
    }
}

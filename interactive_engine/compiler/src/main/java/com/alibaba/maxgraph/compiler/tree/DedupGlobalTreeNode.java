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
import com.alibaba.maxgraph.compiler.logical.LogicalQueryPlan;
import com.alibaba.maxgraph.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.compiler.tree.addition.AbstractUseKeyNode;
import com.alibaba.maxgraph.common.util.SchemaUtils;
import com.alibaba.maxgraph.compiler.utils.TreeNodeUtils;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.compiler.logical.LogicalEdge;
import com.alibaba.maxgraph.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.compiler.logical.LogicalUnaryVertex;
import com.alibaba.maxgraph.compiler.logical.LogicalVertex;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.compiler.tree.source.SourceDelegateNode;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.structure.T;

import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

public class DedupGlobalTreeNode extends AbstractUseKeyNode {
    private TreeNode dedupTreeNode;
    // Dedup in sub query
    private boolean subDedupFlag = false;

    public DedupGlobalTreeNode(TreeNode input, GraphSchema schema, Set<String> dedupLabelList) {
        super(input, NodeType.FILTER, schema);
        checkArgument(dedupLabelList.isEmpty(), "Not support dedup label yet");
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        LogicalSubQueryPlan logicalSubQueryPlan = new LogicalSubQueryPlan(contextManager);
        LogicalVertex sourceVertex = getInputNode().getOutputVertex();
        logicalSubQueryPlan.addLogicalVertex(sourceVertex);

        QueryFlowOuterClass.OperatorType dedupType =
                getUseKeyOperator(QueryFlowOuterClass.OperatorType.DEDUP);
        if (null == dedupTreeNode || dedupTreeNode instanceof SourceDelegateNode) {
            ProcessorFunction dedupFunction = new ProcessorFunction(dedupType);
            LogicalVertex dedupVertex =
                    new LogicalUnaryVertex(
                            contextManager.getVertexIdManager().getId(),
                            dedupFunction,
                            isPropLocalFlag(),
                            sourceVertex);
            logicalSubQueryPlan.addLogicalVertex(dedupVertex);
            logicalSubQueryPlan.addLogicalEdge(sourceVertex, dedupVertex, new LogicalEdge());
        } else {
            UnaryTreeNode unaryDedupTreeNode = UnaryTreeNode.class.cast(dedupTreeNode);
            if (unaryDedupTreeNode.getInputNode() instanceof SourceDelegateNode
                    && ((unaryDedupTreeNode instanceof ElementValueTreeNode
                                    && ElementValueTreeNode.class
                                                    .cast(unaryDedupTreeNode)
                                                    .getByPassTraversal()
                                            == null)
                            || unaryDedupTreeNode instanceof SelectOneTreeNode
                            || unaryDedupTreeNode instanceof TokenTreeNode)) {
                // id()/label() has been converted to select one tree node
                Message.Value.Builder argumentBuilder = Message.Value.newBuilder();
                Set<Integer> usedLabelList = Sets.newHashSet();
                if (unaryDedupTreeNode instanceof ElementValueTreeNode) {
                    String propKey =
                            ((ElementValueTreeNode) unaryDedupTreeNode)
                                    .getPropKeyList()
                                    .iterator()
                                    .next();
                    int propId = SchemaUtils.getPropId(propKey, schema);
                    argumentBuilder.setIntValue(propId);
                } else if (unaryDedupTreeNode instanceof TokenTreeNode) {
                    T token = ((TokenTreeNode) unaryDedupTreeNode).getToken();
                    argumentBuilder.setIntValue(
                            contextManager
                                    .getTreeNodeLabelManager()
                                    .getLabelIndex(token.getAccessor()));
                } else {
                    String label = ((SelectOneTreeNode) unaryDedupTreeNode).getSelectLabel();
                    argumentBuilder.setIntValue(
                            contextManager.getTreeNodeLabelManager().getLabelIndex(label));
                    usedLabelList.add(
                            contextManager.getTreeNodeLabelManager().getLabelIndex(label));
                }
                ProcessorFunction dedupFunction = new ProcessorFunction(dedupType, argumentBuilder);
                LogicalVertex dedupVertex =
                        new LogicalUnaryVertex(
                                contextManager.getVertexIdManager().getId(),
                                dedupFunction,
                                isPropLocalFlag(),
                                sourceVertex);
                logicalSubQueryPlan.addLogicalVertex(dedupVertex);
                logicalSubQueryPlan.addLogicalEdge(sourceVertex, dedupVertex, new LogicalEdge());
            } else {
                TreeNode currentDedupNode =
                        TreeNodeUtils.buildSingleOutputNode(dedupTreeNode, schema);
                Pair<LogicalQueryPlan, Integer> planLabelPair =
                        TreeNodeUtils.buildSubQueryWithLabel(
                                currentDedupNode, sourceVertex, contextManager);
                logicalSubQueryPlan.mergeLogicalQueryPlan(planLabelPair.getLeft());
                LogicalVertex dedupInputVertex = logicalSubQueryPlan.getOutputVertex();

                ProcessorFunction dedupFunction =
                        new ProcessorFunction(
                                dedupType,
                                Message.Value.newBuilder().setIntValue(planLabelPair.getRight()));
                dedupFunction.getUsedLabelList().add(planLabelPair.getRight());
                LogicalVertex dedupVertex =
                        new LogicalUnaryVertex(
                                contextManager.getVertexIdManager().getId(),
                                dedupFunction,
                                isPropLocalFlag(),
                                dedupInputVertex);
                logicalSubQueryPlan.addLogicalVertex(dedupVertex);
                logicalSubQueryPlan.addLogicalEdge(
                        dedupInputVertex, dedupVertex, new LogicalEdge());
            }
        }

        LogicalVertex dedupVertex = logicalSubQueryPlan.getOutputVertex();
        addUsedLabelAndRequirement(dedupVertex, contextManager.getTreeNodeLabelManager());
        setFinishVertex(dedupVertex, contextManager.getTreeNodeLabelManager());
        return logicalSubQueryPlan;
    }

    @Override
    public ValueType getOutputValueType() {
        return getInputNode().getOutputValueType();
    }

    public void setDedupTreeNode(TreeNode dedupTreeNode) {
        this.dedupTreeNode = dedupTreeNode;
        if (dedupTreeNode instanceof UnaryTreeNode) {
            UnaryTreeNode unaryDedupNode = (UnaryTreeNode) dedupTreeNode;
            if (unaryDedupNode.getInputNode() instanceof SourceDelegateNode
                    && unaryDedupNode instanceof ElementValueTreeNode) {
                String propKey =
                        ((ElementValueTreeNode) unaryDedupNode).getPropKeyList().iterator().next();
                TreeNode inputTreeNode = getInputNode();
                if (inputTreeNode instanceof PropFillTreeNode) {
                    ((PropFillTreeNode) inputTreeNode).getPropKeyList().add(propKey);
                } else {
                    PropFillTreeNode propFillTreeNode =
                            new PropFillTreeNode(inputTreeNode, Sets.newHashSet(propKey), schema);
                    this.setInputNode(propFillTreeNode);
                }
            }
        }
    }
}

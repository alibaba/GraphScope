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

import com.alibaba.maxgraph.QueryFlowOuterClass;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.logical.edge.EdgeShuffleType;
import com.alibaba.maxgraph.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.compiler.utils.TreeNodeUtils;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.compiler.logical.LogicalBinaryVertex;
import com.alibaba.maxgraph.compiler.logical.LogicalEdge;
import com.alibaba.maxgraph.compiler.logical.LogicalSourceDelegateVertex;
import com.alibaba.maxgraph.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.compiler.logical.LogicalUnaryVertex;
import com.alibaba.maxgraph.compiler.logical.LogicalVertex;
import com.alibaba.maxgraph.compiler.logical.VertexIdManager;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorRepeatFunction;
import com.alibaba.maxgraph.compiler.tree.source.SourceDelegateNode;
import com.alibaba.maxgraph.compiler.tree.source.SourceTreeNode;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

public class RepeatTreeNode extends UnaryTreeNode {
    public static final long DEFAULT_LOOP_TIMES = 1000;
    private TreeNode repeatBodyTreeNode;
    private TreeNode untilFirstTreeNode;
    private TreeNode emitFirstTreeNode;
    private TreeNode untilTreeNode;
    private TreeNode emitTreeNode;
    private long maxLoopTimes = DEFAULT_LOOP_TIMES;

    // dfs operator relate tree node
    private TreeNode dfsEmitTreeNode;
    private DfsFinishTreeNode dfsFeedTreeNode;
    private Map<String, Object> queryConfig;

    public RepeatTreeNode(TreeNode input, GraphSchema schema, Map<String, Object> queryConfig) {
        super(input, NodeType.REPEAT, schema);
        this.untilFirstTreeNode = null;
        this.emitFirstTreeNode = null;

        this.dfsEmitTreeNode = null;
        this.dfsFeedTreeNode = null;
        this.queryConfig = queryConfig;
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        TreeNodeLabelManager labelManager = contextManager.getTreeNodeLabelManager();
        VertexIdManager vertexIdManager = contextManager.getVertexIdManager();
        LogicalSubQueryPlan logicalSubQueryPlan = new LogicalSubQueryPlan(contextManager);
        LogicalVertex delegateSourceVertex = getInputNode().getOutputVertex();
        logicalSubQueryPlan.addLogicalVertex(delegateSourceVertex);

        List<LogicalVertex> outputVertexList = Lists.newArrayList();
        LogicalVertex inputVertex = delegateSourceVertex;
        ProcessorRepeatFunction processorRepeatFunction = new ProcessorRepeatFunction();
        if (null != untilFirstTreeNode) {
            Pair<LogicalVertex, Pair<LogicalVertex, LogicalSubQueryPlan>> untilTable = buildUntilQueryPlan(untilTreeNode, delegateSourceVertex, labelManager, contextManager, vertexIdManager);
            outputVertexList.add(untilTable.getLeft());
            inputVertex = untilTable.getRight().getLeft();
            logicalSubQueryPlan.mergeLogicalQueryPlan(untilTable.getRight().getRight());
        }
        if (null != emitFirstTreeNode) {
            LogicalVertex emitVertex;
            if (emitFirstTreeNode instanceof SourceDelegateNode) {
                emitVertex = inputVertex;
            } else {
                emitVertex = buildFilterResultPlan(contextManager, vertexIdManager, labelManager, logicalSubQueryPlan, inputVertex, emitFirstTreeNode);
            }
            outputVertexList.add(emitVertex);
            processorRepeatFunction.enableEmitFlag();
        }

        checkArgument(null != repeatBodyTreeNode, "repeat body tree node can't be null");
        processorRepeatFunction.setMaxLoopTimes(maxLoopTimes);

        LogicalVertex repeatVertex = new LogicalUnaryVertex(vertexIdManager.getId(), processorRepeatFunction, false, inputVertex);
        logicalSubQueryPlan.addLogicalVertex(repeatVertex);
        logicalSubQueryPlan.addLogicalEdge(inputVertex, repeatVertex, new LogicalEdge());

        List<LogicalVertex> leaveVertexList = Lists.newArrayList();
        LogicalSubQueryPlan repeatBodyPlan = new LogicalSubQueryPlan(contextManager);
        LogicalSourceDelegateVertex repeatSourceVertex = new LogicalSourceDelegateVertex(inputVertex);
        repeatBodyPlan.addLogicalVertex(repeatSourceVertex);
        repeatBodyPlan.mergeLogicalQueryPlan(TreeNodeUtils.buildQueryPlanWithSource(repeatBodyTreeNode, labelManager, contextManager, vertexIdManager, repeatSourceVertex));

        LogicalVertex feedbackVertex = repeatBodyPlan.getOutputVertex();
        if (null != untilTreeNode) {
            Pair<LogicalVertex, Pair<LogicalVertex, LogicalSubQueryPlan>> untilTable = buildUntilQueryPlan(untilTreeNode, feedbackVertex, labelManager, contextManager, vertexIdManager);
            feedbackVertex = untilTable.getRight().getLeft();

            LogicalVertex untilVertex = untilTable.getLeft();
            untilVertex.getAfterRequirementList().add(QueryFlowOuterClass.RequirementValue.newBuilder().setReqType(QueryFlowOuterClass.RequirementType.KEY_DEL));
            leaveVertexList.add(untilVertex);
            repeatBodyPlan.mergeLogicalQueryPlan(untilTable.getRight().getRight());
        }
        if (null != emitTreeNode) {
            LogicalVertex emitVertex;
            if (emitTreeNode instanceof SourceDelegateNode) {
                emitVertex = feedbackVertex;
            } else {
                emitVertex = buildFilterResultPlan(contextManager, vertexIdManager, labelManager, repeatBodyPlan, feedbackVertex, emitTreeNode);
            }
            if (null != dfsEmitTreeNode) {
                repeatBodyPlan.mergeLogicalQueryPlan(TreeNodeUtils.buildQueryPlanWithSource(dfsEmitTreeNode, labelManager, contextManager, vertexIdManager, emitVertex));
                emitVertex = repeatBodyPlan.getOutputVertex();
            }
            leaveVertexList.add(emitVertex);
            processorRepeatFunction.enableEmitFlag();
        }
        if (null != dfsFeedTreeNode) {
            dfsFeedTreeNode.setRepeatSourceVertex(repeatBodyPlan.getTargetVertex(repeatSourceVertex));
            LogicalSubQueryPlan dfsQueryPlan = TreeNodeUtils.buildQueryPlanWithSource(dfsFeedTreeNode, labelManager, contextManager, vertexIdManager, feedbackVertex);
            feedbackVertex = dfsQueryPlan.getOutputVertex();
            repeatBodyPlan.mergeLogicalQueryPlan(dfsQueryPlan);
        }
        processorRepeatFunction.setEnterVertex(repeatBodyPlan.getTargetVertex(repeatSourceVertex));
        processorRepeatFunction.setFeedbackVertex(feedbackVertex);
        processorRepeatFunction.setRepeatPlan(repeatBodyPlan);

        if (!leaveVertexList.isEmpty()) {
            if (leaveVertexList.size() == 1) {
                processorRepeatFunction.setLeaveVertex(leaveVertexList.get(0));
            } else {
                LogicalVertex unionLeaveVertex = unionVertexList(vertexIdManager, repeatBodyPlan, leaveVertexList);
                processorRepeatFunction.setLeaveVertex(unionLeaveVertex);
            }
        }

        LogicalVertex outputVertex = repeatVertex;
        if (!outputVertexList.isEmpty()) {
            List<LogicalVertex> outputUnionVertexList = Lists.newArrayList(outputVertexList);
            outputUnionVertexList.add(repeatVertex);
            outputVertex = unionVertexList(vertexIdManager, logicalSubQueryPlan, outputUnionVertexList);
        }

        addUsedLabelAndRequirement(outputVertex, labelManager);
        setFinishVertex(outputVertex, labelManager);
        return logicalSubQueryPlan;
    }

    /**
     * Build until query plan with given tree node
     *
     * @param untilTreeNode The given until tree node
     * @param inputVertex   The input vertex of until
     * @return The table of result. The first one is until vertex, the second one is feedback vertex and the third one is result plan
     */
    private Pair<LogicalVertex, Pair<LogicalVertex, LogicalSubQueryPlan>> buildUntilQueryPlan(
            TreeNode untilTreeNode,
            LogicalVertex inputVertex,
            TreeNodeLabelManager treeNodeLabelManager,
            ContextManager contextManager,
            VertexIdManager vertexIdManager) {
        LogicalSubQueryPlan logicalSubQueryPlan = new LogicalSubQueryPlan(contextManager);
        LogicalVertex untilVertex = null, feedbackVertex = null;
        if (untilTreeNode instanceof NotTreeNode) {
            NotTreeNode notTreeNode = NotTreeNode.class.cast(untilTreeNode);
            TreeNode notFilterNode = notTreeNode.getNotTreeNode();
            TreeNode currentUntilNode = TreeNodeUtils.buildSingleOutputNode(notFilterNode, schema);
            LogicalSubQueryPlan untilPlan = TreeNodeUtils.buildSubQueryPlanWithKey(currentUntilNode, inputVertex, treeNodeLabelManager, contextManager, vertexIdManager);
            LogicalVertex enterKeyVertex = TreeNodeUtils.getSourceTreeNode(currentUntilNode).getOutputVertex();
            logicalSubQueryPlan.mergeLogicalQueryPlan(untilPlan);

            LogicalVertex feedbackKeyVertex = untilPlan.getOutputVertex();
            feedbackVertex = new LogicalUnaryVertex(
                    vertexIdManager.getId(),
                    new ProcessorFunction(QueryFlowOuterClass.OperatorType.KEY_MESSAGE),
                    true,
                    feedbackKeyVertex);
            logicalSubQueryPlan.addLogicalVertex(feedbackVertex);
            logicalSubQueryPlan.addLogicalEdge(feedbackKeyVertex, feedbackVertex, new LogicalEdge(EdgeShuffleType.FORWARD));

            untilVertex = new LogicalBinaryVertex(
                    vertexIdManager.getId(),
                    new ProcessorFunction(QueryFlowOuterClass.OperatorType.JOIN_DIRECT_FILTER_NEGATE),
                    false,
                    enterKeyVertex,
                    feedbackKeyVertex);
            logicalSubQueryPlan.addLogicalVertex(untilVertex);
            logicalSubQueryPlan.addLogicalEdge(enterKeyVertex, untilVertex, new LogicalEdge());
            logicalSubQueryPlan.addLogicalEdge(feedbackKeyVertex, untilVertex, new LogicalEdge());

        } else {
            TreeNode currentUntilNode = TreeNodeUtils.buildSingleOutputNode(untilTreeNode, schema);
            LogicalSubQueryPlan untilPlan = TreeNodeUtils.buildSubQueryPlanWithKey(currentUntilNode, inputVertex, treeNodeLabelManager, contextManager, vertexIdManager);
            LogicalVertex enterKeyVertex = TreeNodeUtils.getSourceTreeNode(currentUntilNode).getOutputVertex();
            LogicalVertex untilOutputVertex = untilPlan.getOutputVertex();
            logicalSubQueryPlan.mergeLogicalQueryPlan(untilPlan);

            feedbackVertex = new LogicalBinaryVertex(
                    vertexIdManager.getId(),
                    new ProcessorFunction(QueryFlowOuterClass.OperatorType.JOIN_DIRECT_FILTER_NEGATE),
                    false,
                    enterKeyVertex,
                    untilOutputVertex);
            logicalSubQueryPlan.addLogicalVertex(feedbackVertex);
            logicalSubQueryPlan.addLogicalEdge(enterKeyVertex, feedbackVertex, new LogicalEdge());
            logicalSubQueryPlan.addLogicalEdge(untilOutputVertex, feedbackVertex, new LogicalEdge());

            untilVertex = new LogicalUnaryVertex(
                    vertexIdManager.getId(),
                    new ProcessorFunction(QueryFlowOuterClass.OperatorType.KEY_MESSAGE),
                    true,
                    untilOutputVertex);
            logicalSubQueryPlan.addLogicalVertex(untilVertex);
            logicalSubQueryPlan.addLogicalEdge(untilOutputVertex, untilVertex, new LogicalEdge(EdgeShuffleType.FORWARD));
        }

        return Pair.of(untilVertex, Pair.of(feedbackVertex, logicalSubQueryPlan));
    }

    private LogicalVertex buildFilterResultPlan(ContextManager contextManager,
                                                VertexIdManager vertexIdManager,
                                                TreeNodeLabelManager labelManager,
                                                LogicalSubQueryPlan logicalSubQueryPlan,
                                                LogicalVertex delegateSourceVertex,
                                                TreeNode emitTreeNode) {
        TreeNode currentEmitTreeNode = TreeNodeUtils.buildSingleOutputNode(emitTreeNode, schema);
        LogicalSubQueryPlan untilPlan = TreeNodeUtils.buildSubQueryPlan(currentEmitTreeNode, delegateSourceVertex, contextManager);
        LogicalVertex enterKeyVertex = TreeNodeUtils.getSourceTreeNode(currentEmitTreeNode).getOutputVertex();

        LogicalVertex untilResultVertex = untilPlan.getOutputVertex();
        logicalSubQueryPlan.mergeLogicalQueryPlan(untilPlan);
        if (currentEmitTreeNode instanceof HasTreeNode &&
                ((HasTreeNode) currentEmitTreeNode).getInputNode() instanceof SourceDelegateNode) {
            return untilResultVertex;
        } else {
            LogicalVertex currentOutputVertex = new LogicalBinaryVertex(
                    vertexIdManager.getId(),
                    new ProcessorFunction(QueryFlowOuterClass.OperatorType.JOIN_DIRECT_FILTER),
                    false,
                    enterKeyVertex,
                    untilResultVertex);
            logicalSubQueryPlan.addLogicalVertex(currentOutputVertex);
            logicalSubQueryPlan.addLogicalEdge(enterKeyVertex, currentOutputVertex, new LogicalEdge());
            logicalSubQueryPlan.addLogicalEdge(untilResultVertex, currentOutputVertex, new LogicalEdge());
            return currentOutputVertex;
        }
    }

    @Override
    public void addPathRequirement() {
        TreeNode currTreeNode = repeatBodyTreeNode;
        while (!(currTreeNode instanceof SourceTreeNode)) {
            currTreeNode.addPathRequirement();
            currTreeNode = UnaryTreeNode.class.cast(currTreeNode).getInputNode();
        }
    }

    private LogicalVertex unionVertexList(VertexIdManager vertexIdManager, LogicalSubQueryPlan repeatBodyPlan, List<LogicalVertex> leaveVertexList) {
        LogicalVertex unionVertex = leaveVertexList.get(0);
        for (int i = 1; i < leaveVertexList.size(); i++) {
            LogicalVertex currentUnionVertex = new LogicalBinaryVertex(vertexIdManager.getId(), new ProcessorFunction(QueryFlowOuterClass.OperatorType.UNION), false, unionVertex, leaveVertexList.get(i));
            repeatBodyPlan.addLogicalVertex(currentUnionVertex);
            repeatBodyPlan.addLogicalEdge(unionVertex, currentUnionVertex, new LogicalEdge(EdgeShuffleType.FORWARD));
            repeatBodyPlan.addLogicalEdge(leaveVertexList.get(i), currentUnionVertex, new LogicalEdge(EdgeShuffleType.FORWARD));
            unionVertex = currentUnionVertex;
        }
        return unionVertex;
    }

    @Override
    public ValueType getOutputValueType() {
        return getInputNode().getOutputValueType();
    }

    /**
     * Enable global stop by global limit operator
     */
    @Override
    public void enableGlobalStop() {
        throw new IllegalArgumentException("repeat tree node can't enable global stop");
    }

    /**
     * Enable global filter by global stop
     */
    @Override
    public void enableGlobalFilter() {
        super.enableGlobalFilter();
        TreeNode currNode = repeatBodyTreeNode;
        while (currNode instanceof UnaryTreeNode) {
            currNode.enableGlobalFilter();
            currNode = ((UnaryTreeNode) currNode).getInputNode();
        }
    }

    public void setRepeatBodyTreeNode(TreeNode repeatBodyTreeNode) {
        this.repeatBodyTreeNode = repeatBodyTreeNode;
    }

    public void setUntilTreeNode(TreeNode untilTreeNode) {
        this.untilTreeNode = untilTreeNode;
    }

    public void setEmitTreeNode(TreeNode emitTreeNode) {
        this.emitTreeNode = emitTreeNode;
    }

    public void setMaxLoopTimes(long maxLoopTimes) {
        this.maxLoopTimes = maxLoopTimes;
    }

    public TreeNode getUntilFirstTreeNode() {
        return untilFirstTreeNode;
    }

    public void setUntilFirstTreeNode(TreeNode untilFirstTreeNode) {
        this.untilFirstTreeNode = untilFirstTreeNode;
    }

    public TreeNode getEmitFirstTreeNode() {
        return emitFirstTreeNode;
    }

    public void setEmitFirstTreeNode(TreeNode emitFirstTreeNode) {
        this.emitFirstTreeNode = emitFirstTreeNode;
    }

    public TreeNode getRepeatBodyTreeNode() {
        return repeatBodyTreeNode;
    }

    public void setDfsFeedTreeNode(DfsFinishTreeNode dfsFeedTreeNode) {
        this.dfsFeedTreeNode = dfsFeedTreeNode;
    }

    public void setDfsEmitTreeNode(TreeNode dfsEmitTreeNode) {
        this.dfsEmitTreeNode = dfsEmitTreeNode;
    }
}

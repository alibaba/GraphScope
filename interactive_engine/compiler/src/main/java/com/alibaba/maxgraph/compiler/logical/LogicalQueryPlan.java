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
package com.alibaba.maxgraph.compiler.logical;

import com.alibaba.maxgraph.QueryFlowOuterClass;
import com.alibaba.maxgraph.common.util.CompilerConstant;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.compiler.logical.chain.LogicalChainSourceVertex;
import com.alibaba.maxgraph.compiler.logical.edge.EdgeShuffleType;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorRepeatFunction;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorSourceFunction;
import com.alibaba.maxgraph.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.compiler.tree.TreeNodeLabelManager;
import com.alibaba.maxgraph.compiler.utils.PlanUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;

public class LogicalQueryPlan {
    private Graph<LogicalVertex, LogicalEdge> plan = new DefaultDirectedGraph<>(LogicalEdge.class);
    private ValueType resultValueType;
    private ContextManager contextManager;

    public LogicalQueryPlan(ContextManager contextManager) {
        this.contextManager = contextManager;
    }

    /**
     * Add processor vertex to logical plan
     *
     * @param processorVertex The given processor vertex
     */
    public void addLogicalVertex(LogicalVertex processorVertex) {
        checkArgument(plan.addVertex(processorVertex));
    }

    /**
     * Add edge between source and target vertex
     *
     * @param source The given source vertex
     * @param target The given target vertex
     * @param edge   The given edge
     */
    public void addLogicalEdge(LogicalVertex source, LogicalVertex target, LogicalEdge edge) {
        checkArgument(plan.addEdge(source, target, edge));
    }

    /**
     * Merger sub query plan to logical query plan
     *
     * @param queryPlan The given sub query plan
     */
    public void mergeLogicalQueryPlan(LogicalQueryPlan queryPlan) {
        List<LogicalVertex> logicalVertexList = queryPlan.getOrderVertexList();
        for (LogicalVertex logicalVertex : logicalVertexList) {
            if (!plan.containsVertex(logicalVertex)) {
                addLogicalVertex(logicalVertex);
                List<Pair<LogicalEdge, LogicalVertex>> sourceEdgeVertexList = queryPlan.getSourceEdgeVertexList(logicalVertex);
                Set<LogicalVertex> binaryParentList = Sets.newHashSet();
                for (Pair<LogicalEdge, LogicalVertex> sourceEdgeVertex : sourceEdgeVertexList) {
                    addLogicalEdge(sourceEdgeVertex.getRight(), logicalVertex, sourceEdgeVertex.getLeft());
                    binaryParentList.add(sourceEdgeVertex.getRight());
                }
                if (logicalVertex instanceof LogicalBinaryVertex && sourceEdgeVertexList.size() == 1) {
                    LogicalBinaryVertex logicalBinaryVertex = LogicalBinaryVertex.class.cast(logicalVertex);
                    LogicalVertex parentVertex = binaryParentList.contains(logicalBinaryVertex.getLeftInput()) ?
                            logicalBinaryVertex.getRightInput() : logicalBinaryVertex.getLeftInput();
                    if (existVertex(parentVertex)) {
                        if (parentVertex.isPropLocalFlag()) {
                            addLogicalEdge(parentVertex, logicalBinaryVertex, new LogicalEdge(EdgeShuffleType.FORWARD));
                        } else {
                            addLogicalEdge(parentVertex, logicalBinaryVertex, new LogicalEdge());
                        }
                    }
                }
            }
        }
    }

    private boolean existVertex(LogicalVertex vertex) {
        return plan.vertexSet().contains(vertex);
    }

    void optimizeLogicalPlan() {
        optimizeLogicalPlan(Sets.newHashSet());
    }

    private void optimizeLogicalPlan(Set<Integer> labelUsedList) {
        optimizeFoldVertex();
        optimizeGraphVertex();
        optimizeLabelDelete(labelUsedList);
        optimizeKeyDelete();
    }

    public boolean isPullGraphEnable() {
        return this.contextManager.getQueryConfig().getBoolean(CompilerConstant.QUERY_GRAPH_PULL_ENABLE, false);
    }

    private List<LogicalVertex> getSourceVertexList(LogicalVertex currentVertex) {
        return this.getSourceEdgeVertexList(currentVertex)
                .stream()
                .map(Pair::getRight)
                .collect(Collectors.toList());
    }

    private void optimizeKeyDelete() {
        List<LogicalVertex> logicalVertexList = this.getLogicalVertexList();
        for (int i = 0; i < logicalVertexList.size(); i++) {
            LogicalVertex logicalVertex = logicalVertexList.get(i);
            ProcessorFunction processorFunction = logicalVertex.getProcessorFunction();
            if (null != processorFunction) {
                if (logicalVertex instanceof LogicalBinaryVertex) {
                    LogicalBinaryVertex binaryVertex = (LogicalBinaryVertex) logicalVertex;
                    LogicalVertex leftVertex = binaryVertex.getLeftInput();
                    if (leftVertex.getProcessorFunction().getOperatorType() == QueryFlowOuterClass.OperatorType.ENTER_KEY) {
                        if (((LogicalBinaryVertex) logicalVertex).containsAfterKeyDelete()) {
                            for (int k = i - 1; k >= 0; k--) {
                                LogicalVertex currentBinaryVertex = logicalVertexList.get(k);
                                if (currentBinaryVertex.getProcessorFunction().getOperatorType() == QueryFlowOuterClass.OperatorType.ENTER_KEY) {
                                    if (currentBinaryVertex != leftVertex) {
                                        throw new IllegalArgumentException("Not support nest enter key");
                                    }
                                    break;
                                }
                                if (currentBinaryVertex instanceof LogicalBinaryVertex) {
                                    ((LogicalBinaryVertex) currentBinaryVertex).removeAfterKeyDelete();
                                }
                            }
                        }
                    } else {
                        binaryVertex.removeAfterKeyDelete();
                    }
                } else {
                    QueryFlowOuterClass.OperatorType operatorType = logicalVertex.getProcessorFunction().getOperatorType();
                    if (operatorType == QueryFlowOuterClass.OperatorType.KEY_MESSAGE ||
                            operatorType == QueryFlowOuterClass.OperatorType.BYKEY_ENTRY) {
                        LogicalVertex currentKeyVertex = logicalVertex;
                        while (true) {
                            LogicalVertex currentSourceVertex = getSourceVertex(currentKeyVertex);
                            if (currentSourceVertex instanceof LogicalSourceVertex) {
                                break;
                            } else if (currentSourceVertex instanceof LogicalBinaryVertex) {
                                ((LogicalBinaryVertex) currentSourceVertex).removeAfterKeyDelete();
                                break;
                            } else {
                                currentKeyVertex = currentSourceVertex;
                            }
                        }
                    }
                }
            }

        }
    }

    private void optimizeGraphVertex() {
        List<LogicalVertex> logicalVertexList = this.getLogicalVertexList();
        LogicalVertex graphVertex = null;
        for (LogicalVertex logicalVertex : logicalVertexList) {
            if (logicalVertex.getProcessorFunction().getOperatorType() == QueryFlowOuterClass.OperatorType.SUBGRAPH) {
                graphVertex = logicalVertex;
                break;
            }
        }
        if (null != graphVertex) {
            List<Pair<LogicalEdge, LogicalVertex>> graphSourceVertexList = this.getSourceEdgeVertexList(graphVertex);
            List<Pair<LogicalEdge, LogicalVertex>> graphTargetVertexList = this.getTargetEdgeVertexList(graphVertex);
            if (graphTargetVertexList != null && graphTargetVertexList.size() == 1) {
                LogicalVertex targetVertex = graphTargetVertexList.get(0).getRight();
                if (targetVertex.getProcessorFunction().getOperatorType() == QueryFlowOuterClass.OperatorType.OUTPUT_VINEYARD_VERTEX) {
                    return;
                }
            }
            if (graphSourceVertexList.size() == 1) {
                LogicalVertex graphSourceVertex = graphSourceVertexList.get(0).getRight();
                if (graphSourceVertex.getProcessorFunction() instanceof ProcessorSourceFunction) {
                    ProcessorSourceFunction processorSourceFunction = ProcessorSourceFunction.class.cast(graphSourceVertex.getProcessorFunction());
                    processorSourceFunction.resetOperatorType(QueryFlowOuterClass.OperatorType.SUBGRAPH_SOURCE);
                    if (graphVertex.getProcessorFunction().getArgumentBuilder() != null) {
                        processorSourceFunction.getArgumentBuilder().mergeFrom(graphVertex.getProcessorFunction().getArgumentBuilder().build());
                    }
                    this.removeVertex(graphVertex);
                }
            }

        }
    }

    private void optimizeLabelDelete(Set<Integer> labelUsedList) {
        List<LogicalVertex> logicalVertexList = getLogicalVertexList();
        for (int i = logicalVertexList.size() - 1; i >= 0; i--) {
            LogicalVertex logicalVertex = logicalVertexList.get(i);
            if (logicalVertex instanceof LogicalSourceDelegateVertex) {
                continue;
            }
            if (logicalVertex.getProcessorFunction() instanceof ProcessorRepeatFunction) {
                ProcessorRepeatFunction processorRepeatFunction = ProcessorRepeatFunction.class.cast(logicalVertex.getProcessorFunction());
                processorRepeatFunction.getRepeatPlan().optimizeLogicalPlan(labelUsedList);
                continue;
            }
            Set<Integer> removeLabelList = Sets.newHashSet();
            for (Integer labelId : logicalVertex.getProcessorFunction().getFunctionUsedLabelList()) {
                if (!labelUsedList.contains(labelId)) {
                    removeLabelList.add(labelId);
                    labelUsedList.add(labelId);
                }
            }
            if (!removeLabelList.isEmpty()) {
                QueryFlowOuterClass.RequirementValue.Builder removeRequirementBuilder = null;
                for (QueryFlowOuterClass.RequirementValue.Builder reqBuilder : logicalVertex.getAfterRequirementList()) {
                    if (reqBuilder.getReqType() == QueryFlowOuterClass.RequirementType.LABEL_DEL) {
                        removeRequirementBuilder = reqBuilder;
                        break;
                    }
                }
                if (null == removeRequirementBuilder) {
                    removeRequirementBuilder = QueryFlowOuterClass.RequirementValue.newBuilder().setReqType(QueryFlowOuterClass.RequirementType.LABEL_DEL);
                    logicalVertex.getAfterRequirementList().add(removeRequirementBuilder);
                }
                removeRequirementBuilder.getReqArgumentBuilder().addAllIntValueList(removeLabelList);
            }

            Iterator<QueryFlowOuterClass.RequirementValue.Builder> reqValueIterator = logicalVertex.getAfterRequirementList().iterator();
            while (reqValueIterator.hasNext()) {
                QueryFlowOuterClass.RequirementValue.Builder reqBuilder = reqValueIterator.next();
                if (reqBuilder.getReqType() == QueryFlowOuterClass.RequirementType.LABEL_START) {
                    List<Integer> startLabelList = reqBuilder.getReqArgumentBuilder()
                            .getIntValueListList()
                            .stream()
                            .filter(labelUsedList::contains)
                            .collect(Collectors.toList());
                    if (startLabelList.isEmpty()) {
                        reqValueIterator.remove();
                    } else {
                        reqBuilder.getReqArgumentBuilder().clear();
                        reqBuilder.getReqArgumentBuilder().addAllIntValueList(startLabelList);
                    }
                    break;
                }
            }
            reqValueIterator = logicalVertex.getBeforeRequirementList().iterator();
            while (reqValueIterator.hasNext()) {
                QueryFlowOuterClass.RequirementValue.Builder reqBuilder = reqValueIterator.next();
                if (reqBuilder.getReqType() == QueryFlowOuterClass.RequirementType.LABEL_START) {
                    List<Integer> startLabelList = reqBuilder.getReqArgumentBuilder()
                            .getIntValueListList()
                            .stream()
                            .filter(labelUsedList::contains)
                            .collect(Collectors.toList());
                    if (startLabelList.isEmpty()) {
                        reqValueIterator.remove();
                    } else {
                        reqBuilder.getReqArgumentBuilder().clear();
                        reqBuilder.getReqArgumentBuilder().addAllIntValueList(startLabelList);
                    }
                    break;
                }
            }
        }
    }

    private void optimizeFoldVertex() {
        List<LogicalVertex> logicalVertexList = getLogicalVertexList();
        Set<LogicalVertex> removeVertexList = Sets.newHashSet();
        for (LogicalVertex logicalVertex : logicalVertexList) {
            if (logicalVertex instanceof LogicalSourceDelegateVertex) {
                continue;
            }
            if (logicalVertex.getProcessorFunction().getOperatorType() == QueryFlowOuterClass.OperatorType.FOLD
                    || logicalVertex.getProcessorFunction().getOperatorType() == QueryFlowOuterClass.OperatorType.FOLD_BY_KEY
                    || logicalVertex.getProcessorFunction().getOperatorType() == QueryFlowOuterClass.OperatorType.FOLDMAP) {
                if (logicalVertex.getAfterRequirementList().isEmpty()) {
                    List<LogicalVertex> targetVertexList = getTargetVertexList(logicalVertex);
                    if (targetVertexList.size() == 1) {
                        LogicalVertex targetVertex = targetVertexList.get(0);
                        if (targetVertex.getBeforeRequirementList().isEmpty()) {
                            ProcessorFunction targetFunction = targetVertex.getProcessorFunction();
                            if (targetFunction.getOperatorType() == QueryFlowOuterClass.OperatorType.UNFOLD) {
                                LogicalVertex inputVertex = getSourceVertex(logicalVertex);
                                List<LogicalVertex> targetOutputList = getTargetVertexList(targetVertex);
                                if (targetOutputList.size() <= 1) {
                                    inputVertex.getAfterRequirementList().addAll(logicalVertex.getBeforeRequirementList());
                                    if (targetOutputList.size() == 1) {
                                        LogicalVertex targetOutputVertex = targetOutputList.get(0);
                                        targetOutputVertex.getBeforeRequirementList().addAll(targetVertex.getAfterRequirementList());
                                    }
                                    removeVertexList.add(logicalVertex);
                                    removeVertexList.add(targetVertex);
                                }
                            }
                        }
                    }
                }
            }
        }
        for (LogicalVertex removeVertex : removeVertexList) {
            removeVertex(removeVertex);
        }
    }

    public void removeVertex(LogicalVertex vertex) {
        List<Pair<LogicalEdge, LogicalVertex>> sourcePairList = getSourceEdgeVertexList(vertex);
        List<Pair<LogicalEdge, LogicalVertex>> targetPairList = getTargetEdgeVertexList(vertex);

        plan.removeVertex(vertex);
        if (sourcePairList.isEmpty()) {
            return;
        }

        checkArgument(sourcePairList.size() == 1, "source of fold vertex must be 1");
        Pair<LogicalEdge, LogicalVertex> sourcePair = sourcePairList.get(0);
        for (Pair<LogicalEdge, LogicalVertex> targetPair : targetPairList) {
            targetPair.getRight().resetInputVertex(vertex, sourcePair.getRight());
            plan.addEdge(sourcePair.getRight(), targetPair.getRight(), targetPair.getLeft());
        }
    }

    public void removeEdge(LogicalEdge edge) {
        plan.removeEdge(edge);
    }

    private List<LogicalVertex> getTargetVertexList(LogicalVertex logicalVertex) {
        List<LogicalVertex> targetVertexList = Lists.newArrayList();
        for (Pair<LogicalEdge, LogicalVertex> targetPair : getTargetEdgeVertexList(logicalVertex)) {
            targetVertexList.add(targetPair.getRight());
        }
        return targetVertexList;
    }

    List<Pair<LogicalEdge, LogicalVertex>> getTargetEdgeVertexList(LogicalVertex logicalVertex) {
        return PlanUtils.getTargetEdgeVertexList(plan, logicalVertex);
    }

    List<Pair<LogicalEdge, LogicalVertex>> getSourceEdgeVertexList(LogicalVertex logicalVertex) {
        return PlanUtils.getSourceEdgeVertexList(plan, logicalVertex);
    }

    List<LogicalVertex> getOrderVertexList() {
        return PlanUtils.getOrderVertexList(plan);
    }

    public LogicalVertex getOutputVertex() {
        return PlanUtils.getOutputVertex(plan);
    }

    public LogicalVertex getSourceVertex() {
        return PlanUtils.getSourceVertex(plan);
    }

    public LogicalVertex getSourceVertex(LogicalVertex vertex) {
        return PlanUtils.getSourceEdgeVertexList(plan, vertex).get(0).getRight();
    }

    public TreeNodeLabelManager getLabelManager() {
        return this.contextManager.getTreeNodeLabelManager();
    }

    public ValueType getResultValueType() {
        return resultValueType;
    }

    public void setResultValueType(ValueType resultValueType) {
        this.resultValueType = resultValueType;
    }

    public List<LogicalVertex> getLogicalVertexList() {
        return PlanUtils.getOrderVertexList(plan);
    }

    /**
     * Chain the operator, here we chain the g.V().outE().inV() source operator first
     *
     * @return The chained logical query plan
     */
    public LogicalQueryPlan chainOptimize() {
        if (this.isPullGraphEnable()) {
            List<LogicalVertex> orderVertexList = PlanUtils.getOrderVertexList(plan);
            boolean outVertexFlag = false;
            for (LogicalVertex logicalVertex : orderVertexList) {
                if (logicalVertex.getProcessorFunction() != null) {
                    QueryFlowOuterClass.OperatorType operatorType = logicalVertex.getProcessorFunction().getOperatorType();
                    if (QueryFlowOuterClass.OperatorType.OUT == operatorType ||
                            QueryFlowOuterClass.OperatorType.OUT_E == operatorType ||
                            QueryFlowOuterClass.OperatorType.IN == operatorType ||
                            QueryFlowOuterClass.OperatorType.IN_E == operatorType ||
                            QueryFlowOuterClass.OperatorType.BOTH == operatorType ||
                            QueryFlowOuterClass.OperatorType.BOTH_E == operatorType) {
                        if (!outVertexFlag &&
                                (QueryFlowOuterClass.OperatorType.OUT == operatorType ||
                                        QueryFlowOuterClass.OperatorType.OUT_E == operatorType)) {
                            outVertexFlag = true;
                            continue;
                        }
                        logicalVertex.getProcessorFunction()
                                .getArgumentBuilder()
                                .setExecLocalDisable(true);
                        break;
                    }
                }
            }
        }

        boolean chainFlag = true;
        while (chainFlag) {
            chainFlag = false;
            List<LogicalVertex> orderVertexList = PlanUtils.getOrderVertexList(plan);
            for (LogicalVertex logicalVertex : orderVertexList) {
                List<LogicalVertex> targetVertexList = PlanUtils.getTargetVertexList(plan, logicalVertex);
                if (targetVertexList.size() == 1) {
                    LogicalVertex targetVertex = targetVertexList.get(0);
                    if (checkVertexChain(logicalVertex, targetVertex)) {
                        LogicalChainSourceVertex logicalChainSourceVertex;
                        if (logicalVertex instanceof LogicalSourceVertex) {
                            logicalChainSourceVertex = new LogicalChainSourceVertex((LogicalSourceVertex) logicalVertex);
                            removeVertex(logicalVertex);
                            addLogicalVertex(logicalChainSourceVertex);
                            logicalChainSourceVertex.addLogicalVertex(targetVertex);

                            List<Pair<LogicalEdge, LogicalVertex>> targetEdgeVertexList = PlanUtils.getTargetEdgeVertexList(plan, targetVertex);
                            removeVertex(targetVertex);
                            for (Pair<LogicalEdge, LogicalVertex> targetPair : targetEdgeVertexList) {
                                addLogicalEdge(logicalChainSourceVertex, targetPair.getRight(), targetPair.getLeft());
                                targetPair.getRight().resetInputVertex(targetVertex, logicalChainSourceVertex);
                            }
                        } else if (logicalVertex instanceof LogicalChainSourceVertex) {
                            logicalChainSourceVertex = LogicalChainSourceVertex.class.cast(logicalVertex);
                            logicalChainSourceVertex.addLogicalVertex(targetVertex);
                            removeVertex(targetVertex);
                        } else {
                            break;
                        }
                        chainFlag = true;
                        break;
                    }
                } else {
                    break;
                }
            }
        }

        return this;
    }

    private boolean checkVertexChain(LogicalVertex sourceVertex, LogicalVertex targetVertex) {
        ProcessorFunction targetFunction = targetVertex.getProcessorFunction();
        QueryFlowOuterClass.OperatorType targetOperatorType = targetFunction.getOperatorType();
        if (sourceVertex instanceof LogicalSourceVertex) {
            if ((targetOperatorType == QueryFlowOuterClass.OperatorType.OUT ||
                    targetOperatorType == QueryFlowOuterClass.OperatorType.OUT_E) &&
                    targetVertex.getAfterRequirementList().isEmpty() &&
                    targetVertex.getBeforeRequirementList().isEmpty() &&
                    targetVertex.getProcessorFunction().getRangeLimit() == null &&
                    sourceVertex.getAfterRequirementList().isEmpty() &&
                    sourceVertex.getBeforeRequirementList().isEmpty() &&
                    sourceVertex.getProcessorFunction().getRangeLimit() == null &&
                    ((ProcessorSourceFunction) sourceVertex.getProcessorFunction()).getOdpsQueryInput() == null) {
                return true;
            }
        } else if (sourceVertex instanceof LogicalChainSourceVertex) {
            if ((targetOperatorType == QueryFlowOuterClass.OperatorType.IN_V ||
                    targetOperatorType == QueryFlowOuterClass.OperatorType.OUT_V ||
                    targetOperatorType == QueryFlowOuterClass.OperatorType.BOTH_V ||
                    targetOperatorType == QueryFlowOuterClass.OperatorType.OTHER_V) &&
                    targetVertex.getBeforeRequirementList().isEmpty() &&
                    targetVertex.getAfterRequirementList().isEmpty() &&
                    targetVertex.getProcessorFunction().getRangeLimit() == null) {
                return true;
            }
        }
        return false;
    }

    public Pair<LogicalVertex, LogicalVertex> getOutputVertexPair() {
        List<LogicalVertex> logicalVertexList = PlanUtils.getOutputVertexList(plan);
        checkArgument(logicalVertexList.size() == 2, "There should be two output vertex while current is " + logicalVertexList);

        return Pair.of(logicalVertexList.get(0), logicalVertexList.get(1));
    }

    public LogicalVertex getTargetVertex(LogicalVertex vertex) {
        return PlanUtils.getTargetVertexList(plan, vertex).get(0);
    }

    public LogicalEdge getTargetEdge(LogicalVertex vertex) {
        List<Pair<LogicalEdge, LogicalVertex>> targetEdgeVertexList = PlanUtils.getTargetEdgeVertexList(this.plan, vertex);
        checkArgument(targetEdgeVertexList.size() == 1, "Must only one target");
        return targetEdgeVertexList.get(0).getLeft();
    }

    public LogicalEdge getLogicalEdge(LogicalVertex sourceVertex, LogicalVertex targetVertex) {
        return plan.getEdge(sourceVertex, targetVertex);
    }

    public List<LogicalVertex> getOutputVertexList() {
        return PlanUtils.getOutputVertexList(plan);
    }

    public boolean isDebugLogEnable() {
        Configuration configuration = this.contextManager.getQueryConfig();
        try {
            return configuration.getBoolean(CompilerConstant.QUERY_DEBUG_LOG_ENABLE, false);
        } catch (Exception e) {
            throw new IllegalArgumentException("use g.enableDebugLog() or g.config(\"query.debug.log.enable\", true) to open debug log");
        }
    }

    public long getTimeoutMilliSec() {
        Configuration configuration = this.contextManager.getQueryConfig();
        try {
            return configuration.getLong(CompilerConstant.QUERY_TIMEOUT_MILLISEC, -1);
        } catch (Exception e) {
            throw new IllegalArgumentException("use g.timeout(milliSec) or g.timeoutSec(sec) or g.config(\"query.timeout.millsec\", milliSec) to set timeout");
        }
    }

    public QueryFlowOuterClass.InputBatchLevel getInputBatchLavel() {
        Configuration configuration = this.contextManager.getQueryConfig();
        String batchLavelValue = configuration.getString(CompilerConstant.QUERY_SCHEDULE_GRANULARITY, null);
        try {
            if (StringUtils.isEmpty(batchLavelValue)) {
                return QueryFlowOuterClass.InputBatchLevel.Medium;
            } else {
                return QueryFlowOuterClass.InputBatchLevel.valueOf(batchLavelValue);
            }

        } catch (Exception e) {
            throw new IllegalArgumentException("use g.scheduleVerySmall()/Small()/Medium()/Large()/VeryLarge() to set scheduler batch lavel");
        }
    }

    public boolean getEarlyStopEnable() {
        Configuration configuration = this.contextManager.getQueryConfig();
        try {
            return configuration.getBoolean(CompilerConstant.QUERY_EARLY_STOP_ENABLE, false);
        } catch (Exception e) {
            throw new IllegalArgumentException("use g.earlyStop() to open early stop timeout");
        }
    }

    public boolean isSnapshotDisable() {
        Configuration configuration = this.contextManager.getQueryConfig();
        try {
            return configuration.getBoolean(CompilerConstant.QUERY_DISABLE_SNAPSHOT, false);
        } catch (Exception e) {
            throw new IllegalArgumentException("use g.earlyStop() to open early stop timeout");
        }
    }

    public VertexIdManager getPlanVertexIdManager() {
        return this.contextManager.getVertexIdManager();
    }

    @Override
    public String toString() {
        List<LogicalVertex> vertexList = this.getLogicalVertexList();
        StringBuilder builder = new StringBuilder("digraph G {\n");
        for (LogicalVertex vertex : vertexList) {
            List<Pair<LogicalEdge, LogicalVertex>> targetPairList = getTargetEdgeVertexList(vertex);
            if (null != targetPairList) {
                for (Pair<LogicalEdge, LogicalVertex> targetPair : targetPairList) {
                    LogicalVertex targetVertex = targetPair.getValue();
                    builder.append(vertex.toString())
                            .append(" -> ")
                            .append(targetVertex.toString())
                            .append(";\n");
                }
            }
        }
        builder.append("}");
        return builder.toString();
    }
}

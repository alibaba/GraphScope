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
package com.alibaba.maxgraph.compiler.optimizer;

import com.alibaba.maxgraph.Message;
import com.alibaba.maxgraph.QueryFlowOuterClass.*;
import com.alibaba.maxgraph.Message.*;
import com.alibaba.maxgraph.compiler.logical.LogicalEdge;
import com.alibaba.maxgraph.compiler.logical.VertexIdManager;
import com.alibaba.maxgraph.compiler.logical.chain.LogicalChainUnaryVertex;
import com.alibaba.maxgraph.compiler.logical.edge.EdgeShuffleType;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorLabelValueFunction;
import com.alibaba.maxgraph.compiler.tree.TreeConstants;
import com.alibaba.maxgraph.compiler.utils.CompilerUtils;
import com.alibaba.maxgraph.compiler.executor.ExecuteParam;
import com.alibaba.maxgraph.compiler.logical.LogicalBinaryVertex;
import com.alibaba.maxgraph.compiler.logical.LogicalQueryPlan;
import com.alibaba.maxgraph.compiler.logical.LogicalSourceDelegateVertex;
import com.alibaba.maxgraph.compiler.logical.LogicalSourceVertex;
import com.alibaba.maxgraph.compiler.logical.LogicalUnaryVertex;
import com.alibaba.maxgraph.compiler.logical.LogicalVertex;
import com.alibaba.maxgraph.compiler.logical.chain.LogicalChainSourceVertex;
import com.alibaba.maxgraph.compiler.logical.chain.ProcessorChainFunction;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorRepeatFunction;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorSourceFunction;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.TextFormat;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Build query flow from plan
 */
public class QueryFlowBuilder {
    private static final Logger logger = LoggerFactory.getLogger(QueryFlowBuilder.class);

    /**
     * Build logical query plan to query flow
     *
     * @param queryPlan The given plan
     * @return The result query flow
     */
    public QueryFlow.Builder buildQueryFlow(LogicalQueryPlan queryPlan, long snapshot) {
        QueryFlowManager queryFlowManager = prepareQueryFlow(queryPlan, snapshot);
        if (!queryFlowManager.checkValidPrepareFlow()) {
            return queryFlowManager.getQueryFlow();
        } else {
            throw new IllegalArgumentException("PREPARE query flow can't be execute directly");
        }
    }

    /**
     * PREPARE logical query plan to query flow
     *
     * @param queryPlan The given query plan
     * @return The result query flow
     */
    public QueryFlowManager prepareQueryFlow(LogicalQueryPlan queryPlan, long snapshot) {
        QueryFlow.Builder flowBuilder = QueryFlow.newBuilder();
        long currentSnapshotId = snapshot;
        if (queryPlan.isSnapshotDisable()) {
            currentSnapshotId = Long.MAX_VALUE;
        }
        flowBuilder.setSnapshot(currentSnapshotId);

        QueryPlan.Builder queryPlanBuilder = QueryPlan.newBuilder();
        buildQueryFlow(queryPlanBuilder, queryPlan, currentSnapshotId);
        flowBuilder.setQueryPlan(queryPlanBuilder)
                .setExecLocalFlag(queryPlan.isPullGraphEnable());

        flowBuilder.setDebugLogFlag(queryPlan.isDebugLogEnable())
                .setInputBatchLevel(queryPlan.getInputBatchLavel());
        long timeoutMilliSec = queryPlan.getTimeoutMilliSec();
        if (timeoutMilliSec > 0) {
            flowBuilder.setTimeoutMs(timeoutMilliSec);
        }

        return new QueryFlowManager(flowBuilder, null, queryPlan.getLabelManager(), queryPlan.getResultValueType());
    }

    private int parseInputOperatorId(int id, OperatorType operatorType, Map<Integer, Pair<Integer, Integer>> dfsGraphVertexMapping) {
        if (dfsGraphVertexMapping.containsKey(id)) {
            if (operatorType == OperatorType.DFS_FINISH_JOIN) {
                return dfsGraphVertexMapping.get(id).getLeft();
            } else {
                return dfsGraphVertexMapping.get(id).getRight();
            }
        }

        return id;
    }

    private void buildQueryFlow(QueryPlan.Builder queryPlanBuilder, LogicalQueryPlan queryPlan, long snapshot) {
        List<LogicalVertex> logicalVertexList = queryPlan.getLogicalVertexList();
        Map<Integer, Pair<Integer, Integer>> dfsGraphVertexMapping = Maps.newHashMap();
        for (LogicalVertex logicalVertex : logicalVertexList) {
            ProcessorFunction processorFunction = logicalVertex.getProcessorFunction();
            if (processorFunction instanceof ProcessorRepeatFunction) {
                ProcessorRepeatFunction processorRepeatFunction = ProcessorRepeatFunction.class.cast(processorFunction);
                QueryFlowBuilder repeatBuilder = new QueryFlowBuilder();
                QueryFlow queryFlow = repeatBuilder.buildQueryFlow(processorRepeatFunction.getRepeatPlan(), snapshot).build();
                RepeatArgumentProto.Builder repeatArgument = RepeatArgumentProto.newBuilder()
                        .setPlan(queryFlow.getQueryPlan())
                        .setLoopLimit((int) processorRepeatFunction.getMaxLoopTimes())
                        .setFeedbackId(processorRepeatFunction.getFeedbackVertex().getId())
                        .setEmitFlag(processorRepeatFunction.isHasEmitFlag());
                if (processorRepeatFunction.getLeaveVertex() != null) {
                    repeatArgument.setLeaveId(processorRepeatFunction.getLeaveVertex().getId());
                } else {
                    repeatArgument.setLeaveId(-1);
                }
                logger.info("repeat argument => ");
                logger.info(TextFormat.printToString(repeatArgument));

                OperatorBase.Builder repeatBase = OperatorBase.newBuilder()
                        .setId(logicalVertex.getId())
                        .setOperatorType(OperatorType.REPEAT)
                        .setArgument(Value.newBuilder().setPayload(
                                repeatArgument.build().toByteString()));
                queryPlanBuilder.addUnaryOp(UnaryOperator.newBuilder().setBase(repeatBase)
                        .setInputOperatorId(queryPlan.getSourceVertex(logicalVertex).getId()));
                queryPlanBuilder.addOperatorIdList(logicalVertex.getId());
            } else if (processorFunction instanceof ProcessorChainFunction) {
                OperatorBase.Builder baseBuilder = OperatorBase.newBuilder();
                baseBuilder.setOperatorType(processorFunction.getOperatorType())
                        .setId(logicalVertex.getId());
                if (null != logicalVertex.getEarlyStopArgument()) {
                    baseBuilder.setEarlyStopArgument(logicalVertex.getEarlyStopArgument().build());
                }
                ProcessorChainFunction processorChainFunction = ProcessorChainFunction.class.cast(processorFunction);
                processorChainFunction.getChainVertexList()
                        .forEach(v -> baseBuilder.addChainedFunction(createChainedFunction(v)));

                if (logicalVertex instanceof LogicalChainSourceVertex) {
                    LogicalSourceVertex logicalSourceVertex = ((LogicalChainSourceVertex) logicalVertex).getLogicalSourceVertex();
                    baseBuilder.setArgument(logicalSourceVertex.getProcessorFunction().getArgumentBuilder())
                            .addAllLogicalCompare(logicalSourceVertex.getProcessorFunction().getLogicalCompareList());
                    SourceOperator.Builder sourceBuilder = SourceOperator.newBuilder()
                            .setBase(baseBuilder);
                    queryPlanBuilder.setSourceOp(sourceBuilder);
                } else {
                    LogicalChainUnaryVertex logicalUnaryVertex = (LogicalChainUnaryVertex) logicalVertex;
                    UnaryOperator.Builder unaryOperator = UnaryOperator.newBuilder()
                            .setBase(baseBuilder)
                            .setInputOperatorId(parseInputOperatorId(queryPlan.getSourceVertex(logicalUnaryVertex).getId(),
                                    processorFunction.getOperatorType(),
                                    dfsGraphVertexMapping));
                    LogicalEdge logicalEdge = queryPlan.getLogicalEdge(queryPlan.getSourceVertex(logicalUnaryVertex), logicalUnaryVertex);
                    unaryOperator.setShuffleType(CompilerUtils.parseShuffleTypeFromEdge(logicalEdge)).setInputStreamIndex(logicalEdge.getStreamIndex());
                    if (logicalEdge.getShuffleType() == EdgeShuffleType.SHUFFLE_BY_KEY) {
                        InputEdgeShuffle.Builder shuffleBuilder = InputEdgeShuffle.newBuilder()
                                .setShuffleType(InputShuffleType.SHUFFLE_BY_ID_TYPE);
                        if (logicalEdge.getShufflePropId() != 0) {
                            shuffleBuilder.setShuffleValue(logicalEdge.getShufflePropId());
                        }
                        unaryOperator.setInputShuffle(shuffleBuilder);
                    }
                    queryPlanBuilder.addUnaryOp(unaryOperator);
                }

                queryPlanBuilder.addOperatorIdList(logicalVertex.getId());
            } else if (processorFunction instanceof ProcessorLabelValueFunction) {
                ProcessorLabelValueFunction processorLabelValueFunction = (ProcessorLabelValueFunction) processorFunction;
                LogicalVertex labelValueVertex = processorLabelValueFunction.getLabelValueVertex();
                OperatorBase.Builder labelOperatorBuilder = createChainedFunction(labelValueVertex);
                int labelId = processorLabelValueFunction.getLabelId();

                OperatorBase.Builder baseBuilder = OperatorBase.newBuilder()
                        .setOperatorType(processorFunction.getOperatorType())
                        .setId(logicalVertex.getId())
                        .setArgument(Value.newBuilder()
                                .setIntValue(labelId)
                                .setBoolValue(processorLabelValueFunction.getRequireLabelFlag())
                                .setPayload(labelOperatorBuilder.build().toByteString()));

                LogicalVertex inputVertex = queryPlan.getSourceVertex(logicalVertex);
                UnaryOperator.Builder unaryOperator = UnaryOperator.newBuilder()
                        .setBase(baseBuilder)
                        .setInputOperatorId(parseInputOperatorId(inputVertex.getId(),
                                processorFunction.getOperatorType(),
                                dfsGraphVertexMapping));
                LogicalEdge logicalEdge = queryPlan.getLogicalEdge(inputVertex, logicalVertex);
                unaryOperator.setShuffleType(CompilerUtils.parseShuffleTypeFromEdge(logicalEdge)).setInputStreamIndex(logicalEdge.getStreamIndex());
                queryPlanBuilder.addUnaryOp(unaryOperator)
                        .addOperatorIdList(logicalVertex.getId());
            } else {
                if (logicalVertex instanceof LogicalSourceDelegateVertex) {
                    continue;
                }
                OperatorBase.Builder baseBuilder = OperatorBase.newBuilder();
                baseBuilder.setOperatorType(processorFunction.getOperatorType())
                        .setId(logicalVertex.getId());

                if (processorFunction.getArgumentBuilder() != null) {
                    baseBuilder.setArgument(processorFunction.getArgumentBuilder());
                }
                processorFunction.getLogicalCompareList().forEach(baseBuilder::addLogicalCompare);
                if (processorFunction.getRangeLimit() != null) {
                    baseBuilder.setRangeLimit(processorFunction.getRangeLimit());
                }
                baseBuilder.addAllAfterRequirement(logicalVertex.getAfterRequirementList().stream().map(v -> v.build()).collect(Collectors.toList()));
                baseBuilder.addAllBeforeRequirement(logicalVertex.getBeforeRequirementList().stream().map(v -> v.build()).collect(Collectors.toList()));

                if (logicalVertex instanceof LogicalSourceVertex) {
                    ProcessorSourceFunction processorSourceFunction = (ProcessorSourceFunction) logicalVertex.getProcessorFunction();
                    SourceOperator.Builder sourceBuilder = SourceOperator.newBuilder()
                            .setBase(baseBuilder);
                    if (null != processorSourceFunction.getOdpsQueryInput()) {
                        sourceBuilder.setOdpsInput(processorSourceFunction.getOdpsQueryInput())
                                .setSourceType(SourceType.ODPS);
                    } else {
                        sourceBuilder.setSourceType(SourceType.GRAPH);
                    }
                    queryPlanBuilder.setSourceOp(sourceBuilder);
                } else if (logicalVertex instanceof LogicalUnaryVertex) {
                    EarlyStopArgument.Builder earlyStopArgument = logicalVertex.getEarlyStopArgument();
                    if (null != earlyStopArgument &&
                            (earlyStopArgument.getGlobalFilterFlag() ||
                                    earlyStopArgument.getGlobalStopFlag())) {
                        baseBuilder.setEarlyStopArgument(logicalVertex.getEarlyStopArgument().build());
                    }
                    LogicalUnaryVertex logicalUnaryVertex = LogicalUnaryVertex.class.cast(logicalVertex);
                    UnaryOperator.Builder unaryOperator = UnaryOperator.newBuilder()
                            .setBase(baseBuilder)
                            .setInputOperatorId(parseInputOperatorId(queryPlan.getSourceVertex(logicalUnaryVertex).getId(),
                                    processorFunction.getOperatorType(),
                                    dfsGraphVertexMapping));
                    LogicalEdge logicalEdge = queryPlan.getLogicalEdge(queryPlan.getSourceVertex(logicalUnaryVertex), logicalUnaryVertex);
                    unaryOperator.setShuffleType(CompilerUtils.parseShuffleTypeFromEdge(logicalEdge)).setInputStreamIndex(logicalEdge.getStreamIndex());
                    queryPlanBuilder.addUnaryOp(unaryOperator);

                    if (processorFunction.getOperatorType() == OperatorType.DFS_REPEAT_GRAPH) {
                        VertexIdManager vertexIdManager = queryPlan.getPlanVertexIdManager();
                        int cmdLeftId = vertexIdManager.getId();
                        int dataRightId = vertexIdManager.getId();
                        dfsGraphVertexMapping.put(baseBuilder.getId(), Pair.of(cmdLeftId, dataRightId));

                        UnaryOperator.Builder cmdLeftOp = UnaryOperator.newBuilder()
                                .setBase(OperatorBase.newBuilder()
                                        .setOperatorType(OperatorType.DFS_REPEAT_CMD)
                                        .setId(cmdLeftId))
                                .setInputOperatorId(baseBuilder.getId())
                                .setShuffleType(InputShuffleType.FORWARD_TYPE);
                        queryPlanBuilder.addUnaryOp(cmdLeftOp);

                        UnaryOperator.Builder dataRightOp = UnaryOperator.newBuilder()
                                .setBase(OperatorBase.newBuilder()
                                        .setOperatorType(OperatorType.DFS_REPEAT_DATA)
                                        .setId(dataRightId))
                                .setInputOperatorId(baseBuilder.getId())
                                .setShuffleType(InputShuffleType.FORWARD_TYPE);
                        queryPlanBuilder.addUnaryOp(dataRightOp);
                    }
                } else if (logicalVertex instanceof LogicalBinaryVertex) {
                    LogicalBinaryVertex logicalBinaryVertex = LogicalBinaryVertex.class.cast(logicalVertex);
                    BinaryOperator.Builder binaryOperator = BinaryOperator.newBuilder()
                            .setBase(baseBuilder)
                            .setLeftInputOperatorId(parseInputOperatorId(logicalBinaryVertex.getLeftInput().getId(),
                                    processorFunction.getOperatorType(),
                                    dfsGraphVertexMapping))
                            .setRightInputOperatorId(parseInputOperatorId(logicalBinaryVertex.getRightInput().getId(),
                                    processorFunction.getOperatorType(),
                                    dfsGraphVertexMapping));
                    LogicalEdge leftEdge = queryPlan.getLogicalEdge(logicalBinaryVertex.getLeftInput(), logicalBinaryVertex);
                    LogicalEdge rightEdge = queryPlan.getLogicalEdge(logicalBinaryVertex.getRightInput(), logicalBinaryVertex);
                    binaryOperator.setLeftShuffleType(CompilerUtils.parseShuffleTypeFromEdge(leftEdge))
                            .setLeftStreamIndex(leftEdge.getStreamIndex())
                            .setRightShuffleType(CompilerUtils.parseShuffleTypeFromEdge(rightEdge))
                            .setRightStreamIndex(rightEdge.getStreamIndex());
                    queryPlanBuilder.addBinaryOp(binaryOperator);
                } else {
                    throw new IllegalArgumentException(logicalVertex.toString());
                }

                queryPlanBuilder.addOperatorIdList(logicalVertex.getId());
                Pair<Integer, Integer> dfsCmdPair = dfsGraphVertexMapping.get(logicalVertex.getId());
                if (null != dfsCmdPair) {
                    queryPlanBuilder.addOperatorIdList(dfsCmdPair.getLeft());
                    queryPlanBuilder.addOperatorIdList(dfsCmdPair.getRight());
                }
            }
        }
    }

    private OperatorBase.Builder createChainedFunction(LogicalVertex logicalVertex) {
        OperatorBase.Builder functionBuilder = OperatorBase.newBuilder();
        ProcessorFunction processorFunction = logicalVertex.getProcessorFunction();
        functionBuilder.setOperatorType(processorFunction.getOperatorType());
        Value.Builder argumentBuilder = processorFunction.getArgumentBuilder();
        if (processorFunction instanceof ProcessorLabelValueFunction) {
            if (null == argumentBuilder) {
                argumentBuilder = Value.newBuilder();
            }
            ProcessorLabelValueFunction processorLabelValueFunction = (ProcessorLabelValueFunction) processorFunction;
            argumentBuilder.setIntValue(processorLabelValueFunction.getLabelId());
            if (null != processorLabelValueFunction.getLabelValueVertex()) {
                OperatorBase.Builder labelValueBuilder = createChainedFunction(processorLabelValueFunction.getLabelValueVertex());
                argumentBuilder.setPayload(labelValueBuilder.build().toByteString());
            }
        }
        if (argumentBuilder != null) {
            functionBuilder.setArgument(argumentBuilder);
        }
        functionBuilder.addAllLogicalCompare(processorFunction.getLogicalCompareList());
        if (processorFunction.getRangeLimit() != null) {
            functionBuilder.setRangeLimit(processorFunction.getRangeLimit());
        }

        functionBuilder.addAllAfterRequirement(logicalVertex.getAfterRequirementList().stream().map(v -> v.build()).collect(Collectors.toList()));
        functionBuilder.addAllBeforeRequirement(logicalVertex.getBeforeRequirementList().stream().map(v -> v.build()).collect(Collectors.toList()));

        return functionBuilder;
    }

    private List<LogicalCompare> parseSourceLogicalCompare(ExecuteParam executeParam, Message.Value.Builder valueBuilder) {
        boolean pushLabelFlag = false;
        boolean pushIdFlag = false;
        List<LogicalCompare> logicalCompareList = Lists.newArrayList();
        for (LogicalCompare logicalCompare : executeParam.getLogicalCompareList()) {
            if (logicalCompare.getPropId() == TreeConstants.LABEL_INDEX
                    && valueBuilder.getIntValueListCount() == 0) {
                if (logicalCompare.getCompare() == CompareType.EQ) {
                    valueBuilder.addIntValueList(logicalCompare.getValue().getIntValue());
                    if (pushLabelFlag) {
                        logicalCompareList.add(logicalCompare);
                    } else {
                        pushLabelFlag = true;
                    }
                } else if (logicalCompare.getCompare() == CompareType.WITHIN) {
                    valueBuilder.addAllIntValueList(logicalCompare.getValue().getIntValueListList());
                    if (pushLabelFlag) {
                        logicalCompareList.add(logicalCompare);
                    } else {
                        pushLabelFlag = true;
                    }
                }
            } else if (logicalCompare.getPropId() == TreeConstants.ID_INDEX
                    && valueBuilder.getLongValueListCount() == 0) {
                if (logicalCompare.getCompare() == CompareType.EQ) {
                    valueBuilder.addLongValueList(logicalCompare.getValue().getLongValue());
                    if (pushIdFlag) {
                        logicalCompareList.add(logicalCompare);
                    } else {
                        pushIdFlag = true;
                    }
                } else if (logicalCompare.getCompare() == CompareType.WITHIN) {
                    valueBuilder.addAllLongValueList(logicalCompare.getValue().getLongValueListList());
                    if (pushIdFlag) {
                        logicalCompareList.add(logicalCompare);
                    } else {
                        pushIdFlag = true;
                    }
                }
            } else {
                logicalCompareList.add(logicalCompare);
            }
        }
        return logicalCompareList;
    }
}

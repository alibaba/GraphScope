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

import com.alibaba.maxgraph.Message;
import com.alibaba.maxgraph.QueryFlowOuterClass;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.compiler.optimizer.DataStatistics;
import com.google.protobuf.InvalidProtocolBufferException;

public class LogicalUnaryVertex extends LogicalVertex {
    private LogicalVertex inputVertex;

    public LogicalUnaryVertex(int id, ProcessorFunction processorFunction, LogicalVertex inputVertex) {
        super(id, processorFunction, false);
        this.inputVertex = inputVertex;
    }

    public LogicalUnaryVertex(int id, ProcessorFunction processorFunction, boolean propLocalFlag, LogicalVertex inputVertex) {
        super(id, processorFunction, propLocalFlag);
        this.inputVertex = inputVertex;
    }

    public LogicalVertex getInputVertex() {
        return inputVertex;
    }

    @Override
    public void resetInputVertex(LogicalVertex oldInput, LogicalVertex newInput) {
        this.inputVertex = newInput;
    }

    @Override
    public void computeOutputEstimates(DataStatistics statistics, GraphSchema schema) {
        this.estimatedNumRecords = outputNumEstimates(statistics, inputVertex);
        this.estimatedValueSize = outputValueSizeEstimates(statistics, inputVertex);
    }

    private double outputValueSizeEstimates(DataStatistics statistics, LogicalVertex inputVertex) {
        QueryFlowOuterClass.OperatorType operatorType = processorFunction.getOperatorType();
        switch (operatorType) {
            case V:
            case E:
            case V_COUNT:
            case E_COUNT: {
                throw new IllegalArgumentException("invalid source operator in unary");
            }

            case OUT:
            case IN:
            case BOTH:
            case OUT_V:
            case IN_V:
            case BOTH_V:
            case OTHER_V: {
                return statistics.getVertexValueSize();
            }
            case OUT_E:
            case IN_E:
            case BOTH_E: {
                return statistics.getEdgeValueSize();
            }
            case SELECT_ONE:
            case SELECT:
            case PATH_OUT:
            case PROP_FILL:
            case PROP_VALUE:
            case PROP_MAP_VALUE:
            case PROP_KEY_VALUE:
            case PROPERTIES:
            case CONSTANT:
            case COLUMN:
            case ENTRY_OUT:
            case OUT_COUNT:
            case IN_COUNT:
            case BOTH_COUNT:
            case COUNT_LOCAL:
            case RANGE_LOCAL:
            case ORDER_LOCAL:
            case GRAPH_SOURCE:
            case PROGRAM_CC:
            case NOT:
            case SAMPLE:
            case SACK_OUT: {
                return inputVertex.getEstimatedNumRecords();
            }
            case FILTER:
            case HAS:
            case WHERE_LABEL:
            case WHERE:
            case SIMPLE_PATH:
            case ORDER:
            case RANGE:
            case RANGE_BY_KEY:
            case COUNT_LIMIT:
            case DEDUP:
            case DEDUP_BY_KEY:
            case BRANCH_OPTION:
            case MAX:
            case MIN:
            case SUM:
            case FOLD_BY_KEY:
            case SUM_BY_KEY:
            case MAX_BY_KEY:
            case MIN_BY_KEY:
            case DEDUP_COUNT_LABEL:
            case DUPLICATE_LABEL:
            case GROUP_COUNT:
            case COUNT_BY_KEY: {
                return inputVertex.getEstimatedAvgWidthPerOutputValue();
            }
            case COUNT:
            case WRITE_ODPS:
            case DFS_SOURCE:
            case DFS_REPEAT_GRAPH:
            case DFS_FINISH_JOIN: {
                return statistics.getLongValueSize();
            }
            case FOLD:
            case FOLDMAP: {
                return inputVertex.getEstimatedAvgWidthPerOutputValue() / inputVertex.getEstimatedNumRecords();
            }
            case UNFOLD: {
                return (inputVertex.getEstimatedAvgWidthPerOutputValue() / statistics.getUnfoldFactor());
            }

            case JOIN_LABEL:
            case UNION:
            case JOIN_COUNT_LABEL:
            case JOIN_DIRECT_FILTER:
            case JOIN_DIRECT_FILTER_NEGATE:
            case JOIN_DIRECT_FILTER_KEY_NEGATE:
            case BINARY_CHAIN: {
                throw new IllegalArgumentException("binary operator can't be estimate here");
            }
            case REPEAT_START:
            case REPEAT:
            case SOURCE_CHAIN:
            case UNARY_CHAIN:
            default: {
                throw new IllegalArgumentException(operatorType.toString());
            }
        }
    }

    private double outputNumEstimates(DataStatistics dataStatistics, LogicalVertex inputVertex) {
        QueryFlowOuterClass.OperatorType operatorType = processorFunction.getOperatorType();
        QueryFlowOuterClass.RangeLimit.Builder rangeLimit = processorFunction.getRangeLimit();
        switch (operatorType) {
            case V:
            case E:
            case V_COUNT:
            case E_COUNT:
            case SOURCE_CHAIN: {
                throw new IllegalArgumentException("invalid source operator in unary");
            }

            case OUT:
            case OUT_E: {
                return (inputVertex.getEstimatedNumRecords() * dataStatistics.getOutNumFactor());
            }
            case IN:
            case IN_E: {
                return (inputVertex.getEstimatedNumRecords() * dataStatistics.getInNumFactor());
            }
            case BOTH:
            case BOTH_E: {
                return (inputVertex.getEstimatedNumRecords() * (dataStatistics.getOutNumFactor() + dataStatistics.getInNumFactor()));
            }
            case OUT_V:
            case IN_V:
            case OTHER_V:
            case SELECT_ONE:
            case SELECT:
            case PATH_OUT:
            case PROP_FILL:
            case PROP_VALUE:
            case PROP_MAP_VALUE:
            case PROP_KEY_VALUE:
            case PROPERTIES:
            case CONSTANT:
            case COLUMN:
            case ENTRY_OUT:
            case OUT_COUNT:
            case IN_COUNT:
            case BOTH_COUNT:
            case COUNT_LOCAL:
            case RANGE_LOCAL:
            case ORDER_LOCAL:
            case GRAPH_SOURCE:
            case PROGRAM_CC:
            case NOT:
            case SAMPLE:
            case SACK_OUT: {
                return inputVertex.getEstimatedNumRecords();
            }
            case BOTH_V: {
                return inputVertex.getEstimatedNumRecords() * 2;
            }
            case FILTER:
            case HAS:
            case WHERE_LABEL:
            case WHERE:
            case SIMPLE_PATH: {
                return (inputVertex.getEstimatedNumRecords() * dataStatistics.getFilterFactor());
            }
            case ORDER: {
                if (null == rangeLimit) {
                    return inputVertex.getEstimatedNumRecords();
                }
                long count = rangeLimit.getRangeEnd() - rangeLimit.getRangeStart();
                if (count <= 0 || count > inputVertex.getEstimatedNumRecords()) {
                    return inputVertex.getEstimatedNumRecords();
                } else {
                    return count;
                }
            }
            case RANGE:
            case RANGE_BY_KEY:
            case COUNT_LIMIT: {
                long count = rangeLimit.getRangeEnd() - rangeLimit.getRangeStart();
                if (count <= 0 || count > inputVertex.getEstimatedNumRecords()) {
                    return inputVertex.getEstimatedNumRecords();
                } else {
                    return count;
                }
            }
            case DEDUP:
            case DEDUP_BY_KEY:
            case COUNT_BY_KEY:
            case FOLD_BY_KEY:
            case SUM_BY_KEY:
            case MAX_BY_KEY:
            case MIN_BY_KEY:
            case DEDUP_COUNT_LABEL:
            case DUPLICATE_LABEL: {
                return (inputVertex.getEstimatedNumRecords() * dataStatistics.getDedupFactor());
            }
            case BRANCH_OPTION: {
                try {
                    Message.BranchOptionList branchOptionList = Message.BranchOptionList.parseFrom(processorFunction.getArgumentBuilder().getPayload());
                    return inputVertex.getEstimatedNumRecords() / branchOptionList.getOptionValueCount();
                } catch (InvalidProtocolBufferException e) {
                    throw new IllegalArgumentException(e);
                }
            }
            case COUNT:
            case MAX:
            case MIN:
            case FOLD:
            case SUM:
            case FOLDMAP:
            case WRITE_ODPS:
            case DFS_SOURCE:
            case DFS_REPEAT_GRAPH:
            case DFS_FINISH_JOIN:
            case GROUP_COUNT: {
                return 1L;
            }
            case UNFOLD: {
                return (inputVertex.getEstimatedNumRecords() * dataStatistics.getUnfoldFactor());
            }
            case JOIN_LABEL:
            case UNION:
            case JOIN_COUNT_LABEL:
            case JOIN_DIRECT_FILTER:
            case JOIN_DIRECT_FILTER_NEGATE:
            case JOIN_DIRECT_FILTER_KEY_NEGATE:
            case BINARY_CHAIN: {
                throw new IllegalArgumentException("binary operator can't be estimate here");
            }
            case REPEAT_START:
            case REPEAT:
            case UNARY_CHAIN:
            default: {
                throw new IllegalArgumentException(operatorType.toString());
            }
        }
    }
}

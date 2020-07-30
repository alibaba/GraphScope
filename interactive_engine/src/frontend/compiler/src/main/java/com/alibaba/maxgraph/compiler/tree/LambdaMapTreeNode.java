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
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.tree.value.MapEntryValueType;
import com.alibaba.maxgraph.compiler.logical.edge.EdgeShuffleType;
import com.alibaba.maxgraph.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.compiler.utils.SchemaUtils;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.map.RangeSumFunction;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.map.MapPropFillFunction;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.output.OutputOdpsFunction;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.output.OutputOdpsTable;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.compiler.tree.value.ValueValueType;
import com.alibaba.maxgraph.compiler.logical.LogicalEdge;
import com.alibaba.maxgraph.compiler.logical.LogicalSourceDelegateVertex;
import com.alibaba.maxgraph.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.compiler.logical.LogicalUnaryVertex;
import com.alibaba.maxgraph.compiler.logical.LogicalVertex;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.compiler.utils.CompilerUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class LambdaMapTreeNode extends UnaryTreeNode {
    private Function mapFunction;
    private String lambdaIndex;


    public LambdaMapTreeNode(TreeNode prev, GraphSchema schema, Function mapFunction, String lambdaIndex) {
        super(prev, NodeType.LAMBDA_MAP, schema);
        checkArgument(mapFunction instanceof OutputOdpsFunction ||
                        mapFunction instanceof MapPropFillFunction ||
                        mapFunction instanceof RangeSumFunction || null != lambdaIndex,
                        "lambdaIndex for LambdaMap can't be null unless it demands to write odps or fill prop or range sum");
        this.mapFunction = checkNotNull(mapFunction);
        this.lambdaIndex = lambdaIndex;
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        if (this.mapFunction instanceof OutputOdpsFunction) {
            OutputOdpsFunction outputOdpsFunction = OutputOdpsFunction.class.cast(this.mapFunction);
            OutputOdpsTable outputOdpsTable = outputOdpsFunction.getOutputOdpsTable();
            QueryFlowOuterClass.OdpsOutputConfig.Builder odpsConfigBuilder = QueryFlowOuterClass.OdpsOutputConfig.newBuilder()
                    .setEndpoint(outputOdpsTable.getEndpoint())
                    .setAccessId(outputOdpsTable.getAccessId())
                    .setAccessKey(outputOdpsTable.getAccessKey());
            odpsConfigBuilder.setProject(outputOdpsTable.getProject())
                    .setTableName(outputOdpsTable.getTable());
            if (StringUtils.isNotEmpty(outputOdpsTable.getDs())) {
                odpsConfigBuilder.setDs(outputOdpsTable.getDs());
            }
            for (String propName : outputOdpsTable.getPropNameList()) {
                odpsConfigBuilder.addPropId(SchemaUtils.getPropId(propName, schema));
            }
            Message.Value.Builder argumentBuilder = Message.Value.newBuilder().setPayload(odpsConfigBuilder.build().toByteString());
            ProcessorFunction odpsWriteFunction = new ProcessorFunction(QueryFlowOuterClass.OperatorType.WRITE_ODPS, argumentBuilder);
            LogicalSubQueryPlan logicalSubQueryPlan = parseSingleUnaryVertex(contextManager.getVertexIdManager(), contextManager.getTreeNodeLabelManager(), odpsWriteFunction, contextManager);

            LogicalVertex outputVertex = logicalSubQueryPlan.getOutputVertex();
            ProcessorFunction sumFunction = new ProcessorFunction(
                    QueryFlowOuterClass.OperatorType.SUM,
                    Message.Value.newBuilder().setValueType(Message.VariantType.VT_LONG));
            LogicalVertex sumVertex = new LogicalUnaryVertex(contextManager.getVertexIdManager().getId(), sumFunction, true, outputVertex);
            logicalSubQueryPlan.addLogicalVertex(sumVertex);
            logicalSubQueryPlan.addLogicalEdge(outputVertex, sumVertex, new LogicalEdge(EdgeShuffleType.SHUFFLE_BY_CONST));

            addUsedLabelAndRequirement(sumVertex, contextManager.getTreeNodeLabelManager());
            setFinishVertex(sumVertex, contextManager.getTreeNodeLabelManager());

            return logicalSubQueryPlan;
        } else if (this.mapFunction instanceof RangeSumFunction) {
            RangeSumFunction rangeSumFunction = RangeSumFunction.class.cast(this.mapFunction);
            int propId = SchemaUtils.getPropId(rangeSumFunction.getPropName(), schema);
            ProcessorFunction processorFunction = new ProcessorFunction(
                    QueryFlowOuterClass.OperatorType.RANGE_SUM,
                    Message.Value.newBuilder().setIntValue(propId)
                            .addIntValueList(rangeSumFunction.getStart())
                            .addIntValueList(rangeSumFunction.getCount()));
            return parseSingleUnaryVertex(contextManager.getVertexIdManager(), contextManager.getTreeNodeLabelManager(), processorFunction, contextManager);
        } else if (this.mapFunction instanceof MapPropFillFunction){
            MapPropFillFunction mapPropFillFunction = MapPropFillFunction.class.cast(this.mapFunction);
            setPropLocalFlag(true);
            List<String> propNameList = mapPropFillFunction.getPropNameList();
            List<Integer> propIdList = propNameList.stream()
                    .map(v -> CompilerUtils.getPropertyId(schema, v))
                    .collect(Collectors.toList());
            LogicalVertex logicalVertex = getInputNode().getOutputVertex();
            if (logicalVertex.getProcessorFunction().getOperatorType() == QueryFlowOuterClass.OperatorType.PROP_FILL) {
                logicalVertex.getProcessorFunction().getArgumentBuilder().addAllIntValueList(propIdList);
                LogicalSubQueryPlan logicalSubQueryPlan = new LogicalSubQueryPlan(contextManager);
                logicalSubQueryPlan.addLogicalVertex(new LogicalSourceDelegateVertex(logicalVertex));
                return logicalSubQueryPlan;
            } else {
                ProcessorFunction processorFunction = new ProcessorFunction(
                        QueryFlowOuterClass.OperatorType.PROP_FILL,
                        Message.Value.newBuilder().addAllIntValueList(propIdList));
                return parseSingleUnaryVertex(contextManager.getVertexIdManager(), contextManager.getTreeNodeLabelManager(), processorFunction, contextManager);
            }
        } else {
            Message.Value.Builder argumentBuilder = Message.Value.newBuilder()
                    .setStrValue(this.lambdaIndex);

            ProcessorFunction processorFunction = new ProcessorFunction(QueryFlowOuterClass.OperatorType.LAMBDA_MAP, argumentBuilder);
            return parseSingleUnaryVertex(contextManager.getVertexIdManager(), contextManager.getTreeNodeLabelManager(), processorFunction, contextManager);

        }
    }

    @Override
    public ValueType getOutputValueType() {
        if (mapFunction instanceof OutputOdpsFunction) {
            return new ValueValueType(Message.VariantType.VT_LONG);
        } else if (mapFunction instanceof RangeSumFunction) {
            return new MapEntryValueType(getInputNode().getOutputValueType(), new ValueValueType(Message.VariantType.VT_DOUBLE));
        } else {
            return getInputNode().getOutputValueType();
        }
    }
}

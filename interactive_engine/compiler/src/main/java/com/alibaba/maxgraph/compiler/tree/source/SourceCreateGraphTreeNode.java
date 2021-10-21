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
package com.alibaba.maxgraph.compiler.tree.source;

import com.alibaba.maxgraph.Message;
import com.alibaba.maxgraph.QueryFlowOuterClass;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.logical.LogicalEdge;
import com.alibaba.maxgraph.compiler.logical.LogicalSourceVertex;
import com.alibaba.maxgraph.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.compiler.logical.LogicalUnaryVertex;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorSourceFunction;
import com.alibaba.maxgraph.compiler.optimizer.ContextManager;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.compiler.tree.value.ValueValueType;
import com.alibaba.maxgraph.compiler.utils.CompilerUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.StringUtils;

import static com.alibaba.maxgraph.common.util.CompilerConstant.QUERY_CREATE_GRAPH_TYPE;
import static com.google.common.base.Preconditions.checkNotNull;

public class SourceCreateGraphTreeNode extends SourceTreeNode {
    private String graphName;
    private Configuration configuration;


    public SourceCreateGraphTreeNode(GraphSchema schema, String graphName, Configuration configuration) {
        super(null, schema);
        this.graphName = checkNotNull(graphName);
        this.configuration = checkNotNull(configuration);
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        LogicalSubQueryPlan logicalQueryPlan = new LogicalSubQueryPlan(contextManager);
        logicalQueryPlan.setDelegateSourceFlag(false);

        String graphType = configuration.getString(QUERY_CREATE_GRAPH_TYPE, null);
        if (StringUtils.isEmpty(graphType)) {
            throw new IllegalArgumentException("Graph type must be setted by g.createGraph(graphName).with('graphType', '...')");
        }
        QueryFlowOuterClass.CreateGraphTypeProto createGraphType = QueryFlowOuterClass.CreateGraphTypeProto.valueOf(
                StringUtils.upperCase(graphType));
        if (createGraphType == QueryFlowOuterClass.CreateGraphTypeProto.VINEYARD) {
            QueryFlowOuterClass.RuntimeGraphSchemaProto schemaProto = CompilerUtils.buildRuntimeGraphSchema(schema);
            ProcessorSourceFunction createGraphFunction = new ProcessorSourceFunction(QueryFlowOuterClass.OperatorType.GRAPH_VINEYARD_BUILDER,
                    Message.Value.newBuilder().setStrValue(graphName).setPayload(schemaProto.toByteString()),
                    null);
            LogicalSourceVertex logicalSourceVertex = new LogicalSourceVertex(contextManager.getVertexIdManager().getId(),
                    createGraphFunction);
            logicalQueryPlan.addLogicalVertex(logicalSourceVertex);

            ProcessorFunction streamFunction = new ProcessorFunction(QueryFlowOuterClass.OperatorType.GRAPH_VINEYARD_STREAM,
                    Message.Value.newBuilder().setStrValue(graphName));
            LogicalUnaryVertex vineyardStreamVertex = new LogicalUnaryVertex(contextManager.getVertexIdManager().getId(), streamFunction, logicalSourceVertex);
            logicalQueryPlan.addLogicalVertex(vineyardStreamVertex);
            logicalQueryPlan.addLogicalEdge(logicalSourceVertex, vineyardStreamVertex, LogicalEdge.shuffleConstant());

            setFinishVertex(logicalQueryPlan.getOutputVertex(), contextManager.getTreeNodeLabelManager());
            return logicalQueryPlan;
        } else {
            throw new IllegalArgumentException("Only support create vineyard graph yet");
        }
    }

    @Override
    public ValueType getOutputValueType() {
        return new ValueValueType(Message.VariantType.VT_STRING);
    }
}

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
import com.alibaba.maxgraph.QueryFlowOuterClass.OdpsOutputConfig.Builder;
import com.alibaba.maxgraph.common.util.SchemaUtils;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.compiler.tree.value.VertexValueType;
import com.alibaba.maxgraph.compiler.logical.LogicalEdge;
import com.alibaba.maxgraph.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.compiler.logical.LogicalUnaryVertex;
import com.alibaba.maxgraph.compiler.logical.LogicalVertex;
import com.alibaba.maxgraph.compiler.logical.VertexIdManager;
import com.alibaba.maxgraph.compiler.logical.edge.EdgeShuffleType;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.compiler.optimizer.ContextManager;

import org.apache.commons.lang3.StringUtils;

public class OutputTreeNode extends UnaryTreeNode {
    public static final String TUNNEL = "tunnel://";
    private final static IllegalArgumentException ILLEGAL_TUNNEL_EXCEPTION =
        new IllegalArgumentException(
            "output URL format: tunnel://accsessID:accessKey@endpoint#project=maxgraph&table=table&ds=20190101");
    private final String path;
    private final String[] properties;

    public OutputTreeNode(TreeNode input, GraphSchema schema, String path, String... properties) {
        super(input, NodeType.FLATMAP, schema);
        this.path = path;
        this.properties = properties;
    }

    @Override
    public LogicalSubQueryPlan buildLogicalQueryPlan(ContextManager contextManager) {
        TreeNodeLabelManager labelManager = contextManager.getTreeNodeLabelManager();
        VertexIdManager vertexIdManager = contextManager.getVertexIdManager();
        Builder odpsConfigBuilder = buildArg();
        for (String propName : properties) {
            odpsConfigBuilder.addPropId(SchemaUtils.getPropId(propName, schema));
        }
        Message.Value.Builder argumentBuilder = Message.Value.newBuilder().setPayload(
            odpsConfigBuilder.build().toByteString());
        ProcessorFunction processorFunction = new ProcessorFunction(QueryFlowOuterClass.OperatorType.WRITE_ODPS,
            argumentBuilder);
        LogicalSubQueryPlan logicalSubQueryPlan = parseSingleUnaryVertex(vertexIdManager, labelManager,
            processorFunction, contextManager);
        LogicalVertex outputVertex = logicalSubQueryPlan.getOutputVertex();
        ProcessorFunction sumFunction = new ProcessorFunction(
                QueryFlowOuterClass.OperatorType.SUM,
                Message.Value.newBuilder().setValueType(Message.VariantType.VT_LONG));
        LogicalVertex sumVertex = new LogicalUnaryVertex(vertexIdManager.getId(), sumFunction, true, outputVertex);
        logicalSubQueryPlan.addLogicalVertex(sumVertex);
        logicalSubQueryPlan.addLogicalEdge(outputVertex, sumVertex, new LogicalEdge(EdgeShuffleType.SHUFFLE_BY_CONST));

        addUsedLabelAndRequirement(sumVertex, labelManager);
        setFinishVertex(sumVertex, labelManager);

        return logicalSubQueryPlan;
    }

    private Builder buildArg() {
        if (this.path.startsWith(TUNNEL)) {
            String remain = StringUtils.removeStart(path, TUNNEL);
            String[] tokens = StringUtils.split(remain, '@');
            if (tokens.length != 2) {
                throw ILLEGAL_TUNNEL_EXCEPTION;
            }
            String[] idKey = StringUtils.split(tokens[0], ':');
            if (idKey.length != 2) {
                throw ILLEGAL_TUNNEL_EXCEPTION;
            }
            Builder odpsConfigBuilder = QueryFlowOuterClass.OdpsOutputConfig.newBuilder()
                .setAccessId(idKey[0])
                .setAccessKey(idKey[1]);

            String[] endpointAndOthers = StringUtils.split(tokens[1], '#');
            if (endpointAndOthers.length != 2) {
                throw ILLEGAL_TUNNEL_EXCEPTION;
            }
            odpsConfigBuilder.setEndpoint(endpointAndOthers[0]);
            String[] details = StringUtils.split(endpointAndOthers[1], '&');
            for (String detail : details) {
                String[] keyValue = StringUtils.split(detail, '=');
                if (keyValue.length != 2) {
                    throw ILLEGAL_TUNNEL_EXCEPTION;
                }
                if (keyValue[0].equalsIgnoreCase("project")) {
                    odpsConfigBuilder.setProject(keyValue[1]);
                } else if (keyValue[0].equalsIgnoreCase("table")) {
                    odpsConfigBuilder.setTableName(keyValue[1]);
                } else if (keyValue[0].equalsIgnoreCase("ds")) {
                    odpsConfigBuilder.setDs(keyValue[1]);
                } else {
                    throw ILLEGAL_TUNNEL_EXCEPTION;
                }
            }
            return odpsConfigBuilder;
        }
        throw ILLEGAL_TUNNEL_EXCEPTION;
    }

    @Override
    public ValueType getOutputValueType() {
        return new VertexValueType();
    }
}

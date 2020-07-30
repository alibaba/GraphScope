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
package com.alibaba.maxgraph.compiler.tree.addition;

import com.alibaba.maxgraph.QueryFlowOuterClass;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.logical.LogicalBinaryVertex;
import com.alibaba.maxgraph.compiler.logical.LogicalEdge;
import com.alibaba.maxgraph.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.compiler.logical.LogicalVertex;
import com.alibaba.maxgraph.compiler.logical.VertexIdManager;
import com.alibaba.maxgraph.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.compiler.tree.NodeType;
import com.alibaba.maxgraph.compiler.tree.TreeNode;
import com.alibaba.maxgraph.compiler.tree.UnaryTreeNode;

/**
 * Determine whether aggregation by key
 */
public abstract class AbstractUseKeyNode extends UnaryTreeNode {
    private boolean useKeyFlag = false;
    private LogicalVertex sourceVertex = null;

    public AbstractUseKeyNode(TreeNode input, NodeType nodeType, GraphSchema schema) {
        super(input, nodeType, schema);
    }

    public void enableUseKeyFlag(LogicalVertex sourceVertex) {
        this.useKeyFlag = true;
        this.sourceVertex = sourceVertex;
    }

    protected boolean isUseKeyFlag() {
        return this.useKeyFlag;
    }

    protected LogicalVertex getSourceVertex() {
        return this.sourceVertex;
    }

    protected QueryFlowOuterClass.OperatorType getUseKeyOperator(QueryFlowOuterClass.OperatorType operatorType) {
        if (this.useKeyFlag) {
            return QueryFlowOuterClass.OperatorType.valueOf(operatorType.name() + "_BY_KEY");
        } else {
            return operatorType;
        }
    }

    /**
     * Process zero for sum and count vertex
     *
     * @param vertexIdManager     The vertex id manager
     * @param logicalSubQueryPlan The logical sub query plan
     * @param valueVertex         The value vertex
     * @param joinZeroFlag        The join zero flag
     * @return The output vertex after deal with join zero flag
     */
    protected LogicalVertex processJoinZeroVertex(VertexIdManager vertexIdManager,
                                                  LogicalSubQueryPlan logicalSubQueryPlan,
                                                  LogicalVertex valueVertex,
                                                  boolean joinZeroFlag) {
        LogicalVertex outputVertex = valueVertex;
        LogicalVertex leftVertex = getSourceVertex();
        if (joinZeroFlag && null != leftVertex) {
            ProcessorFunction joinZeroFunction = new ProcessorFunction(QueryFlowOuterClass.OperatorType.JOIN_RIGHT_ZERO_JOIN);
            LogicalBinaryVertex logicalBinaryVertex = new LogicalBinaryVertex(
                    vertexIdManager.getId(),
                    joinZeroFunction,
                    false,
                    leftVertex,
                    valueVertex);
            logicalSubQueryPlan.addLogicalVertex(logicalBinaryVertex);
            logicalSubQueryPlan.addLogicalEdge(valueVertex, logicalBinaryVertex, new LogicalEdge());
            outputVertex = logicalBinaryVertex;
        }
        return outputVertex;
    }
}

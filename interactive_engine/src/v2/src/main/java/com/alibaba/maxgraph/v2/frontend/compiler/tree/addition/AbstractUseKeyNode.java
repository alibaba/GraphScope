package com.alibaba.maxgraph.v2.frontend.compiler.tree.addition;

import com.alibaba.maxgraph.proto.v2.OperatorType;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalBinaryVertex;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalEdge;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalSubQueryPlan;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalVertex;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.VertexIdManager;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.NodeType;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.TreeNode;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.UnaryTreeNode;

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

    protected OperatorType getUseKeyOperator(OperatorType operatorType) {
        if (this.useKeyFlag) {
            return OperatorType.valueOf(operatorType.name() + "_BY_KEY");
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
            ProcessorFunction joinZeroFunction = new ProcessorFunction(OperatorType.JOIN_RIGHT_ZERO_JOIN);
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

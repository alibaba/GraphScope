package com.alibaba.maxgraph.v2.frontend.compiler.optimizer.costs;

import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalBinaryVertex;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalEdge;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalQueryPlan;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalSourceDelegateVertex;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalSourceVertex;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalUnaryVertex;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalVertex;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.edge.EdgeShuffleType;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.function.ProcessorFunction;
import com.alibaba.maxgraph.v2.frontend.compiler.optimizer.DataStatistics;

import java.util.List;

public class CostEstimator {
    /**
     * This method computes the cost of an operator. The cost is composed of cost for input shipping,
     * locally processing an input, and running the operator.
     * <p>
     * It requires at least that all inputs are set and have a proper ship strategy set,
     * which is not equal to <tt>NONE</tt>.
     *
     * @param queryPlan The given query plan
     * @param vertex    The node to compute the costs for.
     */
    private Costs costOperator(LogicalQueryPlan queryPlan, LogicalVertex vertex) {
        Costs costs = new Costs();
        if (vertex instanceof LogicalSourceVertex) {
            if (vertex instanceof LogicalSourceDelegateVertex) {
                LogicalSourceDelegateVertex logicalSourceDelegateVertex = LogicalSourceDelegateVertex.class.cast(vertex);
                LogicalVertex delegateVertex = logicalSourceDelegateVertex.getDelegateVertex();
                costs.setCpuCost(delegateVertex.getEstimatedNumRecords());
                costs.setNetworkCost(delegateVertex.getEstimatedOutputSize());
            } else {
                costs.setCpuCost(vertex.getEstimatedNumRecords());
                costs.setNetworkCost(0);
            }
        } else if (vertex instanceof LogicalUnaryVertex) {
            LogicalUnaryVertex unaryVertex = LogicalUnaryVertex.class.cast(vertex);
            LogicalVertex inputVertex = unaryVertex.getInputVertex();
            LogicalEdge logicalEdge = queryPlan.getLogicalEdge(inputVertex, unaryVertex);
            if (null != logicalEdge && logicalEdge.getShuffleType() != EdgeShuffleType.FORWARD) {
                costs.setNetworkCost(inputVertex.getEstimatedOutputSize());
            } else {
                costs.setNetworkCost(0);
            }
            costs.setCpuCost(inputVertex.getEstimatedNumRecords() * getOperatorFactor(vertex.getProcessorFunction()));
        } else {
            LogicalBinaryVertex logicalBinaryVertex = LogicalBinaryVertex.class.cast(vertex);
            LogicalVertex leftVertex = logicalBinaryVertex.getLeftInput();
            double networkCost = 0;
            if (queryPlan.getLogicalEdge(leftVertex, logicalBinaryVertex).getShuffleType() != EdgeShuffleType.FORWARD) {
                networkCost += leftVertex.getEstimatedOutputSize();
            }
            LogicalVertex rightVertex = logicalBinaryVertex.getRightInput();
            if (queryPlan.getLogicalEdge(rightVertex, logicalBinaryVertex).getShuffleType() != EdgeShuffleType.FORWARD) {
                networkCost += rightVertex.getEstimatedOutputSize();
            }
            costs.setNetworkCost(networkCost);
            costs.setCpuCost(leftVertex.getEstimatedNumRecords() * 2 + rightVertex.getEstimatedNumRecords());
        }

        return costs;
    }

    private double getOperatorFactor(ProcessorFunction processorFunction) {
        return 1.0;
    }

    /**
     * This method computes the cost of an query plan.
     *
     * @param queryPlan  The given query plan
     * @param statistics The DataStatistics
     * @param schema     The Schema
     * @return The costs of query plan
     */
    public Costs costQueryPlan(LogicalQueryPlan queryPlan, DataStatistics statistics, GraphSchema schema) {
        List<LogicalVertex> logicalVertexList = queryPlan.getLogicalVertexList();
        Costs totalCosts = new Costs();
        for (LogicalVertex logicalVertex : logicalVertexList) {
            logicalVertex.computeOutputEstimates(statistics, schema);
            Costs vertexCosts = costOperator(queryPlan, logicalVertex);
            totalCosts.addCosts(vertexCosts);
        }

        return totalCosts;
    }
}

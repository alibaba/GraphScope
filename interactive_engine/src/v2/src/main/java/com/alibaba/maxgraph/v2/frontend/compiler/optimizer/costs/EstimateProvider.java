package com.alibaba.maxgraph.v2.frontend.compiler.optimizer.costs;

/**
 * Methods for operators / connections that provide estimated about data size and
 * characteristics.
 */
public interface EstimateProvider {
    /**
     * Gets the estimated output size from this node.
     *
     * @return The estimated output size.
     */
    double getEstimatedOutputSize();

    /**
     * Gets the estimated number of records in the output of this node.
     *
     * @return The estimated number of records.
     */
    double getEstimatedNumRecords();

    /**
     * Gets the estimated number of bytes per record.
     *
     * @return The estimated number of bytes per value.
     */
    double getEstimatedAvgWidthPerOutputValue();

    /**
     * Gets the estimated number of bytes of label value.
     *
     * @return The estimated number of bytes of label value.
     */
    double getEstimatedAvgWidthOutputLabel();

    /**
     * Gets the estimated number of bytes of path value.
     *
     * @return The estimated number of bytes of path value.
     */
    double getEstimatedAvgWidthOutputPath();
}

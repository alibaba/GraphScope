package com.alibaba.graphscope.common.ir.rel.metadata;

import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;

import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalExpand;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalGetV;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalPathExpand;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalSource;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalPattern;

/**
 * A provider for
 * {@link com.alibaba.graphscope.common.ir.rel.metadata.GlogueRowCount} via
 * reflection.
 * It is used to get row count estimation for non-fuzzy graph operators.
 */

// TODO: in this class, we can get row count estimation directly via glogue.
public class GlogueRowCountImpl implements MetadataHandler<GlogueRowCount> {

    @Deprecated
    public MetadataDef<GlogueRowCount> getDef() {
        return GlogueRowCount.DEF;
    }

    /**
     * Implementation of {@link GlogueRowCount.RowCount#getRowCount} for
     * {@link org.apache.calcite.rel.logical.GraphLogicalSource}, called via
     * reflection.
     */
    @SuppressWarnings("UnusedDeclaration")
    public Double getRowCount(GraphLogicalSource source) {
        // TODO: get row count via glogue
        System.out.println("GlogueRowCountImpl.getRowCount(GraphLogicalSource source)");
        return 1.0;
    }

    /**
     * Implementation of {@link GlogueRowCount.RowCount#getRowCount} for
     * {@link org.apache.calcite.rel.logical.GraphLogicalExpand}, called via
     * reflection.
     */
    @SuppressWarnings("UnusedDeclaration")
    public Double getRowCount(GraphLogicalExpand expand) {
        // TODO: get row count via glogue
        System.out.println("GraphLogicalExpand.getRowCount(GraphLogicalExpand expand)");
        return 1.0;
    }

    /**
     * Implementation of {@link GlogueRowCount.RowCount#getRowCount} for
     * {@link org.apache.calcite.rel.logical.GraphLogicalGetV}, called via
     * reflection.
     */
    @SuppressWarnings("UnusedDeclaration")
    public Double getRowCount(GraphLogicalGetV getV) {
        // TODO: get row count via glogue
        return 1.0;
    }

    /**
     * Implementation of {@link GlogueRowCount.RowCount#getRowCount} for
     * {@link org.apache.calcite.rel.logical.GraphLogicalPathExpand}, called via
     * reflection.
     */
    @SuppressWarnings("UnusedDeclaration")
    public Double getRowCount(GraphLogicalPathExpand pathExpand) {
        // TODO: get row count via glogue
        return 1.0;
    }

    /**
     * Implementation of {@link GlogueRowCount.RowCount#getRowCount} for
     * {@link org.apache.calcite.rel.logical.GraphLogicalPattern}, called via
     * reflection.
     */
    @SuppressWarnings("UnusedDeclaration")
    public Double getRowCount(GraphLogicalPattern pattern) {
        // TODO: get row count via glogue
        return 1.0;
    }

}

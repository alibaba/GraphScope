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
 * It is used to get row count estimation for fuzzy graph operators.
 */

// TODO: in this class, we can should firstly process with fuzzy patterns, and
// then call GlogueRowCountImpl to get row count estimation.
public class GlogueRowCountArbitraryImpl implements MetadataHandler<GlogueRowCount> {
    private GlogueRowCountImpl nonFuzzyImpl;

    public GlogueRowCountArbitraryImpl() {
        this.nonFuzzyImpl = new GlogueRowCountImpl();
    }

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
        System.out.println("GlogueRowCountFuzzyImpl.getRowCount(GraphLogicalSource source)");
        return nonFuzzyImpl.getRowCount(source);
    }

    /**
     * Implementation of {@link GlogueRowCount.RowCount#getRowCount} for
     * {@link org.apache.calcite.rel.logical.GraphLogicalExpand}, called via
     * reflection.
     */
    @SuppressWarnings("UnusedDeclaration")
    public Double getRowCount(GraphLogicalExpand expand) {
        // TODO: process fuzzy patterns
        System.out.println("GlogueRowCountFuzzyImpl.getRowCount(GraphLogicalExpand expand)");
        return nonFuzzyImpl.getRowCount(expand);
    }

    /**
     * Implementation of {@link GlogueRowCount.RowCount#getRowCount} for
     * {@link org.apache.calcite.rel.logical.GraphLogicalGetV}, called via
     * reflection.
     */
    @SuppressWarnings("UnusedDeclaration")
    public Double getRowCount(GraphLogicalGetV getV) {
        // TODO: process fuzzy patterns
        return nonFuzzyImpl.getRowCount(getV);
    }

    /**
     * Implementation of {@link GlogueRowCount.RowCount#getRowCount} for
     * {@link org.apache.calcite.rel.logical.GraphLogicalPathExpand}, called via
     * reflection.
     */
    @SuppressWarnings("UnusedDeclaration")
    public Double getRowCount(GraphLogicalPathExpand pathExpand) {
        // TODO: process fuzzy patterns
        return nonFuzzyImpl.getRowCount(pathExpand);
    }

    /**
     * Implementation of {@link GlogueRowCount.RowCount#getRowCount} for
     * {@link org.apache.calcite.rel.logical.GraphLogicalPattern}, called via
     * reflection.
     */
    @SuppressWarnings("UnusedDeclaration")
    public Double getRowCount(GraphLogicalPattern pattern) {
        // TODO: process fuzzy patterns
        System.out.println("GlogueRowCountFuzzyImpl.getRowCount(GraphLogicalPattern pattern)");
        return nonFuzzyImpl.getRowCount(pattern);
    }

}

package com.alibaba.graphscope.common.ir.rel.metadata;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.MetadataHandlerProvider;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import static org.apache.calcite.linq4j.Nullness.castNonNull;

// in RelOptCluster.java, can setMetadataQuerySupplier() with the customized GlogueMetadataQuery 
// RelMetadataQuery holds all the default handlers in BuiltInMetadata;
// while GlogueMetadataQuery extends with handler of GlogueRowCount for row count estimation.

public class GlogueMetadataQuery extends RelMetadataQuery {
    private GlogueRowCount.Handler rowCountHandler;

    /**
     * Create a GlogueMetadataQuery with a given {@link MetadataHandlerProvider}.
     * 
     * @param provider The provider to use for construction.
     */
    GlogueMetadataQuery(MetadataHandlerProvider provider) {
        super(provider);
        this.rowCountHandler = provider.handler(GlogueRowCount.Handler.class);
    }

    /**
     * Returns the
     * {@link GlogueMetadata.RowCount#getRowCount()}
     * statistic.
     *
     * @param rel the relational expression
     * @return estimated row count, or null if no reliable estimate can be
     *         determined
     */
    @Override
    public Double getRowCount(RelNode rel) {
        for (;;) {
            try {
                Double result = rowCountHandler.getRowCount(rel, this);
                return RelMdUtil.validateResult(castNonNull(result));
            } catch (MetadataHandlerProvider.NoHandler e) {
                rowCountHandler = revise(GlogueRowCount.Handler.class);
            }
        }
    }

}

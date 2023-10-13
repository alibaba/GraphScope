package com.alibaba.graphscope.common.ir.rel.metadata;

import java.lang.reflect.Method;

import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.checkerframework.checker.nullness.qual.Nullable;

public interface GlogueRowCount extends Metadata {

    Method METHOD = Types.lookupMethod(GlogueRowCount.class, "getRowCount");

    MetadataDef<GlogueRowCount> DEF = MetadataDef.of(GlogueRowCount.class, GlogueRowCount.Handler.class,
            METHOD);

    /**
     * Estimates the number of rows which will be returned by a relational
     * expression. The default implementation for this query asks the rel itself
     * via {@link RelNode#estimateRowCount}, but metadata providers can override
     * this with their own cost models.
     *
     * @return estimated row count, or null if no reliable estimate can be
     *         determined
     */
    @Nullable
    Double getRowCount(RelNode r);

    /** Handler API. */
    @FunctionalInterface
    interface Handler extends MetadataHandler<GlogueRowCount> {
        @Nullable
        Double getRowCount(RelNode r, RelMetadataQuery mq);

        @Override
        default MetadataDef<GlogueRowCount> getDef() {
            return DEF;
        }
    }

}

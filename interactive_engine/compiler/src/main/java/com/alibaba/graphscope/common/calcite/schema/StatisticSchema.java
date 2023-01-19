package com.alibaba.graphscope.common.calcite.schema;

import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;

import org.apache.calcite.schema.Statistic;

import java.util.List;

/**
 * extends {@link GraphSchema} to add {@link Statistic}
 */
public interface StatisticSchema extends GraphSchema {
    // get meta for CBO
    Statistic getStatistic(List<String> tableName);
}

package com.alibaba.graphscope.common.calcite.rel.builder.config;

/**
 * to transform to {@link org.apache.calcite.sql.JoinType} in a {@link org.apache.calcite.rel.core.Join} structure
 */
public enum MatchOpt {
    INNER,
    ANTI, // the sentence is anti, i.e. `not(as("a").out().as("b"))` in gremlin query
    OPTIONAL, // the sentence is optional, still keep the sentence even though it is not joined by
              // any others
}

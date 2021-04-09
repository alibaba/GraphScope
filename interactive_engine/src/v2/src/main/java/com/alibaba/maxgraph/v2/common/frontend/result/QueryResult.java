package com.alibaba.maxgraph.v2.common.frontend.result;

/**
 * Top interface for query result
 */
public interface QueryResult {
    /**
     * Convert the {@link QueryResult} to Gremlin vertex/edge/property and java basic instance
     *
     * @return The gremlin/java basic result object
     */
    Object convertToGremlinStructure();
}

package com.alibaba.graphscope.gaia.common;

import org.apache.tinkerpop.gremlin.driver.ResultSet;

public class GremlinGraphResultSet implements GraphResultSet {
    private final ResultSet resultSet; 

    public GremlinGraphResultSet(ResultSet gremlinResultSet) {
        this.resultSet = gremlinResultSet;
    }

    @Override
    public boolean hasNext() {
        return resultSet.iterator().hasNext();
    }

    @Override
    public Object next() {
        return resultSet.iterator().next();
    }
}
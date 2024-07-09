package com.alibaba.graphscope.gaia.common;

import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;

import java.util.Iterator;

public class GremlinGraphResultSet implements GraphResultSet {
    private final Iterator<Result> resultIterator;

    public GremlinGraphResultSet(ResultSet gremlinResultSet) {
        this.resultIterator = gremlinResultSet.iterator();
    }

    @Override
    public boolean hasNext() {
        return resultIterator.hasNext();
    }

    @Override
    public Object next() {
        return resultIterator.next();
    }

    @Override
    public Object get() {
        return resultIterator;
    }
}

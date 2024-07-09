package com.alibaba.graphscope.gaia.common;

import org.neo4j.driver.Result;

public class CypherGraphResultSet implements GraphResultSet {
    private final Result resultSet;

    public CypherGraphResultSet(Result cypherResultSet) {
        this.resultSet = cypherResultSet;
    }

    @Override
    public boolean hasNext() {
        return resultSet.hasNext();
    }

    @Override
    public Object next() {
        return resultSet.next().asMap();
    }
}

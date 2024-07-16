package com.alibaba.graphscope.gaia.clients.impls;

import com.alibaba.graphscope.gaia.clients.GraphResultSet;

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
        return resultSet.next().values();
    }
}

package com.alibaba.graphscope.gaia.common;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;

public class CypherGraphClient implements GraphClient {
    private final Session session;

    public CypherGraphClient(String endpoint, String username, String password) {
        String uri = "bolt://" + endpoint;
        Driver driver = GraphDatabase.driver(uri, AuthTokens.basic(username, password));
        this.session = driver.session();
        if (session == null) {
            throw new RuntimeException("Failed to create session with neo4j server");
        }
    }

    @Override
    public GraphResultSet submit(String query) {
        return new CypherGraphResultSet(session.run(query));
    }

    @Override
    public void close() {
        session.close();
    }
}

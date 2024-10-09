/**
 * Copyright 2024 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.gaia.clients.cypher;

import com.alibaba.graphscope.gaia.clients.GraphClient;
import com.alibaba.graphscope.gaia.clients.GraphResultSet;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CypherGraphClient implements GraphClient {
    private final Session session;
    private static Logger logger = LoggerFactory.getLogger(CypherGraphClient.class);

    public CypherGraphClient(String endpoint, String username, String password) {
        String uri = "bolt://" + endpoint;
        Driver driver = GraphDatabase.driver(uri, AuthTokens.basic(username, password));
        this.session = driver.session();
        if (session == null) {
            throw new RuntimeException("Failed to create session with neo4j server");
        }
        logger.info("Connected to neo4j server at " + endpoint);
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

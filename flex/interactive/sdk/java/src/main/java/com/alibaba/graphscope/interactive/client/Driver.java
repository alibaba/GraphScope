/*
 * Copyright 2022 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.alibaba.graphscope.interactive.client;

import com.alibaba.graphscope.interactive.client.common.Result;
import com.alibaba.graphscope.interactive.client.impl.DefaultSession;
import com.alibaba.graphscope.interactive.models.*;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.neo4j.driver.SessionConfig;

import java.util.logging.Logger;

/**
 * The entrypoint for all graphscope operations.
 */
public class Driver {
    private static final Logger logger = Logger.getLogger(Driver.class.getName());

    private final String host;
    private final int port;
    private Session defaultSession;

    public static Driver connect(String host, int port) {
        return new Driver(host + ":" + port);
    }

    public static Driver connect(String uri) {
        return new Driver(uri);
    }

    public static ProcedureInterface procedureOnly(String uri) {
        return DefaultSession.queryServiceOnly(uri);
    }

    private Driver(String uri) {
        // Parse uri
        String[] parts = uri.split(":");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid uri: " + uri);
        }
        String host = parts[0];
        this.port = Integer.parseInt(parts[1]);
        if (host.startsWith("http://")) {
            this.host = host.substring(7);
        } else {
            this.host = host;
        }
    }

    private Driver(String host, int port) {
        this.host = host;
        this.port = port;
    }

    /**
     * Create a GraphScope session
     *
     * @return
     */
    public Session session() {
        return DefaultSession.newInstance(this.host, this.port);
    }

    public Session getDefaultSession() {
        if (defaultSession == null) {
            defaultSession = session();
        }
        return defaultSession;
    }

    /**
     * Create a neo4j session with the given session config.
     *
     * @param sessionConfig
     * @return
     */
    public org.neo4j.driver.Session getNeo4jSession(SessionConfig sessionConfig) {
        return getNeo4jSessionImpl(sessionConfig);
    }

    /**
     * Create a neo4j session with default session config.
     *
     * @return a neo4j session
     */
    public org.neo4j.driver.Session getNeo4jSession() {
        SessionConfig sessionConfig = SessionConfig.builder().build();
        return getNeo4jSessionImpl(sessionConfig);
    }

    public String getNeo4jEndpoint() {
        Pair<String, Integer> endpoint = getNeo4jEndpointImpl();
        if (endpoint == null) {
            return null;
        }
        return "bolt://" + endpoint.getLeft() + ":" + endpoint.getRight();
    }

    /**
     * Get the gremlin endpoint.
     *
     * @return a pair of host and port
     */
    public Pair<String, Integer> getGremlinEndpoint() {
        return getGremlinEndpointImpl();
    }

    public Client getGremlinClient() {
        Pair<String, Integer> endpoint = getGremlinEndpointImpl();
        Cluster cluster =
                Cluster.build()
                        .addContactPoint(endpoint.getLeft())
                        .port(endpoint.getRight())
                        .create();
        return cluster.connect();
    }

    /**
     * Get a neo4j session, user can use this session to execute cypher query.
     * For a more customized session, user can first get the bolt endpoint, and then create a session with the endpoint.
     *
     * @return a neo4j session
     */
    private org.neo4j.driver.Session getNeo4jSessionImpl(SessionConfig sessionConfig) {
        Pair<String, Integer> endpoint = getNeo4jEndpointImpl();
        String boltUri = "neo4j://" + endpoint.getLeft() + ":" + endpoint.getRight();
        logger.info("Connecting to neo4j with uri: " + boltUri);
        org.neo4j.driver.Driver driver = org.neo4j.driver.GraphDatabase.driver(boltUri);
        return driver.session(sessionConfig);
    }

    // TODO(zhangle): return null if bolt is not enabled
    private Pair<String, Integer> getNeo4jEndpointImpl() {
        Session gsSession = getDefaultSession();
        Result<ServiceStatus> serviceStatus = gsSession.getServiceStatus();
        if (!serviceStatus.isOk()) {
            throw new RuntimeException(
                    "Failed to get service status: " + serviceStatus.getStatusMessage());
        } else {
            ServiceStatus status = serviceStatus.getValue();
            Integer boltPort = status.getBoltPort();
            if (!status.getStatus().equals("Running")) {
                throw new RuntimeException("Interactive Query Service is not running");
            }
            // Currently, we assume the host is the same as the gs server
            return Pair.of(host, boltPort);
        }
    }

    // TODO(zhanglei): return null if gremlin is not enabled
    private Pair<String, Integer> getGremlinEndpointImpl() {
        Session gsSession = getDefaultSession();
        Result<ServiceStatus> serviceStatus = gsSession.getServiceStatus();
        if (!serviceStatus.isOk()) {
            throw new RuntimeException(
                    "Failed to get service status: " + serviceStatus.getStatusMessage());
        } else {
            ServiceStatus status = serviceStatus.getValue();
            Integer gremlinPort = status.getGremlinPort();
            // Currently, we assume the host is the same as the gs server
            return Pair.of(host, gremlinPort);
        }
    }
}

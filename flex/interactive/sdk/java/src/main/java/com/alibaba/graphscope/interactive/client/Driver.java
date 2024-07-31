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

import com.alibaba.graphscope.interactive.client.common.Config;
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

    private final String adminUri;
    private final String storedProcUri;
    private String cypherUri;
    private String gremlinUri;
    private String host;
    private int port;
    private Session defaultSession;

    /**
     * Connect to the interactive service with the environment variables
     *  INTERACTIVE_ADMIN_ENDPOINT, INTERACTIVE_STORED_PROC_ENDPOINT, INTERACTIVE_CYPHER_ENDPOINT, INTERACTIVE_GREMLIN_ENDPOINT
     * @return The driver object.
     */
    public static Driver connect() {
        String adminUri = System.getenv("INTERACTIVE_ADMIN_ENDPOINT");
        if (adminUri == null) {
            throw new IllegalArgumentException(
                    "INTERACTIVE_ADMIN_ENDPOINT is not set, did you forget to export the"
                            + " environment variable after deploying Interactive? see"
                            + " https://graphscope.io/docs/latest/flex/interactive/installation");
        }
        String storedProcUri = System.getenv("INTERACTIVE_STORED_PROC_ENDPOINT");
        if (storedProcUri == null) {
            logger.warning(
                    "INTERACTIVE_STORED_PROC_ENDPOINT is not set, will try to parse endpoint from"
                            + " service_status");
        }
        String cypherUri = System.getenv("INTERACTIVE_CYPHER_ENDPOINT");
        if (cypherUri == null) {
            logger.warning(
                    "INTERACTIVE_CYPHER_ENDPOINT is not set, will try to parse endpoint from"
                            + " service_status");
        }
        String gremlinUri = System.getenv("INTERACTIVE_GREMLIN_ENDPOINT");
        if (gremlinUri == null) {
            logger.warning(
                    "INTERACTIVE_GREMLIN_ENDPOINT is not set, will try to parse endpoint from"
                            + " service_status");
        }
        return connect(adminUri, storedProcUri, cypherUri, gremlinUri);
    }

    public static QueryInterface queryServiceOnly(String storedProcUri) {
        return queryServiceOnly(storedProcUri, Config.newBuilder().build());
    }

    public static QueryInterface queryServiceOnly(String storedProcUri, Config config) {
        if (storedProcUri == null || storedProcUri.isEmpty()) {
            throw new IllegalArgumentException("uri is null or empty");
        }
        return DefaultSession.queryInterfaceOnly(storedProcUri, config);
    }

    /**
     * Connect to the interactive service by specifying the URIs of the admin, stored procedure, cypher, and gremlin services.
     * @param adminUri The URI of the admin service.
     * @param storedProcUri The URI of the stored procedure service.
     * @param cypherUri The URI of the cypher service.
     * @param gremlinUri The URI of the gremlin service.
     * @return The driver object.
     */
    public static Driver connect(
            String adminUri, String storedProcUri, String cypherUri, String gremlinUri) {
        return new Driver(adminUri, storedProcUri, cypherUri, gremlinUri);
    }

    /**
     * Should only be used internally. Users should use method connect() or connect(admin_uri,storedProcUri,cypherUri,gremlinUri)
     * which require the URIs of all services, or need all uris exported to the environment variables.
     * Connect to the interactive service by only specifying the admin service's uri.
     * @param uri The URI of the admin service.
     * @return The driver object.
     */
    public static Driver connect(String uri) {
        return new Driver(uri);
    }

    private Driver(String uri) {
        if (uri == null) {
            throw new IllegalArgumentException("Invalid uri is null");
        }
        this.adminUri = uri;
        this.storedProcUri = null;
        this.cypherUri = null;
        this.gremlinUri = null;
        // Parse uri
        initHostPort();
        this.defaultSession = null;
    }

    private Driver(String adminUri, String storedProcUri, String cypherUri, String gremlinUri) {
        this.adminUri = adminUri;
        this.storedProcUri = storedProcUri;
        this.cypherUri = cypherUri;
        this.gremlinUri = gremlinUri;
        // Parse uri
        if (storedProcUri != null && !storedProcUri.startsWith("http")) {
            throw new IllegalArgumentException("Invalid uri: " + storedProcUri);
        }
        initHostPort();
        this.defaultSession = null;
    }

    /**
     * Create a GraphScope session
     *
     * @return
     */
    public Session session() {
        Config config = Config.newBuilder().build();
        return session(config);
    }

    public Session session(Config config) {
        if (storedProcUri == null) {
            return DefaultSession.newInstance(adminUri, config);
        } else {
            return DefaultSession.newInstance(adminUri, storedProcUri, config);
        }
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
        if (cypherUri != null) {
            return cypherUri;
        }
        Pair<String, Integer> endpoint = getNeo4jEndpointImpl();
        if (endpoint == null) {
            return null;
        }
        cypherUri = "bolt://" + endpoint.getLeft() + ":" + endpoint.getRight();
        return cypherUri;
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
        Pair<String, Integer> endpoint = getGremlinEndpoint();
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
        if (gremlinUri != null) {
            // parse host and port from ws://host:port/gremlin
            String[] parts = gremlinUri.split(":");
            if (parts.length != 3) {
                throw new IllegalArgumentException("Invalid uri: " + gremlinUri);
            }
            String host = parts[1].substring(2);
            String portStr = parts[2].split("/")[0];
            Integer port = Integer.parseInt(portStr);
            return Pair.of(host, port);
        }
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

    private void initHostPort() {
        if (adminUri != null) {
            if (!adminUri.startsWith("http")) {
                throw new IllegalArgumentException("Invalid uri: " + adminUri);
            }
            String[] parts = adminUri.split(":");
            if (parts.length != 3) {
                throw new IllegalArgumentException("Invalid uri: " + adminUri);
            }
            host = parts[1].substring(2);
            port = Integer.parseInt(parts[2]);
        }
    }
}

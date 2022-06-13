/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tinkerpop.gremlin.driver;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.URL;
import java.util.List;

public class CloseableGremlinClient implements AutoCloseable {
    private Client client;
    private Cluster cluster;

    public CloseableGremlinClient(List<String> hosts, int port, String userName, String password) {
        if (!hosts.isEmpty()) {
            this.cluster = createCluster(hosts, port, userName, password);
            this.client = this.cluster.connect();
        }
    }

    // return a standard gremlin client to submit query
    public Client gremlinClient() {
        checkNotNull(this.client, "should initiate the client with a non-empty host list");
        return this.client;
    }

    @Override
    public void close() {
        if (this.cluster != null) {
            this.cluster.close();
        }
        if (this.client != null) {
            this.client.close();
        }
    }

    private Cluster createCluster(List<String> hosts, int port, String userName, String password) {
        Settings settings = loadSettings("conf/sdk.yaml");
        settings.hosts = hosts;
        settings.port = port;
        if (userName != null) {
            settings.username = userName;
        }
        if (password != null) {
            settings.password = password;
        }
        return getBuilderFromSettings(settings).create();
    }

    private Cluster.Builder getBuilderFromSettings(Settings settings) {
        List<String> hosts = settings.hosts;
        if (hosts == null || hosts.isEmpty())
            throw new IllegalStateException(
                    "At least one value must be specified to the hosts setting");
        String[] points = hosts.toArray(new String[0]);
        Cluster.Builder builder =
                Cluster.build()
                        .addContactPoints(points)
                        .port(settings.port)
                        .path(settings.path)
                        .enableSsl(settings.connectionPool.enableSsl)
                        .keepAliveInterval(settings.connectionPool.keepAliveInterval)
                        .keyStore(settings.connectionPool.keyStore)
                        .keyStorePassword(settings.connectionPool.keyStorePassword)
                        .keyStoreType(settings.connectionPool.keyStoreType)
                        .trustStore(settings.connectionPool.trustStore)
                        .trustStorePassword(settings.connectionPool.trustStorePassword)
                        .trustStoreType(settings.connectionPool.trustStoreType)
                        .sslCipherSuites(settings.connectionPool.sslCipherSuites)
                        .sslEnabledProtocols(settings.connectionPool.sslEnabledProtocols)
                        .sslSkipCertValidation(settings.connectionPool.sslSkipCertValidation)
                        .nioPoolSize(settings.nioPoolSize)
                        .workerPoolSize(settings.workerPoolSize)
                        .reconnectInterval(settings.connectionPool.reconnectInterval)
                        .resultIterationBatchSize(settings.connectionPool.resultIterationBatchSize)
                        .channelizer(settings.connectionPool.channelizer)
                        .maxContentLength(settings.connectionPool.maxContentLength)
                        .maxWaitForConnection(settings.connectionPool.maxWaitForConnection)
                        .maxInProcessPerConnection(
                                settings.connectionPool.maxInProcessPerConnection)
                        .minInProcessPerConnection(
                                settings.connectionPool.minInProcessPerConnection)
                        .maxSimultaneousUsagePerConnection(
                                settings.connectionPool.maxSimultaneousUsagePerConnection)
                        .minSimultaneousUsagePerConnection(
                                settings.connectionPool.minSimultaneousUsagePerConnection)
                        .maxConnectionPoolSize(settings.connectionPool.maxSize)
                        .minConnectionPoolSize(settings.connectionPool.minSize)
                        .connectionSetupTimeoutMillis(
                                settings.connectionPool.connectionSetupTimeoutMillis)
                        .validationRequest(settings.connectionPool.validationRequest)
                        .loadBalancingStrategy(new LoadBalancingStrategy.RoundRobin());

        if (settings.username != null && settings.password != null)
            builder.credentials(settings.username, settings.password);

        if (settings.jaasEntry != null) builder.jaasEntry(settings.jaasEntry);

        if (settings.protocol != null) builder.protocol(settings.protocol);

        try {
            builder.serializer(settings.serializer.create());
        } catch (Exception ex) {
            throw new IllegalStateException("Could not establish serializer - " + ex.getMessage());
        }

        return builder;
    }

    private Settings loadSettings(String resourceConfig) {
        ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();
        URL resource = currentClassLoader.getResource(resourceConfig);
        File resourceFile = new File(resource.getFile());
        try {
            return Settings.read(new FileInputStream(resourceFile));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(
                    String.format("Configuration file at %s does not exist", resourceConfig));
        }
    }
}

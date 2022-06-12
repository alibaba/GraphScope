package org.apache.tinkerpop.gremlin.driver;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.URL;
import java.util.List;

public class CloseableGremlinClient implements AutoCloseable {
    private Client client;
    private Cluster cluster;

    public CloseableGremlinClient(List<String> hosts, int port,
                                  String userName, String password) {
        this.cluster = createCluster(hosts, port, userName, password);
        this.client = this.cluster.connect();
    }

    public Client gremlinClient() {
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

    private Cluster createCluster(List<String> hosts, int port,
                                  String userName, String password) {
        Settings settings = loadSettings("conf/sdk.yaml");
        settings.hosts = hosts;
        settings.port = port;
        settings.username = userName;
        settings.password = password;
        return getBuilderFromSettings(settings).create();
    }

    private Cluster.Builder getBuilderFromSettings(Settings settings) {
        List<String> hosts = settings.hosts;
        if (hosts == null || hosts.isEmpty())
            throw new IllegalStateException("At least one value must be specified to the hosts setting");
        String[] points = hosts.toArray(new String[0]);
        Cluster.Builder builder = Cluster.build()
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
                .maxInProcessPerConnection(settings.connectionPool.maxInProcessPerConnection)
                .minInProcessPerConnection(settings.connectionPool.minInProcessPerConnection)
                .maxSimultaneousUsagePerConnection(settings.connectionPool.maxSimultaneousUsagePerConnection)
                .minSimultaneousUsagePerConnection(settings.connectionPool.minSimultaneousUsagePerConnection)
                .maxConnectionPoolSize(settings.connectionPool.maxSize)
                .minConnectionPoolSize(settings.connectionPool.minSize)
                .connectionSetupTimeoutMillis(settings.connectionPool.connectionSetupTimeoutMillis)
                .validationRequest(settings.connectionPool.validationRequest);

        if (settings.username != null && settings.password != null)
            builder.credentials(settings.username, settings.password);

        if (settings.jaasEntry != null)
            builder.jaasEntry(settings.jaasEntry);

        if (settings.protocol != null)
            builder.protocol(settings.protocol);

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
            throw new RuntimeException(String.format("Configuration file at %s does not exist", resourceConfig));
        }
    }
}

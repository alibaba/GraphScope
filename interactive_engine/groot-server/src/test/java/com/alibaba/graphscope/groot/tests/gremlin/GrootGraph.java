/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.groot.tests.gremlin;

import com.alibaba.graphscope.compiler.api.exception.GrootException;
import com.alibaba.graphscope.gremlin.integration.graph.RemoteTestGraph;
import com.alibaba.graphscope.gremlin.plugin.traversal.IrCustomizedTraversalSource;
import com.alibaba.graphscope.groot.common.config.CommonConfig;
import com.alibaba.graphscope.groot.common.config.Configs;
import com.alibaba.graphscope.groot.common.config.GremlinConfig;
import com.alibaba.graphscope.groot.sdk.GrootClient;
import com.alibaba.graphscope.groot.servers.MaxNode;
import com.alibaba.graphscope.groot.servers.NodeBase;
import com.alibaba.graphscope.sdkcommon.io.GrootIORegistry;

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV1d0;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.remote.RemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.finalization.ProfileStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.FilterRankingStrategy;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class GrootGraph extends RemoteTestGraph {
    private static final Logger logger = LoggerFactory.getLogger(GrootGraph.class);

    public static GrootGraph INSTANCE = null;

    private NodeBase maxNode;
    private RemoteConnection remoteConnection;
    private Cluster cluster;

    private GrootClient ddlClient;

    public GrootGraph(Configs configs) {
        super(null);
        try {
            this.maxNode = new MaxNode(configs);
            this.maxNode.start();
            // This is to ensure the frontend can communicate some RPC after start, to make the
            // graphdef not null anymore, in snapshotCache.
            Thread.sleep(3000);
            int port = GremlinConfig.GREMLIN_PORT.get(configs);
            this.cluster = createCluster("localhost", port);
            this.ddlClient = GrootClient.newBuilder().addHost("localhost", 55556).build();
            this.remoteConnection = DriverRemoteConnection.using(cluster);
        } catch (Throwable e) {
            this.closeGraph();
            throw new GrootException(e);
        }
    }

    public static GrootGraph open(final Configuration conf) throws Exception {
        if (INSTANCE == null) {
            logger.info("open new MaxTestGraph");
            String log4rsPath =
                    Paths.get(
                                    Thread.currentThread()
                                            .getContextClassLoader()
                                            .getResource("log4rs.yml")
                                            .toURI())
                            .toString();
            Configs.Builder builder = Configs.newBuilder();
            conf.getKeys().forEachRemaining((k) -> builder.put(k, conf.getString(k)));
            Configs configs = builder.put(CommonConfig.LOG4RS_CONFIG.getKey(), log4rsPath).build();
            INSTANCE = new GrootGraph(configs);
        }
        return INSTANCE;
    }

    private Cluster createCluster(String ip, int port) {
        GryoMapper.Builder kryo = GryoMapper.build().addRegistry(GrootIORegistry.instance());
        MessageSerializer serializer = new GryoMessageSerializerV1d0(kryo);
        return Cluster.build()
                .maxContentLength(65536000)
                .addContactPoint(ip)
                .port(port)
                .keepAliveInterval(60000)
                .serializer(serializer)
                .create();
    }

    private void dropData() {
        this.ddlClient.dropSchema();
    }

    public void loadSchema(LoadGraphWith.GraphData graphData)
            throws URISyntaxException, IOException {
        String schemaResource = graphData.name().toLowerCase() + ".schema";
        Path path =
                Paths.get(
                        Thread.currentThread()
                                .getContextClassLoader()
                                .getResource(schemaResource)
                                .toURI());
        String json = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
        this.ddlClient.loadJsonSchema(json);
    }

    public void loadData(LoadGraphWith.GraphData graphData) throws InterruptedException {
        Thread.sleep(5000);
        String graphName = graphData.name().toLowerCase();
        if (graphName.equals("modern")) {
            ddlClient.initWriteSession();

            Map<String, String> v1 = new HashMap<>();
            v1.put("id", "1");
            v1.put("name", "marko");
            v1.put("age", "29");
            ddlClient.addVertex("person", v1);

            Map<String, String> v2 = new HashMap<>();
            v2.put("id", "2");
            v2.put("name", "vadas");
            v2.put("age", "27");
            ddlClient.addVertex("person", v2);

            Map<String, String> v4 = new HashMap<>();
            v4.put("id", "4");
            v4.put("name", "josh");
            v4.put("age", "32");
            ddlClient.addVertex("person", v4);

            Map<String, String> v6 = new HashMap<>();
            v6.put("id", "6");
            v6.put("name", "peter");
            v6.put("age", "35");
            ddlClient.addVertex("person", v6);

            Map<String, String> v3 = new HashMap<>();
            v3.put("id", "3");
            v3.put("name", "lop");
            v3.put("lang", "java");
            ddlClient.addVertex("software", v3);

            Map<String, String> v5 = new HashMap<>();
            v5.put("id", "5");
            v5.put("name", "ripple");
            v5.put("lang", "java");
            ddlClient.addVertex("software", v5);

            ddlClient.commit();
            Thread.sleep(5000);

            ddlClient.addEdge(
                    "knows",
                    "person",
                    "person",
                    Collections.singletonMap("id", "1"),
                    Collections.singletonMap("id", "2"),
                    Collections.singletonMap("weight", "0.5"));

            ddlClient.addEdge(
                    "created",
                    "person",
                    "software",
                    Collections.singletonMap("id", "1"),
                    Collections.singletonMap("id", "3"),
                    Collections.singletonMap("weight", "0.4"));

            ddlClient.addEdge(
                    "knows",
                    "person",
                    "person",
                    Collections.singletonMap("id", "1"),
                    Collections.singletonMap("id", "4"),
                    Collections.singletonMap("weight", "1.0"));

            ddlClient.addEdge(
                    "created",
                    "person",
                    "software",
                    Collections.singletonMap("id", "4"),
                    Collections.singletonMap("id", "3"),
                    Collections.singletonMap("weight", "0.4"));

            ddlClient.addEdge(
                    "created",
                    "person",
                    "software",
                    Collections.singletonMap("id", "4"),
                    Collections.singletonMap("id", "5"),
                    Collections.singletonMap("weight", "1.0"));

            ddlClient.addEdge(
                    "created",
                    "person",
                    "software",
                    Collections.singletonMap("id", "6"),
                    Collections.singletonMap("id", "3"),
                    Collections.singletonMap("weight", "0.2"));

            ddlClient.commit();
            Thread.sleep(5000);
        } else {
            throw new UnsupportedOperationException("graph " + graphName + " is unsupported yet");
        }
    }

    @Override
    public GraphTraversalSource traversal() {
        GraphTraversalSource source =
                AnonymousTraversalSource.traversal(IrCustomizedTraversalSource.class)
                        .withRemote(remoteConnection);
        source.getStrategies().removeStrategies(ProfileStrategy.class, FilterRankingStrategy.class);
        return source;
    }

    @Override
    public Vertex addVertex(Object... keyValues) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <C extends GraphComputer> C compute(Class<C> graphComputerClass)
            throws IllegalArgumentException {
        throw new UnsupportedOperationException();
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<Vertex> vertices(Object... vertexIds) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<Edge> edges(Object... edgeIds) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Transaction tx() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        dropData();
    }

    private void closeGraph() {
        logger.info("close MaxTestGraph");
        if (this.remoteConnection != null) {
            try {
                this.remoteConnection.close();
            } catch (Exception e) {
                logger.error("close remote connection failed", e);
            }
        }
        if (this.cluster != null) {
            this.cluster.close();
        }
        if (this.maxNode != null) {
            try {
                this.maxNode.close();
            } catch (IOException e) {
                logger.error("close maxNode failed", e);
            }
        }
    }

    @Override
    public Variables variables() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Configuration configuration() {
        return null;
    }

    public MaxNode getMaxNode() {
        return (MaxNode) maxNode;
    }
}

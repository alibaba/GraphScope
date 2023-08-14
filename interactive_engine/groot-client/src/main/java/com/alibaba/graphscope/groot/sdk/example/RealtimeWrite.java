package com.alibaba.graphscope.groot.sdk.example;

import com.alibaba.graphscope.groot.sdk.GrootClient;
import com.alibaba.graphscope.groot.sdk.schema.*;
import com.alibaba.graphscope.proto.groot.DataTypePb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class RealtimeWrite {
    private static long recordNum = 10;

    public void initSchema(GrootClient client) {
        VertexLabel.Builder person = VertexLabel.newBuilder();
        person.setLabel("person");
        Property id =
                Property.newBuilder()
                        .setName("id")
                        .setDataType(DataTypePb.LONG)
                        .setPrimaryKey()
                        .build();
        Property.Builder name =
                Property.newBuilder().setName("name").setDataType(DataTypePb.STRING);
        Property.Builder age = Property.newBuilder().setName("age").setDataType(DataTypePb.INT);
        person.addProperty(id);
        person.addProperty(name);
        person.addProperty(age);

        VertexLabel.Builder software = VertexLabel.newBuilder();
        Property.Builder lang = Property.newBuilder().setName("lang").setDataType(DataTypePb.STRING);

        software.setLabel("software");
        software.addProperty(id);
        software.addProperty(name);
        software.addProperty(lang);

        EdgeLabel.Builder created = EdgeLabel.newBuilder();
        created.setLabel("created");
        created.addRelation("person", "software");
        Property.Builder weight = Property.newBuilder().setName("weight").setDataType(DataTypePb.LONG);
        created.addProperty(weight);

        Schema.Builder schema = Schema.newBuilder();
        schema.addVertexLabel(person);
        schema.addVertexLabel(software);
        schema.addEdgeLabel(created);

        System.out.println(client.submitSchema(schema));
        System.out.println("testAddLabel succeed");
    }

    private static void testAddVerticesEdges(GrootClient client) {
        for (int i = 0; i < 10; ++i) {
            Map<String, String> properties = new HashMap<>();
            properties.put("id", String.valueOf(i));
            properties.put("name", "person-" + i);
            properties.put("age", String.valueOf(i + 20));
            client.addVertex(new Vertex("person", properties));

            properties.clear();
            properties.put("id", String.valueOf(i));
            properties.put("name", "software-" + i);
            properties.put("lang", String.valueOf(i + 200));
            client.addVertex(new Vertex("software", properties));
        }
        long snapshotId = 0;
        for (int i = 0; i < 10; ++i) {
            Map<String, String> srcPk = new HashMap<>();
            Map<String, String> dstPk = new HashMap<>();
            Map<String, String> properties = new HashMap<>();

            srcPk.put("id", String.valueOf(i));
            dstPk.put("id", String.valueOf(i));
            properties.put("weight", String.valueOf(i * 100));
            snapshotId = client.addEdge(new Edge("created", "person", "software", srcPk, dstPk, properties));
        }
        client.remoteFlush(snapshotId);
        System.out.println("Finished adding vertices and edges");
    }

    private static void testUpdateDeleteEdge(GrootClient client) {
        Map<String, String> srcPk = new HashMap<>();
        Map<String, String> dstPk = new HashMap<>();
        Map<String, String> properties = new HashMap<>();

        srcPk.put("id", String.valueOf(0));
        dstPk.put("id", String.valueOf(0));
        properties.put("weight", String.valueOf(10000));
        long snapshotId = client.updateEdge(new Edge("created", "person", "software", srcPk, dstPk, properties));
        client.remoteFlush(snapshotId);
        System.out.println("Finished update edge person-0 -> software-0");

        client.deleteEdge(new Edge("created", "person", "software", srcPk, dstPk));
        client.remoteFlush(snapshotId);
        System.out.println("Finished delete edge person-0 -> software-0");
    }

    private static void testUpdateDeleteVertex(GrootClient client) {
        Map<String, String> properties = new HashMap<>();
        properties.put("id", String.valueOf(0));
        properties.put("name", "marko-0-updated");
        long snapshotId = client.updateVertex(new Vertex("person", properties));
        client.remoteFlush(snapshotId);
        System.out.println("Finished update vertex person-0");

        Map<String, String> pk_properties = new HashMap<>();
        client.deleteVertex(new Vertex("person", pk_properties));
        System.out.println("Finished delete vertex person-0");
    }

    private static List<Vertex> getVerticesA() {
        List<Vertex> vertices = new ArrayList<>();
        for (int i = 0; i < recordNum; ++i) {
            Map<String, String> properties = new HashMap<>();
            properties.put("id", String.valueOf(i));
            properties.put("name", "person-" + i);
            properties.put("age", String.valueOf(i + 20));
            vertices.add(new Vertex("person", properties));
        }
        return vertices;
    }
    private static List<Vertex> getVerticesB() {
        List<Vertex> vertices = new ArrayList<>();
        for (int i = 0; i < recordNum; ++i) {
            Map<String, String> properties = new HashMap<>();
            properties.put("id", String.valueOf(i));
            properties.put("name", "software-" + i);
            properties.put("lang", String.valueOf(i + 200));
            vertices.add(new Vertex("person", properties));
        }
        return vertices;
    }
    private static List<Edge> getEdges() {
        List<Edge> edges = new ArrayList<>();
        for (int i = 0; i < recordNum; ++i) {
            Map<String, String> srcPk = new HashMap<>();
            Map<String, String> dstPk = new HashMap<>();
            Map<String, String> properties = new HashMap<>();

            srcPk.put("id", String.valueOf(i));
            dstPk.put("id", String.valueOf(i));
            properties.put("weight", String.valueOf(i * 100));
            edges.add(new Edge("created", "person", "software", srcPk, dstPk, properties));
        }
        return edges;
    }

    class ClientTask implements Runnable {

        private GrootClient client;

        ClientTask(GrootClient client) {
            this.client = client;
        }

        @Override
        public void run() {
            System.out.println("x");
        }
    }

    public void submitAsync(GrootClient client) throws InterruptedException {
        // Create thread pool with 10 threads
        ExecutorService executor = Executors.newFixedThreadPool(10);

        // Submit 10 tasks to call submit()
        for(int i=0; i<10; i++) {
            executor.submit(new ClientTask(client));
        }

        // Shut down thread pool
        executor.shutdown();

        // Wait for tasks to complete
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    }

    public static void main(String[] args) throws InterruptedException {
        String hosts = "localhost";
        int port = 55556;
        GrootClient client = GrootClient.newBuilder().addHost(hosts, port).build();

        RealtimeWrite writer = new RealtimeWrite();
        long snapshotId = 0;

        writer.initSchema(client);
        List<Vertex> vertices = RealtimeWrite.getVerticesA();
        snapshotId = client.addVertices(vertices);
        vertices = RealtimeWrite.getVerticesB();
        snapshotId = client.addVertices(vertices);
        List<Edge> edges = RealtimeWrite.getEdges();
        snapshotId = client.addEdges(edges);
        client.remoteFlush(snapshotId);
        System.out.println("Finished add edges");
//        writer.submitAsync(client);
    }
}

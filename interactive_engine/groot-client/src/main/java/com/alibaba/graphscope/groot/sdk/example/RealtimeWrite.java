package com.alibaba.graphscope.groot.sdk.example;

import com.alibaba.graphscope.groot.sdk.GrootClient;
import com.alibaba.graphscope.groot.sdk.schema.*;
import com.alibaba.graphscope.proto.groot.BatchWriteResponse;
import com.alibaba.graphscope.proto.groot.DataTypePb;

import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class RealtimeWrite {
    private static int startId = 0;
    private static int recordNum = 10000 + startId;

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
        Property.Builder lang =
                Property.newBuilder().setName("lang").setDataType(DataTypePb.STRING);

        software.setLabel("software");
        software.addProperty(id);
        software.addProperty(name);
        software.addProperty(lang);

        EdgeLabel.Builder created = EdgeLabel.newBuilder();
        created.setLabel("created");
        created.addRelation("person", "software");
        Property.Builder weight =
                Property.newBuilder().setName("weight").setDataType(DataTypePb.LONG);
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
            snapshotId =
                    client.addEdge(
                            new Edge("created", "person", "software", srcPk, dstPk, properties));
        }
        client.remoteFlush(snapshotId);
        System.out.println("Finished adding vertices and edges");
    }

    private static void testUpdateAndDeleteEdge(GrootClient client) {
        Map<String, String> srcPk = new HashMap<>();
        Map<String, String> dstPk = new HashMap<>();
        Map<String, String> properties = new HashMap<>();

        srcPk.put("id", String.valueOf(0));
        dstPk.put("id", String.valueOf(0));
        properties.put("weight", String.valueOf(10000));
        long snapshotId =
                client.updateEdge(
                        new Edge("created", "person", "software", srcPk, dstPk, properties));
        client.remoteFlush(snapshotId);
        System.out.println("Finished update edge person-0 -> software-0");

        client.deleteEdge(new Edge("created", "person", "software", srcPk, dstPk));
        client.remoteFlush(snapshotId);
        System.out.println("Finished delete edge person-0 -> software-0");
    }

    private static void testUpdateAndDeleteVertex(GrootClient client) {
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

    private static void testClearProperties(GrootClient client) {
        {
            Map<String, String> properties = new HashMap<>();
            properties.put("id", String.valueOf(1));
            properties.put("name", "");
            properties.put("age", "");
            long snapshotId = client.clearVertexProperty(new Vertex("person", properties));
            client.remoteFlush(snapshotId);
            System.out.println("Finished update vertex person-0");
        }
        {
            Map<String, String> srcPk = new HashMap<>();
            Map<String, String> dstPk = new HashMap<>();
            Map<String, String> properties = new HashMap<>();

            srcPk.put("id", String.valueOf(1));
            dstPk.put("id", String.valueOf(2));
            properties.put("weight", "");
            long snapshotId =
                    client.clearEdgeProperty(
                            new Edge("knows", "person", "person", srcPk, dstPk, properties));
            client.remoteFlush(snapshotId);
        }
    }

    private static List<Vertex> getVerticesA() {
        List<Vertex> vertices = new ArrayList<>();
        for (int i = startId; i < recordNum; ++i) {
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
        for (int i = startId; i < recordNum; ++i) {
            Map<String, String> properties = new HashMap<>();
            properties.put("id", String.valueOf(i));
            properties.put("name", "software-" + i);
            properties.put("lang", String.valueOf(i + 200));
            vertices.add(new Vertex("software", properties));
        }
        return vertices;
    }

    private static List<Edge> getEdges() {
        List<Edge> edges = new ArrayList<>();
        for (int i = startId; i < recordNum; ++i) {
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
        private List<Vertex> vertices;
        private List<Edge> edges;

        private int type;

        ClientTask(GrootClient client, int type, List<Vertex> vertices, List<Edge> edges) {
            this.client = client;
            this.type = type;
            this.vertices = vertices;
            this.edges = edges;
        }

        @Override
        public void run() {
            if (type == 0) {
                for (int i = 0; i < vertices.size(); ++i) {
                    client.addVertex(vertices.get(i));
                }
            } else {
                for (int i = 0; i < edges.size(); ++i) {
                    client.addEdge(edges.get(i));
                }
            }
        }
    }

    public void sequential(
            GrootClient client, List<Vertex> verticesA, List<Vertex> verticesB, List<Edge> edges) {
        long snapshotId = 0;
        TimeWatch watch = TimeWatch.start();
        {
            watch.reset();
            for (Vertex vertex : verticesA) {
                snapshotId = client.addVertex(vertex);
            }
            watch.status("VerticesA");
        }
        {
            watch.reset();
            for (Vertex vertex : verticesB) {
                snapshotId = client.addVertex(vertex);
            }
            watch.status("VerticesB");
        }
        {
            watch.reset();
            client.remoteFlush(snapshotId);
            watch.status("Flush Vertices");
            System.out.println("Finished add vertices");
        }
        {
            watch.reset();
            for (Edge edge : edges) {
                snapshotId = client.addEdge(edge);
            }
            watch.status("Edges");
        }
        {
            watch.reset();
            client.remoteFlush(snapshotId);
            watch.status("Flush Edges");
            System.out.println("Finished add edges");
        }
    }

    public void sequentialBatch(
            GrootClient client, List<Vertex> verticesA, List<Vertex> verticesB, List<Edge> edges) {
        long snapshotId = 0;

        TimeWatch watch = TimeWatch.start();

        {
            watch.reset();
            // snapshotId = client.addVertices(vertices);
            for (int i = 0; i < verticesA.size(); i += 1000) {
                snapshotId = client.addVertices(verticesA.subList(i, i + 1000));
            }
            watch.status("VerticesA");
        }
        {
            watch.reset();
            // snapshotId = client.addVertices(vertices);
            for (int i = 0; i < verticesB.size(); i += 1000) {
                snapshotId = client.addVertices(verticesB.subList(i, i + 1000));
            }
            watch.status("VerticesB");
        }
        {
            watch.reset();
            client.remoteFlush(snapshotId);
            watch.status("Flush Vertices");
            System.out.println("Finished add vertices");
        }
        {
            watch.reset();
            // snapshotId = client.addEdges(edges);
            for (int i = 0; i < edges.size(); i += 1000) {
                snapshotId = client.addEdges(edges.subList(i, i + 1000));
            }
            watch.status("Edges");
        }
        {
            watch.reset();
            client.remoteFlush(snapshotId);
            watch.status("Flush Edges");
            System.out.println("Finished add edges");
        }
    }

    public void parallel(
            GrootClient client, List<Vertex> verticesA, List<Vertex> verticesB, List<Edge> edges)
            throws InterruptedException {
        // Create thread pool with 10 threads
        int taskNum = 30;
        int offset = 10000 / taskNum;
        TimeWatch watch = TimeWatch.start();
        {
            ExecutorService executor = Executors.newFixedThreadPool(10);
            // Submit 10 tasks to call submit()

            for (int i = 0; i < taskNum * offset; i += offset) {
                int start = i;
                int end = start + offset;
                List<Vertex> subVerticesA = verticesA.subList(start, end);
                List<Vertex> subVerticesB = verticesB.subList(start, end);
                executor.submit(new ClientTask(client, 0, subVerticesA, null));
                executor.submit(new ClientTask(client, 0, subVerticesB, null));
            }
            executor.shutdown();
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            watch.status("Vertices");
        }
        {
            Thread.sleep(2000);
            ExecutorService executor = Executors.newFixedThreadPool(10);
            watch.reset();
            for (int i = 0; i < taskNum * offset; i += offset) {
                int start = i;
                int end = start + offset;
                List<Edge> subEdges = edges.subList(start, end);
                executor.submit(new ClientTask(client, 1, null, subEdges));
            }
            executor.shutdown();
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            watch.status("Edges");
        }
    }

    public void sequentialAsync(
            GrootClient client, List<Vertex> verticesA, List<Vertex> verticesB, List<Edge> edges)
            throws InterruptedException {
        TimeWatch watch = TimeWatch.start();
        class VertexCallBack implements StreamObserver<BatchWriteResponse> {
            @Override
            public void onNext(BatchWriteResponse value) {
                // System.out.println("on next");
            }

            @Override
            public void onError(Throwable t) {
                // System.out.println("on next");
            }

            @Override
            public void onCompleted() {
                // System.out.println("completed");
            }
        }
        {
            watch.reset();
            for (Vertex vertex : verticesA) {
                client.addVertex(vertex, new VertexCallBack());
            }
            watch.status("VerticesA");
        }
        {
            watch.reset();
            for (Vertex vertex : verticesB) {
                client.addVertex(vertex, new VertexCallBack());
            }
            watch.status("VerticesB");
        }
    }

    public static void main(String[] args) throws InterruptedException {
        String hosts = "localhost";
        int port = 55556;
        GrootClient client = GrootClient.newBuilder().addHost(hosts, port).build();

        RealtimeWrite writer = new RealtimeWrite();

        // client.dropSchema();
        // writer.initSchema(client);

        // List<Vertex> verticesA = RealtimeWrite.getVerticesA();
        // List<Vertex> verticesB = RealtimeWrite.getVerticesB();
        // List<Edge> edges = RealtimeWrite.getEdges();

        TimeWatch watch = TimeWatch.start();
        // writer.sequential(client, verticesA, verticesB, edges);
        // writer.parallel(client, verticesA, verticesB, edges);
        // writer.sequentialBatch(client, verticesA, verticesB, edges);
        //        writer.sequentialAsync(client, verticesA, verticesB, edges);
        // RealtimeWrite.testAddVerticesEdges(client);
        RealtimeWrite.testClearProperties(client);
        watch.status("Total");
    }
}

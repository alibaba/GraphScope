package com.alibaba.graphscope.groot.sdk;

import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.graphscope.groot.schema.GraphDef;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class DataLoadingTest {
    String host = "localhost";
    int port = 55556;
    Client client = new Client(host, port);

    @Test
    public void testLoadSchema() throws URISyntaxException, IOException {
        Path path =
                Paths.get(
                        Thread.currentThread()
                                .getContextClassLoader()
                                .getResource("modern.schema")
                                .toURI());
        String jsonSchemaRes = client.loadJsonSchema(path);
        System.out.println(jsonSchemaRes);
    }

    @Test
    public void testGetSchema() {
        GraphSchema schema = client.getSchema();
        System.out.println(((GraphDef) schema).toProto().toString());
    }

    @Test
    public void testZddData() throws Exception {
        Thread.sleep(5000);

        Map<String, String> v1 = new HashMap<>();
        v1.put("id", "1");
        v1.put("name", "marko");
        v1.put("age", "29");
        client.addVertex("person", v1);

        Map<String, String> v2 = new HashMap<>();
        v2.put("id", "2");
        v2.put("name", "vadas");
        v2.put("age", "27");
        client.addVertex("person", v2);

        Map<String, String> v4 = new HashMap<>();
        v4.put("id", "4");
        v4.put("name", "josh");
        v4.put("age", "32");
        client.addVertex("person", v4);

        Map<String, String> v6 = new HashMap<>();
        v6.put("id", "6");
        v6.put("name", "peter");
        v6.put("age", "35");
        client.addVertex("person", v6);

        Map<String, String> v3 = new HashMap<>();
        v3.put("id", "3");
        v3.put("name", "lop");
        v3.put("lang", "java");
        client.addVertex("software", v3);

        Map<String, String> v5 = new HashMap<>();
        v5.put("id", "5");
        v5.put("name", "ripple");
        v5.put("lang", "java");
        client.addVertex("software", v5);

        Thread.sleep(5000);

        client.addEdge(
                "knows",
                "person",
                "person",
                Collections.singletonMap("id", "1"),
                Collections.singletonMap("id", "2"),
                Collections.singletonMap("weight", "0.5"));

        client.addEdge(
                "created",
                "person",
                "software",
                Collections.singletonMap("id", "1"),
                Collections.singletonMap("id", "3"),
                Collections.singletonMap("weight", "0.4"));

        client.addEdge(
                "knows",
                "person",
                "person",
                Collections.singletonMap("id", "1"),
                Collections.singletonMap("id", "4"),
                Collections.singletonMap("weight", "1.0"));

        client.addEdge(
                "created",
                "person",
                "software",
                Collections.singletonMap("id", "4"),
                Collections.singletonMap("id", "3"),
                Collections.singletonMap("weight", "0.4"));

        client.addEdge(
                "created",
                "person",
                "software",
                Collections.singletonMap("id", "4"),
                Collections.singletonMap("id", "5"),
                Collections.singletonMap("weight", "1.0"));

        client.addEdge(
                "created",
                "person",
                "software",
                Collections.singletonMap("id", "6"),
                Collections.singletonMap("id", "3"),
                Collections.singletonMap("weight", "0.2"));

        client.commit();

        Thread.sleep(5000);
    }
}

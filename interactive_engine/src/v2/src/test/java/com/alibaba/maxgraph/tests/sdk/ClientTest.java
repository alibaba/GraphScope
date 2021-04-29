package com.alibaba.maxgraph.tests.sdk;

import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.sdk.Client;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ClientTest {

    String host = "localhost";
    int port = 52944;

    @Test
    void testLoadSchema() throws URISyntaxException, IOException {
        Client client = new Client(host, port);
        Path path = Paths.get(Thread.currentThread().getContextClassLoader().getResource("ldbc.schema").toURI());
        String jsonSchemaRes = client.loadJsonSchema(path);
        System.out.println(jsonSchemaRes);
    }

    @Test
    void testGetSchema() {
        Client client = new Client(host, port);
        GraphSchema schema = client.getSchema();
        schema.toString();
    }

    @Test
    void testAddData() {
        Client client = new Client(host, port);
        Map<String, String> properties = new HashMap<>();
        properties.put("firstname", "alice");
        properties.put("id", "12345");
        client.addVertex("person", properties);
        properties = new HashMap<>();
        properties.put("firstname", "bob");
        properties.put("id", "88888");
        client.addVertex("person", properties);

        for (int i = 0; i < 100; i++) {
            properties = new HashMap<>();
            properties.put("firstname", "test" + i);
            properties.put("id", "" + i);
            client.addVertex("person", properties);
        }

        client.addEdge("person_knows_person",
                "person",
                "person",
                Collections.singletonMap("id", "12345"),
                Collections.singletonMap("id", "88888"),
                Collections.singletonMap("creationdate", "20201111"));
        client.commit();
    }
}

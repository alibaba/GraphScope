package com.alibaba.maxgraph.tests.sdk;

import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.sdk.Client;
import com.alibaba.maxgraph.v2.sdk.DataLoadTarget;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ClientTest {

    String host = "100.81.128.150";
    int port = 50389;
    Client client = new Client(host, port);

    @Test
    void testIngestData() {
        String path = "hdfs://100.69.96.93:9000/user/tianli/data/build_1g_p8";
        client.ingestData(path);
    }

    @Test
    void testCommitData() {
        long tableId = -4611686018427387815L;
        DataLoadTarget target = DataLoadTarget.newBuilder()
                .setLabel("person")
                .build();
        client.commitDataLoad(Collections.singletonMap(tableId, target));
    }

    @Test
    void testLoadSchema() throws URISyntaxException, IOException {
        Path path = Paths.get(Thread.currentThread().getContextClassLoader().getResource("schema.json").toURI());
        String jsonSchemaRes = client.loadJsonSchema(path);
        System.out.println(jsonSchemaRes);
    }

    @Test
    void testGetSchema() {
        GraphSchema schema = client.getSchema();
        schema.toString();
    }

    @Test
    void testAddData() {
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
                Collections.singletonMap("creationDate", "20201111"));
        client.commit();
    }
}

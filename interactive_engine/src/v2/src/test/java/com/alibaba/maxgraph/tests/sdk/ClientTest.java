package com.alibaba.maxgraph.tests.sdk;

import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.sdk.Client;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ClientTest {

    String host = "localhost";
    int port = 60885;

    @Test
    void testLoadSchema() throws URISyntaxException, IOException {
        Client client = new Client("localhost", 60885);
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
}

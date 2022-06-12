package com.alibaba.graphscope.groot.sdk;

import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.util.*;

public class GremlinClientTest {
    String host = "localhost";
    int gremlinPort = 8182;
    MaxGraphClient client = MaxGraphClient.newBuilder()
            .addGremlinHost(host)
            .setGremlinPort(gremlinPort)
            .build();

    @Test
    void submitQuery() {
        ResultSet resultSet = client.submitQuery("g.V()");
        Iterator<Result> resultIterator = resultSet.iterator();
        List<String> labels = Arrays.asList("person", "software");
        while (resultIterator.hasNext()) {
            Result result = resultIterator.next();
            Vertex vertex = result.getVertex();
            Assert.assertTrue(labels.contains(vertex.label()));
        }
        client.close();
    }
}

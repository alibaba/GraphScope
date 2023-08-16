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
package com.alibaba.graphscope.groot.sdk;

import com.alibaba.graphscope.groot.sdk.schema.Edge;
import com.alibaba.graphscope.groot.sdk.schema.Vertex;
import com.alibaba.graphscope.proto.groot.GraphDefPb;

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
    int port = 55556;
    GrootClient client = GrootClient.newBuilder().addHost(host, port).build();

    //    @Test
    //    void testIngestData() {
    //        String path = "hdfs://host:port/path";
    //        client.ingestData(path);
    //    }
    //
    //    @Test
    //    void testCommitData() {
    //        long tableId = -4611686018427387871L;
    //        DataLoadTargetPb target = DataLoadTargetPb.newBuilder().setLabel("person").build();
    //        client.commitDataLoad(Collections.singletonMap(tableId, target), "");
    //    }

    @Test
    void testLoadSchema() throws URISyntaxException, IOException {
        Path path =
                Paths.get(
                        Thread.currentThread()
                                .getContextClassLoader()
                                .getResource("schema.json")
                                .toURI());
        String jsonSchemaRes = client.loadJsonSchema(path);
        System.out.println(jsonSchemaRes);
    }

    @Test
    void testDropSchema() {
        GraphDefPb res = client.dropSchema();
        System.out.println(res);
    }

    @Test
    void testGetSchema() {
        GraphDefPb schema = client.getSchema();
        System.out.println(schema.toString());
    }

    @Test
    void testAddData() {
        Map<String, String> properties = new HashMap<>();
        properties.put("name", "alice");
        properties.put("id", "12345");
        client.addVertex(new Vertex("person", properties));
        properties = new HashMap<>();
        properties.put("name", "bob");
        properties.put("id", "88888");
        client.addVertex(new Vertex("person", properties));

        for (int i = 0; i < 100; i++) {
            properties = new HashMap<>();
            properties.put("name", "test" + i);
            properties.put("id", "" + i);
            client.addVertex(new Vertex("person", properties));
        }

        client.addEdge(
                new Edge(
                        "knows",
                        "person",
                        "person",
                        Collections.singletonMap("id", "12345"),
                        Collections.singletonMap("id", "88888"),
                        Collections.singletonMap("weight", "20201111")));
    }
}

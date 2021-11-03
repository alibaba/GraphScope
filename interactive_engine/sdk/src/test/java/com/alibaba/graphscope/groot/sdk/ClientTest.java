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

import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.graphscope.groot.schema.GraphDef;
import com.alibaba.maxgraph.sdkcommon.common.DataLoadTarget;
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
    Client client = new Client(host, port);

    @Test
    void testIngestData() {
        String path = "hdfs://100.69.96.93:9000/user/tianli/data/build_1g_p8";
        client.ingestData(path);
    }

    @Test
    void testCommitData() {
        long tableId = -4611686018427387871L;
        DataLoadTarget target = DataLoadTarget.newBuilder().setLabel("person").build();
        client.commitDataLoad(Collections.singletonMap(tableId, target));
    }

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
    void testGetSchema() {
        GraphSchema schema = client.getSchema();
        System.out.println(((GraphDef) schema).toProto().toString());
    }

    @Test
    void testAddData() {
        Map<String, String> properties = new HashMap<>();
        properties.put("firstName", "alice");
        properties.put("id", "12345");
        client.addVertex("person", properties);
        properties = new HashMap<>();
        properties.put("firstName", "bob");
        properties.put("id", "88888");
        client.addVertex("person", properties);

        for (int i = 0; i < 100; i++) {
            properties = new HashMap<>();
            properties.put("firstName", "test" + i);
            properties.put("id", "" + i);
            client.addVertex("person", properties);
        }

        client.addEdge(
                "knows",
                "person",
                "person",
                Collections.singletonMap("id", "12345"),
                Collections.singletonMap("id", "88888"),
                Collections.singletonMap("creationDate", "20201111"));
        client.commit();
    }
}

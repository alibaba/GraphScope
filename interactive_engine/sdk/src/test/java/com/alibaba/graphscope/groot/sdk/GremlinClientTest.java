/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
    GrootClient client =
            GrootClient.newBuilder().addGremlinHost(host).setGremlinPort(gremlinPort).build();

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

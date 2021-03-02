/**
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
package com.compiler.demo.server;

import org.apache.tinkerpop.gremlin.driver.*;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.ser.GraphSONMessageSerializerV1d0;

import java.util.concurrent.CompletableFuture;

public class SubmitQueryTest {
    public static void main(String[] args) throws Exception {
        MessageSerializer serializer = new GraphSONMessageSerializerV1d0();
        Cluster cluster = Cluster.build()
                .addContactPoint("localhost")
                .port(8182)
                .credentials("admin", "admin")
                .serializer(serializer)
                .create();
        Client client = cluster.connect();
        String query = "g.V().as(\"a\").out().select(\"a\").by(\"id\")";
        RequestMessage request = RequestMessage
                .build(Tokens.OPS_EVAL)
                .add(Tokens.ARGS_GREMLIN, query).create();
        CompletableFuture<ResultSet> results = client.submitAsync(request);
    }
}

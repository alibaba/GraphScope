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
package com.compiler.demo.server;

import org.apache.tinkerpop.gremlin.driver.*;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.ser.GraphBinaryMessageSerializerV1;

import java.util.concurrent.CompletableFuture;

public class SubmitQueryTest {
    public static void main(String[] args) throws Exception {
        MessageSerializer serializer = new GraphBinaryMessageSerializerV1();
        Cluster cluster = Cluster.build()
                .addContactPoint("localhost")
                .port(8182)
                .credentials("admin", "admin")
                .serializer(serializer)
                .create();
        Client client = cluster.connect();
        String query = "g.V().hasLabel(\"PERSON\").has(\"id\",30786325583618).both(\"PERSON_KNOWS_PERSON\")";
        RequestMessage request = RequestMessage
                .build(Tokens.OPS_EVAL)
                .add(Tokens.ARGS_GREMLIN, query).processor("plan").create();
        CompletableFuture<ResultSet> resultSet = client.submitAsync(request);
        while (!resultSet.isDone()) {
            Thread.sleep(100);
        }
        String content = resultSet.get().one().getString();
        System.out.println(content);
        client.close();
        cluster.close();
    }
}

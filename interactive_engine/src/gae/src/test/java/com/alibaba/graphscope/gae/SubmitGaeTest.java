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
package com.alibaba.graphscope.gae;

import org.apache.tinkerpop.gremlin.driver.*;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV1d0;

import java.net.URI;
import java.util.concurrent.CompletableFuture;

public class SubmitGaeTest {
    public static void main(String[] args) throws Exception {
        MessageSerializer serializer = new GryoMessageSerializerV1d0();
        // new File(getResource("gremlin-sdk.yaml"))
        Cluster cluster = Cluster.build()
                .addContactPoint("localhost")
                .port(8183)
                .credentials("admin", "admin")
                .serializer(serializer)
                .create();
        Client client = cluster.connect();
        String query = "g.V().hasLabel(\"person\").process(\n" +
                "   V().property('$pr', expr('1.0/TOTAL_V')) \n" +
                "      .repeat( \n" +
                "         V().property('$tmp', expr('$pr/OUT_DEGREE')) \n" +
                "         .scatter('$tmp').by(out())\n" +
                "         .gather('$tmp', sum) \n" +
                "         .property('$new', expr('0.15/TOTAL_V+0.85*$tmp')) \n" +
                "         .where(expr('abs($new-$pr)>1e-10')) \n" +
                "         .property('$pr', expr('$new')))\n" +
                "      .until(count().is(0)) \n" +
                "   ).with('$pr', 'pr') \n" +
                "   .order().by('pr', desc).limit(10) \n";
        RequestMessage request = RequestMessage
                .build(Tokens.OPS_EVAL)
                .add(Tokens.ARGS_GREMLIN, query)
                .processor("gae")
                .create();
        CompletableFuture<ResultSet> resultSet = client.submitAsync(request);
        while (!resultSet.isDone()) {
            Thread.sleep(100);
        }
        String content = resultSet.get().one().getString();
        System.out.println(content);
        client.close();
        cluster.close();
    }

    public static URI getResource(String resourceName) throws Exception {
        return Thread.currentThread().getContextClassLoader().getResource(resourceName).toURI();
    }
}

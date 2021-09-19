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
package com.alibaba.graphscope.gaia;

import org.apache.tinkerpop.gremlin.driver.*;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV1d0;

import java.net.URI;
import java.util.concurrent.CompletableFuture;

public class SubmitQueryTest {
    public static void main(String[] args) throws Exception {
        MessageSerializer serializer = new GryoMessageSerializerV1d0();
        // new File(getResource("gremlin-sdk.yaml"))
        Cluster cluster = Cluster.build()
                .addContactPoint("localhost")
                .port(8182)
                .credentials("admin", "admin")
                .serializer(serializer)
                .create();
        Client client = cluster.connect();
        String query = "g.V().hasLabel('PERSON').has('id',28587302327593).both('KNOWS').as('p').in('COMMENT_HASCREATOR_PERSON', 'POST_HASCREATOR_PERSON').has('creationDate',lte(20120301080000000)).order().by('creationDate',desc).by('id',asc).limit(20).as('m').select('p', 'm')";
        RequestMessage request = RequestMessage
                .build(Tokens.OPS_EVAL)
                .add(Tokens.ARGS_GREMLIN, query)
                // .processor("plan")
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

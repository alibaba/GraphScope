/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.common.util;

import com.alibaba.maxgraph.sdkcommon.client.Endpoint;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV1d0;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class QueryUtils {
    private static final Logger LOG = LoggerFactory.getLogger(QueryUtils.class);

    public static final long DEFAULT_TIMEOUT_SECS = 300;

    public static TinkerpopClusterAndClient initTinkerpopConnect(Endpoint frontendEndpoint, Pair<String, String> account) {
        final MessageSerializer serializer = new GryoMessageSerializerV1d0();
        final Map<String, Object> config = new HashMap<String, Object>() {{
            put(GryoMessageSerializerV1d0.TOKEN_SERIALIZE_RESULT_TO_STRING, true);
        }};
        Cluster.Builder builder = Cluster.build();
        builder.addContactPoint(frontendEndpoint.getIp());
        builder.port(frontendEndpoint.getGremlinServerPort());
        serializer.configure(config,null);
        builder.serializer(serializer);
        builder.credentials(account.getLeft(), account.getRight());
        Cluster cluster = builder.create();

        return new TinkerpopClusterAndClient(cluster, cluster.connect());
    }

    public static Map<String, String> executeQuery(Endpoint frontendEndpoint, Pair<String, String> account, Map<String, String> expectedQuery2Result) {

        TinkerpopClusterAndClient tinkerpopClusterAndClient = initTinkerpopConnect(frontendEndpoint, account);
        try {
            return executorQueryWithExistClient(tinkerpopClusterAndClient.client, expectedQuery2Result);
        } finally {
            tinkerpopClusterAndClient.client.close();
            tinkerpopClusterAndClient.cluster.close();
        }
    }

    public static Map<String, String> executorQueryWithExistClient(Client client, Map<String, String> expectedQuery2Result) {
        return executorQueryWithExistClient(client, expectedQuery2Result, DEFAULT_TIMEOUT_SECS);
    }

    public static Map<String, String> executorQueryWithExistClient(Client client, Map<String, String>
            expectedQuery2Result, long timeoutSecs) {
        Map<String, String> realQuery2Result = Maps.newHashMap();
        for (Map.Entry<String, String> entry : expectedQuery2Result.entrySet()) {
            CompletableFuture<ResultSet> resultSetCompletableFuture = client.submitAsync(entry.getKey());
            try {
                ResultSet resultSet = resultSetCompletableFuture.get(timeoutSecs, TimeUnit.SECONDS);
                List<String> queryResult = Lists.newArrayList();
                resultSet.stream().forEach(result -> queryResult.add(result.getString()));

                Collections.sort(queryResult);
                realQuery2Result.put(entry.getKey(), queryResult.toString());
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }

        return realQuery2Result;
    }

    public static class TinkerpopClusterAndClient {
        public final Cluster cluster;
        public final Client client;

        public TinkerpopClusterAndClient(Cluster cluster, Client client) {
            this.cluster = cluster;
            this.client = client;
        }
    }
}

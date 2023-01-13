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
package com.alibaba.graphscope.gaia.benchmark;

import com.alibaba.graphscope.gaia.common.CommonQuery;
import com.alibaba.graphscope.gaia.common.Configuration;
import com.alibaba.graphscope.gaia.utils.PropertyUtil;
import com.alibaba.graphscope.gaia.utils.QueryUtil;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV1d0;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class InteractiveBenchmark {
    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.out.println("Error, Usage: <interactive-benchmark.properties>");
            return;
        }

        Properties properties = PropertyUtil.getProperties(args[0], false);
        Configuration configuration = new Configuration(properties);

        String gremlinServerEndpoint =
                configuration.getString(Configuration.GREMLIN_SERVER_ENDPOINT);
        int threadCount = configuration.getInt(Configuration.THREAD_COUNT, 1);
        int warmUpCount = configuration.getInt(Configuration.WARMUP_EVERY_QUERY, 0);
        int operationCount = configuration.getInt(Configuration.OPERATION_COUNT_EVERY_QUERY, 10);
        boolean printQueryName = configuration.getBoolean(Configuration.PRINT_QUERY_NAME, true);
        boolean printQueryResult = configuration.getBoolean(Configuration.PRINT_QUERY_RESULT, true);
        String username = configuration.getString(Configuration.GREMLIN_USERNAME, "");
        String password = configuration.getString(Configuration.GREMLIN_PASSWORD, "");

        List<CommonQuery> ldbcQueryList = QueryUtil.initQueryList(configuration);

        AtomicInteger atomicQueryCount = new AtomicInteger(operationCount * threadCount);
        AtomicInteger atomicParameterIndex = new AtomicInteger(0);

        class MyRunnable implements Runnable {
            private Client client;

            public MyRunnable(String endpoint, String username, String password) {
                String[] address = endpoint.split(":");
                try {
                    Cluster.Builder cluster =
                            Cluster.build()
                                    .addContactPoint(address[0])
                                    .port(Integer.parseInt(address[1]))
                                    .serializer(initializeSerialize());
                    if (!(username == null || username.isEmpty()) && !(password == null || password.isEmpty())) {
                        cluster.credentials(username, password);
                    }
                    client = cluster.create().connect();

                    System.out.println("Connect success.");
                } catch (Exception e) {
                    System.err.println("Connect failure, caused by : " + e);
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void run() {
                for (int index = 0; index < warmUpCount; index++) {
                    System.out.println("Begin Warm up ....");
                    CommonQuery commonQuery = ldbcQueryList.get(index % ldbcQueryList.size());
                    HashMap<String, String> queryParameter = commonQuery.getSingleParameter(index);

                    commonQuery.processGremlinQuery(
                            client, queryParameter, printQueryResult, printQueryName);
                }
                System.out.println("Begin standard test...");
                while (true) {
                    int currentValue = atomicQueryCount.getAndDecrement();
                    if (currentValue > 0) {
                        int queryIndex = currentValue % ldbcQueryList.size();
                        CommonQuery commonQuery =
                                ldbcQueryList.get(queryIndex % ldbcQueryList.size());
                        int parameterIndex = 0;
                        if (queryIndex == 0) {
                            parameterIndex = atomicParameterIndex.getAndIncrement();
                        } else {
                            parameterIndex = atomicParameterIndex.get();
                        }
                        HashMap<String, String> queryParameter =
                                commonQuery.getSingleParameter(parameterIndex);
                        commonQuery.processGremlinQuery(
                                client, queryParameter, printQueryResult, printQueryName);
                    } else {
                        break;
                    }
                }
                client.close();
            }
        }

        ExecutorService threadPool = Executors.newFixedThreadPool(threadCount);

        long startTime = System.currentTimeMillis();
        for (int i = 0; i < threadCount; i++) {
            threadPool.submit(new MyRunnable(gremlinServerEndpoint, username, password));
        }

        threadPool.shutdown();

        while (true) {
            if (threadPool.isTerminated()) {
                long endTime = System.currentTimeMillis();
                long executeTime = endTime - startTime;
                long queryCount = operationCount * threadCount;
                float qps = (float) queryCount / executeTime * 1000;
                System.out.println(
                        "query count: "
                                + queryCount
                                + "; execute time(ms): "
                                + executeTime
                                + "; qps: "
                                + qps);
                System.exit(0);
            }
            Thread.sleep(10);
        }
    }

    private static MessageSerializer initializeSerialize() {
        return new GryoMessageSerializerV1d0();
    }
}

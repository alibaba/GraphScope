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

import com.alibaba.graphscope.gaia.clients.GraphClient;
import com.alibaba.graphscope.gaia.clients.GraphSystem;
import com.alibaba.graphscope.gaia.common.CommonQuery;
import com.alibaba.graphscope.gaia.common.Configuration;
import com.alibaba.graphscope.gaia.utils.BenchmarkResultComparator;
import com.alibaba.graphscope.gaia.utils.BenchmarkSystemUtil;
import com.alibaba.graphscope.gaia.utils.PropertyUtil;
import com.alibaba.graphscope.gaia.utils.QueryUtil;

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

        // compared systems
        List<GraphSystem> comparedSystems = BenchmarkSystemUtil.initSystems(configuration);

        // benchmark queries
        List<CommonQuery> ldbcQueryList = QueryUtil.initQueryList(configuration);
        String expectedResultsPath =
                configuration.getString(Configuration.EXPECTED_RESULTS_PATH, null);
        BenchmarkResultComparator comparator = new BenchmarkResultComparator(expectedResultsPath);

        // running settings
        int threadCount = configuration.getInt(Configuration.THREAD_COUNT, 1);
        int warmUpCount = configuration.getInt(Configuration.WARMUP_EVERY_QUERY, 0);
        int operationCount = configuration.getInt(Configuration.OPERATION_COUNT_EVERY_QUERY, 10);
        boolean printQueryName = configuration.getBoolean(Configuration.PRINT_QUERY_NAME, true);
        boolean printQueryResult = configuration.getBoolean(Configuration.PRINT_QUERY_RESULT, true);

        AtomicInteger atomicQueryCount = new AtomicInteger(operationCount * threadCount);
        AtomicInteger atomicParameterIndex = new AtomicInteger(0);

        class MyRunnable implements Runnable {
            private GraphClient client;
            BenchmarkResultComparator comparator;

            public MyRunnable(GraphClient client, BenchmarkResultComparator comparator) {
                this.comparator = comparator;
                System.out.println("Connect success.");
            }

            @Override
            public void run() {
                for (int index = 0; index < warmUpCount; index++) {
                    System.out.println("Begin Warm up ....");
                    CommonQuery commonQuery = ldbcQueryList.get(index % ldbcQueryList.size());
                    HashMap<String, String> queryParameter = commonQuery.getSingleParameter(index);

                    commonQuery.processGraphQuery(
                            client, queryParameter, printQueryResult, printQueryName, comparator);
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
                        commonQuery.processGraphQuery(
                                client,
                                queryParameter,
                                printQueryResult,
                                printQueryName,
                                comparator);
                    } else {
                        break;
                    }
                }
                client.close();
            }
        }

        for (GraphSystem system : comparedSystems) {
            String name = system.getName();
            GraphClient client = system.getClient();
            System.out.println("Start to benchmark system: " + name);

            ExecutorService threadPool = Executors.newFixedThreadPool(threadCount);

            long startTime = System.currentTimeMillis();

            for (int i = 0; i < threadCount; i++) {
                threadPool.submit(new MyRunnable(client, comparator));
            }

            threadPool.shutdown();

            while (true) {
                if (threadPool.isTerminated()) {
                    long endTime = System.currentTimeMillis();
                    long executeTime = endTime - startTime;
                    long queryCount = operationCount * threadCount;
                    float qps = (float) queryCount / executeTime * 1000;
                    System.out.println(
                            "System: "
                                    + name
                                    + "; query count: "
                                    + queryCount
                                    + "; execute time(ms): "
                                    + executeTime
                                    + "; qps: "
                                    + qps);
                    break;
                }
                Thread.sleep(10);
            }
        }
    }
}

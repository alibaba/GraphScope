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
package com.alibaba.graphscope.groot.coordinator;

import com.alibaba.graphscope.groot.CompletionCallback;
import com.alibaba.graphscope.groot.common.schema.wrapper.GraphDef;
import com.alibaba.graphscope.groot.rpc.RoleClients;
import com.alibaba.graphscope.proto.groot.FetchStatisticsResponse;
import com.alibaba.graphscope.proto.groot.Statistics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public class GraphDefFetcher {
    private static final Logger logger = LoggerFactory.getLogger(GraphDefFetcher.class);

    private final RoleClients<StoreSchemaClient> storeSchemaClients;
    int storeCount;

    public GraphDefFetcher(RoleClients<StoreSchemaClient> storeSchemaClients, int storeCount) {
        this.storeSchemaClients = storeSchemaClients;
        this.storeCount = storeCount;
    }

    public GraphDef fetchGraphDef() {
        return storeSchemaClients.getClient(0).fetchSchema();
    }

    public Map<Integer, Statistics> fetchStatistics() {
        Map<Integer, Statistics> statisticsMap = new ConcurrentHashMap<>();
        CountDownLatch countDownLatch = new CountDownLatch(storeCount);

        for (int i = 0; i < storeCount; ++i) {
            storeSchemaClients
                    .getClient(i)
                    .fetchStatistics(
                            new CompletionCallback<FetchStatisticsResponse>() {
                                @Override
                                public void onCompleted(FetchStatisticsResponse res) {
                                    statisticsMap.putAll(res.getStatisticsMapMap());
                                    finish(null);
                                }

                                @Override
                                public void onError(Throwable t) {
                                    logger.error("failed to fetch statistics", t);
                                    finish(t);
                                }

                                private void finish(Throwable t) {
                                    countDownLatch.countDown();
                                }
                            });
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            logger.error("fetch statistics has been interrupted", e);
        }
        if (statisticsMap.size() != storeCount) {
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                // Ignore
            }
        }
        return statisticsMap;
    }
}

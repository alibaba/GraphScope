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
package com.alibaba.graphscope.groot.metrics;

import com.alibaba.graphscope.groot.CompletionCallback;
import com.alibaba.maxgraph.common.config.CommonConfig;
import com.alibaba.maxgraph.common.config.Configs;
import com.alibaba.maxgraph.common.RoleType;
import com.alibaba.graphscope.groot.rpc.RoleClients;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class MetricsAggregator {

    private Map<RoleType, RoleClients<MetricsCollectClient>> roleToClients = new HashMap<>();

    private ObjectMapper objectMapper;
    private int frontendCount;
    private int ingestorCount;

    public MetricsAggregator(
            Configs configs,
            RoleClients<MetricsCollectClient> frontendMetricsCollectClients,
            RoleClients<MetricsCollectClient> ingestorMetricsCollectClients) {
        this.roleToClients.put(RoleType.FRONTEND, frontendMetricsCollectClients);
        this.roleToClients.put(RoleType.INGESTOR, ingestorMetricsCollectClients);

        this.objectMapper = new ObjectMapper();
        this.frontendCount = CommonConfig.FRONTEND_NODE_COUNT.get(configs);
        this.ingestorCount = CommonConfig.INGESTOR_NODE_COUNT.get(configs);
    }

    public void aggregateMetricsJson(String roleNames, CompletionCallback<String> callback) {
        String[] roleNameArray = roleNames.split(",");
        int totalNode = 0;
        Map<RoleType, Integer> roleTypeToCount = new HashMap<>();
        for (String roleName : roleNameArray) {
            RoleType roleType = RoleType.fromName(roleName);
            switch (roleType) {
                case FRONTEND:
                    totalNode += this.frontendCount;
                    roleTypeToCount.put(roleType, this.frontendCount);
                    break;
                case INGESTOR:
                    totalNode += this.ingestorCount;
                    roleTypeToCount.put(roleType, this.ingestorCount);
                    break;
                default:
                    throw new IllegalArgumentException(
                            "RoleType [" + roleType + "] not supported yet");
            }
        }

        Map<String, Object> aggregated = new ConcurrentHashMap<>();
        AtomicInteger counter = new AtomicInteger(totalNode);

        roleTypeToCount.forEach(
                (role, nodeCount) -> {
                    RoleClients<MetricsCollectClient> metricsCollectClients =
                            this.roleToClients.get(role);
                    for (int i = 0; i < nodeCount; i++) {
                        int nodeId = i;
                        metricsCollectClients
                                .getClient(nodeId)
                                .collectMetrics(
                                        new CompletionCallback<Map<String, String>>() {
                                            String node = role + "#" + nodeId;

                                            @Override
                                            public void onCompleted(Map<String, String> res) {
                                                aggregated.put(node, res);
                                                if (counter.decrementAndGet() == 0) {
                                                    finish();
                                                }
                                            }

                                            @Override
                                            public void onError(Throwable t) {
                                                aggregated.put(node, t);
                                                if (counter.decrementAndGet() == 0) {
                                                    finish();
                                                }
                                            }

                                            private void finish() {
                                                try {
                                                    String jsonResult =
                                                            objectMapper.writeValueAsString(
                                                                    aggregated);
                                                    callback.onCompleted(jsonResult);
                                                } catch (JsonProcessingException e) {
                                                    callback.onError(e);
                                                }
                                            }
                                        });
                    }
                });
    }
}

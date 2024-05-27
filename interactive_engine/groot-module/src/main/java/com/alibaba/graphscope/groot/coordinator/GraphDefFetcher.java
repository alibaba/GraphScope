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

import com.alibaba.graphscope.groot.common.schema.wrapper.GraphDef;
import com.alibaba.graphscope.groot.rpc.RoleClients;
import com.alibaba.graphscope.proto.groot.Statistics;

import java.util.HashMap;
import java.util.Map;

public class GraphDefFetcher {

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
        Map<Integer, Statistics> statisticsMap = new HashMap<>();
        for (int i = 0; i < storeCount; ++i) {
            Map<Integer, Statistics> curMap = storeSchemaClients.getClient(i).fetchStatistics();
            statisticsMap.putAll(curMap);
        }
        return statisticsMap;
    }
}

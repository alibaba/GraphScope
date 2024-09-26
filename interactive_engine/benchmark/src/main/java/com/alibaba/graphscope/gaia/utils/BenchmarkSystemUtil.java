/**
 * Copyright 2024 Alibaba Group Holding Limited.
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
package com.alibaba.graphscope.gaia.utils;

import com.alibaba.graphscope.gaia.clients.GraphClient;
import com.alibaba.graphscope.gaia.clients.GraphSystem;
import com.alibaba.graphscope.gaia.clients.cypher.CypherGraphClient;
import com.alibaba.graphscope.gaia.clients.gremlin.GremlinGraphClient;
import com.alibaba.graphscope.gaia.clients.kuzu.KuzuGraphClient;
import com.alibaba.graphscope.gaia.common.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class BenchmarkSystemUtil {
    public static List<GraphSystem> initSystems(Configuration configuration) throws Exception {
        String username = configuration.getString(Configuration.AUTH_USERNAME, "");
        String password = configuration.getString(Configuration.AUTH_PASSWORD, "");
        List<GraphSystem> systemClients = new ArrayList<>();
        int systemCount = 1;
        while (true) {
            Optional<String> systemName =
                    configuration.getOption("system." + systemCount + ".name");
            if (!systemName.isPresent()) {
                break;
            }
            Optional<String> endpoint =
                    configuration.getOption("system." + systemCount + ".endpoint");
            Optional<String> path = configuration.getOption("system." + systemCount + ".path");
            String clientOpt = configuration.getString("system." + systemCount + ".client");
            GraphClient client;
            switch (clientOpt.toLowerCase()) {
                case "gremlin":
                    if (endpoint.isPresent()) {
                        client = new GremlinGraphClient(endpoint.get(), username, password);
                    } else {
                        throw new IllegalArgumentException(
                                "Gremlin client must have endpoint of the database service");
                    }
                    break;
                case "cypher":
                    if (endpoint.isPresent()) {
                        client = new CypherGraphClient(endpoint.get(), username, password);
                    } else {
                        throw new IllegalArgumentException(
                                "Cypher client must have endpoint of the database service");
                    }
                    break;
                case "kuzu":
                    if (path.isPresent()) {
                        client = new KuzuGraphClient(path.get());
                        long parallelism =
                                configuration.getInt("system." + systemCount + ".parallelism", 1);
                        ((KuzuGraphClient) client).setMaxNumThreadForExec(parallelism);
                    } else {
                        throw new IllegalArgumentException(
                                "Kuzu client must have path to access the database");
                    }
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported client type: " + clientOpt);
            }
            systemClients.add(new GraphSystem(systemName.get(), client));
            systemCount++;
        }
        return systemClients;
    }
}

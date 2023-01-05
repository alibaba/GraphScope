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
package com.alibaba.graphscope.function.test;

import com.alibaba.graphscope.function.test.config.Configuration;

import org.apache.commons.lang3.tuple.Pair;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class TestGlobalMeta {
    private static Configuration testConf;
    // modern graph -> <object_id, schema_path>
    private static Map<String, Pair> graphMetaMap = new HashMap<>();
    // modern graph -> endpoint
    private static Map<String, String> graphMap = new HashMap<>();

    public static Configuration getTestConf() {
        return testConf;
    }

    public static void setTestConf(final Configuration testConf) {
        TestGlobalMeta.testConf = testConf;
    }

    public static void addGraphEndpoint(String graphName, String endpoint) {
        TestGlobalMeta.graphMap.put(graphName, endpoint);
    }

    public static String getGraphEndpoint(String graphName) {
        return graphMap.get(graphName);
    }

    public static void addGraphMeta(String graphName, Pair<String, String> meta) {
        graphMetaMap.put(graphName, meta);
    }

    public static Pair getGraphMeta(String graphName) {
        return graphMetaMap.get(graphName);
    }

    public static Set<String> getAllGraphName() {
        return TestGlobalMeta.graphMap.keySet();
    }
}

/*
 *
 *  * Copyright 2020 Alibaba Group Holding Limited.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.alibaba.graphscope.sdk;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;

public class JNICompilePlanTest {
    @Test
    public void planQuery() throws Exception {
        String configPath = "src/test/resources/config/interactive_config_test.yaml";
        String query =
                "MATCH (src)-[e:test6*4..5]->(dest) WHERE src.__domain__ = 'xzz' RETURN"
                        + " src.__entity_id__ AS sId, dest.__entity_id__ AS dId;";
        String schemaYaml =
                FileUtils.readFileToString(
                        new File("/Users/zhouxiaoli/Downloads/graph_schema.yaml"),
                        StandardCharsets.UTF_8);
        String statsJson =
                FileUtils.readFileToString(
                        new File("/Users/zhouxiaoli/Downloads/statistics.json"),
                        StandardCharsets.UTF_8);
        for (int i = 0; i < 100; ++i) {
            long startTime = System.currentTimeMillis();
            PlanUtils.compilePlan(configPath, query, schemaYaml, statsJson);
            long endTime = System.currentTimeMillis() - startTime;
            System.out.println("Time cost: " + endTime);
        }
    }
}

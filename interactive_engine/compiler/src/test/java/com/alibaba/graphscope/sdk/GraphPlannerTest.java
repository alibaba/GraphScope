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

import com.alibaba.graphscope.gaia.proto.GraphAlgebraPhysical;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class GraphPlannerTest {
    @Test
    public void testCompilePlan() throws Exception {
        String jsonPayLoad = createParameters().toString();
        HttpClient client = HttpClient.newBuilder().build();
        System.out.println(jsonPayLoad);
        HttpRequest request =
                HttpRequest.newBuilder()
                        .uri(URI.create("http://localhost:8080/api/compilePlan"))
                        .setHeader("Content-Type", "application/json")
                        .POST(
                                HttpRequest.BodyPublishers.ofString(
                                        jsonPayLoad, StandardCharsets.UTF_8))
                        .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        String body = response.body();
        System.out.println(body);
        JsonNode planNode = (new ObjectMapper()).readTree(body).get("graphPlan");
        System.out.println(getPhysicalPlan(planNode));
        System.out.println(getResultSchemaYaml(planNode));
    }

    private JsonNode createParameters() throws Exception {
        Map<String, String> params =
                ImmutableMap.of(
                        "configPath",
                        "conf/ir.compiler.properties",
                        "query",
                        "Match (n) Return n;",
                        "schemaYaml",
                        FileUtils.readFileToString(
                                new File("src/test/resources/config/modern/graph.yaml"),
                                StandardCharsets.UTF_8),
                        "statsJson",
                        FileUtils.readFileToString(
                                new File("src/test/resources/statistics/modern_statistics.json"),
                                StandardCharsets.UTF_8));
        return (new ObjectMapper()).valueToTree(params);
    }

    private GraphAlgebraPhysical.PhysicalPlan getPhysicalPlan(JsonNode planNode) throws Exception {
        String base64Str = planNode.get("physicalBytes").asText();
        byte[] bytes = java.util.Base64.getDecoder().decode(base64Str);
        return GraphAlgebraPhysical.PhysicalPlan.parseFrom(bytes);
    }

    private String getResultSchemaYaml(JsonNode planNode) {
        return planNode.get("resultSchemaYaml").asText();
    }
}

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

package com.alibaba.graphscope.sdk.examples;

import com.alibaba.graphscope.gaia.proto.GraphAlgebraPhysical;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class TestGraphPlanner {
    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.out.println("Usage: <configPath> <query> <schemaPath> <statsPath>");
            System.exit(1);
        }
        // set request body in json format
        String jsonPayLoad = createParameters(args[0], args[1], args[2], args[3]).toString();
        HttpClient client = HttpClient.newBuilder().build();
        // create http request, set header and body content
        HttpRequest request =
                HttpRequest.newBuilder()
                        .uri(URI.create("http://localhost:8080/api/compilePlan"))
                        .setHeader("Content-Type", "application/json")
                        .POST(
                                HttpRequest.BodyPublishers.ofString(
                                        jsonPayLoad, StandardCharsets.UTF_8))
                        .build();
        // send request and get response
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        String body = response.body();
        // parse response body as json
        JsonNode planNode = (new ObjectMapper()).readTree(body).get("graphPlan");
        // print result
        System.out.println(getPhysicalPlan(planNode));
        System.out.println(getResultSchemaYaml(planNode));
    }

    private static JsonNode createParameters(
            String configPath, String query, String schemaPath, String statsPath) throws Exception {
        Map<String, String> params =
                ImmutableMap.of(
                        "configPath",
                        configPath,
                        "query",
                        query,
                        "schemaYaml",
                        FileUtils.readFileToString(new File(schemaPath), StandardCharsets.UTF_8),
                        "statsJson",
                        FileUtils.readFileToString(new File(statsPath), StandardCharsets.UTF_8));
        return (new ObjectMapper()).valueToTree(params);
    }

    // get base64 string from json, convert it to physical bytes , then parse it to PhysicalPlan
    private static GraphAlgebraPhysical.PhysicalPlan getPhysicalPlan(JsonNode planNode)
            throws Exception {
        String base64Str = planNode.get("physicalBytes").asText();
        byte[] bytes = java.util.Base64.getDecoder().decode(base64Str);
        return GraphAlgebraPhysical.PhysicalPlan.parseFrom(bytes);
    }

    // get result schema yaml from json
    private static String getResultSchemaYaml(JsonNode planNode) {
        return planNode.get("resultSchemaYaml").asText();
    }
}

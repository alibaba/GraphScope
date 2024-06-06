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

package com.alibaba.graphscope.common.ir.meta.reader;

import com.alibaba.graphscope.common.client.channel.ChannelFetcher;
import com.alibaba.graphscope.common.ir.meta.GraphId;
import com.alibaba.graphscope.common.ir.meta.IrMeta;
import com.alibaba.graphscope.common.ir.meta.SnapshotId;
import com.alibaba.graphscope.common.ir.meta.procedure.GraphStoredProcedures;
import com.alibaba.graphscope.common.ir.meta.schema.FileFormatType;
import com.alibaba.graphscope.common.ir.meta.schema.IrGraphSchema;
import com.alibaba.graphscope.common.ir.meta.schema.IrGraphStatistics;
import com.alibaba.graphscope.common.ir.meta.schema.SchemaInputStream;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;

import org.javatuples.Pair;
import org.yaml.snakeyaml.Yaml;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

// read ir meta from a remote http service
public class HttpIrMetaReader implements IrMetaReader {
    private static final String CONTENT_TYPE = "Content-Type";
    private static final String APPLICATION_JSON = "application/json; utf-8";
    private final ChannelFetcher<URI> fetcher;
    private final HttpClient httpClient;

    public HttpIrMetaReader(ChannelFetcher<URI> fetcher) {
        this.fetcher = fetcher;
        this.httpClient = HttpClient.newBuilder().build();
    }

    @Override
    public IrMeta readMeta() throws IOException {
        try {
            HttpResponse<String> response = sendRequest("/v1/service/status");
            String res = response.body();
            Preconditions.checkArgument(
                    response.statusCode() == 200,
                    "read meta data fail, status code: %s, error message: %s",
                    response.statusCode(),
                    res);
            Pair<GraphId, String> metaPair = convertMetaFromJsonToYaml(res);
            String metaInYaml = metaPair.getValue1();
            return new IrMeta(
                    metaPair.getValue0(),
                    SnapshotId.createEmpty(), // todo: return snapshot id from http service
                    new IrGraphSchema(
                            new SchemaInputStream(
                                    new ByteArrayInputStream(
                                            metaInYaml.getBytes(StandardCharsets.UTF_8)),
                                    FileFormatType.YAML)),
                    new GraphStoredProcedures(
                            new ByteArrayInputStream(metaInYaml.getBytes(StandardCharsets.UTF_8)),
                            this));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public IrGraphStatistics readStats(GraphId graphId) throws IOException {
        try {
            Preconditions.checkArgument(
                    graphId.getId() != null, "graph id should not be null in http meta reader");
            HttpResponse<String> response =
                    sendRequest(String.format("/v1/graph/%s/statistics", graphId.getId()));
            String res = response.body();
            Preconditions.checkArgument(
                    response.statusCode() == 200,
                    "read graph statistics fail, status code: %s, error message: %s",
                    response.statusCode(),
                    res);
            return new IrGraphStatistics(
                    new ByteArrayInputStream(res.getBytes(StandardCharsets.UTF_8)));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private HttpResponse<String> sendRequest(String requestUri)
            throws IOException, InterruptedException {
        List<URI> metaServiceHost = fetcher.fetch();
        Preconditions.checkArgument(!metaServiceHost.isEmpty(), "can not get meta service host");
        URI getMetaUri = metaServiceHost.get(0).resolve(requestUri);
        HttpRequest request =
                HttpRequest.newBuilder()
                        .uri(getMetaUri)
                        .headers(CONTENT_TYPE, APPLICATION_JSON)
                        .GET()
                        .build();
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private Pair<GraphId, String> convertMetaFromJsonToYaml(String metaInJson) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode rootNode = mapper.readTree(metaInJson);
        Map<String, Object> rootMap = mapper.convertValue(rootNode, Map.class);
        Map metaMap = (Map) rootMap.get("graph");
        GraphId graphId = new GraphId(metaMap.get("id"));
        Yaml yaml = new Yaml();
        return Pair.with(graphId, yaml.dump(metaMap));
    }
}

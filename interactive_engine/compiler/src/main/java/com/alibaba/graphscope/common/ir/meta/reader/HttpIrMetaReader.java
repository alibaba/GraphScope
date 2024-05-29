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
import com.alibaba.graphscope.common.ir.meta.IrMeta;
import com.alibaba.graphscope.common.ir.meta.procedure.GraphStoredProcedures;
import com.alibaba.graphscope.common.ir.meta.schema.IrGraphSchema;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;

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
            List<URI> metaServiceHost = fetcher.fetch();
            Preconditions.checkArgument(
                    !metaServiceHost.isEmpty(), "can not get meta service host");
            URI getMetaUri = metaServiceHost.get(0).resolve("/v1/service/status");
            HttpRequest request =
                    HttpRequest.newBuilder()
                            .uri(getMetaUri)
                            .headers(CONTENT_TYPE, APPLICATION_JSON)
                            .GET()
                            .build();
            HttpResponse<String> response =
                    httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            String res = response.body();
            Preconditions.checkArgument(
                    response.statusCode() == 200,
                    "get meta data fail, status code: %s, error message: %s",
                    response.statusCode(),
                    res);
            String metaInYaml = convertMetaFromJsonToYaml(res);
            return new IrMeta(
                    new IrGraphSchema(
                            new SchemaInputStream(
                                    new ByteArrayInputStream(
                                            metaInYaml.getBytes(StandardCharsets.UTF_8)),
                                    FileFormatType.YAML)),
                    new GraphStoredProcedures(
                            new ByteArrayInputStream(metaInYaml.getBytes(StandardCharsets.UTF_8))));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private String convertMetaFromJsonToYaml(String metaInJson) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode rootNode = mapper.readTree(metaInJson);
        Map<String, Object> rootMap = mapper.convertValue(rootNode, Map.class);
        Map metaMap = (Map) rootMap.get("graph");
        Yaml yaml = new Yaml();
        return yaml.dump(metaMap);
    }
}

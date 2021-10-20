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
package com.alibaba.maxgraph.sdkcommon.util;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.commons.collections4.MapUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.*;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Map;

import static java.net.URLDecoder.decode;

public class HTTPUtils {
    private static Logger LOG = LoggerFactory.getLogger(HTTPUtils.class);
    private static final int TIMEOUT_TIME_SECOND = 10;
    private static RequestConfig requestConfig =
            RequestConfig.custom()
                    .setSocketTimeout(TIMEOUT_TIME_SECOND * 1000)
                    .setConnectionRequestTimeout(TIMEOUT_TIME_SECOND * 1000)
                    .setConnectTimeout(TIMEOUT_TIME_SECOND * 1000)
                    .build();
    private static final String HEADER_ERR_MSG = "X-maxgraph-error";

    public static void checkResponse(CloseableHttpResponse response) {
        int responseStatusCode = response.getStatusLine().getStatusCode();
        if (responseStatusCode != 200 && responseStatusCode != 201) {
            Header[] headers = response.getAllHeaders();
            String errMsg = "";
            for (Header header : headers) {
                if (header.getName().equals(HEADER_ERR_MSG)) {
                    errMsg = header.getValue();
                }
            }
            LOG.error("Failed: HTTP error code " + responseStatusCode + "; caused by: " + errMsg);
            throw new IllegalArgumentException(
                    String.format(
                            "Failed: HTTP error code [%s], error message [%s]",
                            responseStatusCode, errMsg));
        }
    }

    private static boolean isMapNotEmpty(Map<String, String> headerMap) {
        return MapUtils.isNotEmpty(headerMap);
    }

    private static CloseableHttpResponse doPost(
            String url, String json, Map<String, String> headerMap) throws IOException {
        LOG.debug("url:{}, and request body:{}", url, json);
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpPost httpPost = new HttpPost(url);

        httpPost.setConfig(requestConfig);

        if (isMapNotEmpty(headerMap)) {
            headerMap.forEach(httpPost::setHeader);
        }
        if (!Strings.isNullOrEmpty(json)) {
            StringEntity stringEntity = new StringEntity(json, "UTF-8");
            stringEntity.setContentType("application/json");
            httpPost.setEntity(stringEntity);
        }

        CloseableHttpResponse response = httpClient.execute(httpPost);
        checkResponse(response);
        return response;
    }

    public static CloseableHttpResponse sendPostRequest(String url, String json)
            throws IOException {
        return doPost(url, json, null);
    }

    public static CloseableHttpResponse sendPostRequestWithHeader(
            String url, String json, Map<String, String> headerMap) throws IOException {
        return doPost(url, json, headerMap);
    }

    private static CloseableHttpResponse doPut(
            String url, Map<String, String> headerMap, String json, int socketTimeOut)
            throws IOException {
        LOG.debug("url:{}, and request body:{}", url, json);
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpPut httpPut = new HttpPut(url);
        httpPut.setConfig(requestConfig);
        if (isMapNotEmpty(headerMap)) {
            headerMap.forEach(httpPut::setHeader);
        }
        RequestConfig requestConfig =
                RequestConfig.custom().setSocketTimeout(socketTimeOut).build();
        httpPut.setConfig(requestConfig);

        if (!Strings.isNullOrEmpty(json)) {
            StringEntity stringEntity = new StringEntity(json, "UTF-8");
            stringEntity.setContentType("application/json");
            httpPut.setEntity(stringEntity);
        }

        CloseableHttpResponse response = httpClient.execute(httpPut);
        checkResponse(response);

        return response;
    }

    public static CloseableHttpResponse sendPutRequest(String url, String json, int socketTimeOut)
            throws IOException {
        return doPut(url, null, json, socketTimeOut);
    }

    public static CloseableHttpResponse sendPutRequestWithHeader(
            String url, String json, Map<String, String> headerMap, int socketTimeOut)
            throws IOException {
        return doPut(url, headerMap, json, socketTimeOut);
    }

    private static CloseableHttpResponse doDelete(String url, Map<String, String> headerMap)
            throws IOException {
        LOG.debug("url:{}", url);
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpDelete httpDelete = new HttpDelete(url);
        httpDelete.setConfig(requestConfig);
        if (isMapNotEmpty(headerMap)) {
            headerMap.forEach(httpDelete::setHeader);
        }
        CloseableHttpResponse response = httpClient.execute(httpDelete);

        checkResponse(response);
        return response;
    }

    public static CloseableHttpResponse sendDeleteRequest(String url) throws IOException {
        return doDelete(url, null);
    }

    public static CloseableHttpResponse sendDeleteRequestWithHeader(
            String url, Map<String, String> headerMap) throws IOException {
        return doDelete(url, headerMap);
    }

    private static CloseableHttpResponse doGet(
            String url, Map<String, String> headerMap, Map<String, String> params)
            throws IOException, URISyntaxException {

        LOG.debug("url:{}", url);
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet(url);
        httpGet.setConfig(requestConfig);
        if (isMapNotEmpty(headerMap)) {
            headerMap.forEach(httpGet::setHeader);
        }
        if (isMapNotEmpty(params)) {
            URIBuilder uriBuilder = new URIBuilder(httpGet.getURI());
            params.forEach(uriBuilder::addParameter);
            httpGet.setURI(uriBuilder.build());
        }
        CloseableHttpResponse response = httpClient.execute(httpGet);

        checkResponse(response);

        return response;
    }

    public static CloseableHttpResponse sendGetRequest(String url, Map<String, String> params)
            throws IOException, URISyntaxException {
        return doGet(url, null, params);
    }

    public static CloseableHttpResponse sendGetRequest(String url)
            throws IOException, URISyntaxException {
        return doGet(url, null, null);
    }

    public static CloseableHttpResponse sendGetRequestWithHeader(
            String url, Map<String, String> headerMap) throws IOException, URISyntaxException {
        return doGet(url, headerMap, null);
    }

    public static String getResponseContent(CloseableHttpResponse response) throws Exception {
        return EntityUtils.toString(response.getEntity());
    }

    public static void main(String[] args) {
        String url =
                "http://graph.alibaba.net:7003/api/instances/91FEC377CE3D11E9AD670242C0A80501/nodes/mapping?flag=true";
        String request =
                "{\"instanceId\":\"91FEC377CE3D11E9AD670242C0A80501\",\"flag\":true,\"schemaVersion\":26,\"typeId\":2,\"type\":1,\"label\":\"Place\",\"comment\":\"Place\",\"color\":\"#2f54eb\",\"size\":\"default\",\"property\":\"id#\",\"createdBy\":\"皋x\",\"createdDate\":1567513430000,\"storeType\":1,\"startLabel\":\"\",\"startTypeId\":null,\"endLabel\":\"\",\"endTypeId\":null,\"sourceFrom\":1,\"sourcePath\":\"\",\"sourceFormat\":null,\"targetFormat\":null,\"dataSeparator\":\"|\",\"invalidDataCount\":0,\"onlineMode\":0,\"dimensionType\":0,\"mrOtherConfigs\":\"\",\"odpsTable\":\"biggraph_dev.ldbc_1w_place\",\"odpsPartition\":null,\"onlineId\":null,\"userOdpsProjectName\":null,\"userOdpsEndpoint\":null,\"userOdpsAccessId\":null,\"userOdpsAccessKey\":null,\"userOdpsBizOwnerId\":null,\"odpsSplitSize\":null,\"runMrWithSql\":false,\"properties\":[{\"id\":5,\"propertyName\":\"id\",\"dataType\":\"LONG\",\"comment\":\"id\",\"index\":0,\"columnName\":\"id\",\"defaultValue\":null,\"unique\":false,\"show\":true,\"pk\":true,\"key\":1},{\"id\":11,\"propertyName\":\"name\",\"dataType\":\"STRING\",\"comment\":\"name\",\"index\":1,\"columnName\":\"name\",\"defaultValue\":null,\"unique\":false,\"show\":false,\"pk\":false,\"key\":2},{\"id\":12,\"propertyName\":\"type\",\"dataType\":\"STRING\",\"comment\":\"type\",\"index\":3,\"columnName\":\"type\",\"defaultValue\":null,\"unique\":false,\"show\":false,\"pk\":false,\"key\":3},{\"id\":13,\"propertyName\":\"url\",\"dataType\":\"STRING\",\"comment\":\"url\",\"index\":2,\"columnName\":\"url\",\"defaultValue\":null,\"unique\":false,\"show\":false,\"pk\":false,\"key\":4}],\"relations\":null,\"dstProperties\":null,\"method\":\"post\"}";

        try {
            CloseableHttpResponse httpResult = HTTPUtils.sendPostRequest(url, request);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

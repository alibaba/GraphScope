package com.alibaba.graphscope.interactive.client.impl;

import com.alibaba.graphscope.interactive.*;
import com.alibaba.graphscope.interactive.api.QueryServiceApi;
import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.internal.http.HttpMethod;
import okio.Buffer;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// add extra customization for query service api interface.
public class QueryServiceApiUtils {
    public static Request.Builder createProcCallRequestBuilder(QueryServiceApi api, String graphId, byte[] body, final ApiCallback _callback) throws ApiException {
        String basePath = null;

        // Determine Base Path to Use
        if (api.getCustomBaseUrl() != null){
            basePath = api.getCustomBaseUrl();
        } else {
            basePath = null;
        }

        ApiClient localVarApiClient = api.getApiClient();

        Object localVarPostBody = body;

        // create path and map variables
        String localVarPath;
        if (graphId != null) {
            localVarPath = "/v1/graph/{graph_id}/query"
                    .replace("{" + "graph_id" + "}", localVarApiClient.escapeString(graphId.toString()));
        }
        else {
            localVarPath = "/v1/graph/current/query";
        }

        List<Pair> localVarQueryParams = new ArrayList<Pair>();
        List<Pair> localVarCollectionQueryParams = new ArrayList<Pair>();
        Map<String, String> localVarHeaderParams = new HashMap<String, String>();
        Map<String, String> localVarCookieParams = new HashMap<String, String>();
        Map<String, Object> localVarFormParams = new HashMap<String, Object>();

        final String[] localVarAccepts = {
                "text/plain"
        };
        final String localVarAccept = localVarApiClient.selectHeaderAccept(localVarAccepts);
        if (localVarAccept != null) {
            localVarHeaderParams.put("Accept", localVarAccept);
        }

        final String[] localVarContentTypes = {
                "text/plain"
        };
        final String localVarContentType = localVarApiClient.selectHeaderContentType(localVarContentTypes);
        if (localVarContentType != null) {
            localVarHeaderParams.put("Content-Type", localVarContentType);
        }

        String[] localVarAuthNames = new String[] {  };
        return createRequestBuilder(localVarApiClient, basePath, localVarPath, "POST", localVarQueryParams, localVarCollectionQueryParams, localVarPostBody, localVarHeaderParams, localVarCookieParams, localVarFormParams, localVarAuthNames, _callback);
    }

    private static Request.Builder createRequestBuilder(ApiClient client, String baseUrl, String path, String method, List<Pair> queryParams, List<Pair> collectionQueryParams, Object body, Map<String, String> headerParams, Map<String, String> cookieParams, Map<String, Object> formParams, String[] authNames, ApiCallback callback)throws ApiException {
        // aggregate queryParams (non-collection) and collectionQueryParams into allQueryParams
        List<Pair> allQueryParams = new ArrayList<Pair>(queryParams);
        allQueryParams.addAll(collectionQueryParams);

        final String url = client.buildUrl(baseUrl, path, queryParams, collectionQueryParams);

        // prepare HTTP request body
        RequestBody reqBody;
        String contentType = headerParams.get("Content-Type");
        String contentTypePure = contentType;
        if (contentTypePure != null && contentTypePure.contains(";")) {
            contentTypePure = contentType.substring(0, contentType.indexOf(";"));
        }
        if (!HttpMethod.permitsRequestBody(method)) {
            reqBody = null;
        } else if ("application/x-www-form-urlencoded".equals(contentTypePure)) {
            reqBody = client.buildRequestBodyFormEncoding(formParams);
        } else if ("multipart/form-data".equals(contentTypePure)) {
            reqBody = client.buildRequestBodyMultipart(formParams);
        } else if (body == null) {
            if ("DELETE".equals(method)) {
                // allow calling DELETE without sending a request body
                reqBody = null;
            } else {
                // use an empty request body (for POST, PUT and PATCH)
                reqBody = RequestBody.create("", contentType == null ? null : MediaType.parse(contentType));
            }
        } else {
            reqBody = client.serialize(body, contentType);
        }

        // update parameters with authentication settings
        client.updateParamsForAuth(authNames, allQueryParams, headerParams, cookieParams, requestBodyToString(reqBody), method, URI.create(url));

        final Request.Builder reqBuilder = new Request.Builder().url(url);
        client.processHeaderParams(headerParams, reqBuilder);
        client.processCookieParams(cookieParams, reqBuilder);

        // Associate callback with request (if not null) so interceptor can
        // access it when creating ProgressResponseBody
        reqBuilder.tag(callback);

        Request request = null;

        if (callback != null && reqBody != null) {
            ProgressRequestBody progressRequestBody = new ProgressRequestBody(reqBody, callback);
             return reqBuilder.method(method, progressRequestBody);
        } else {
             return reqBuilder.method(method, reqBody);
        }
    }

    private static String requestBodyToString(RequestBody requestBody) throws ApiException {
        if (requestBody != null) {
            try {
                final Buffer buffer = new Buffer();
                requestBody.writeTo(buffer);
                return buffer.readUtf8();
            } catch (final IOException e) {
                throw new ApiException(e);
            }
        }

        // empty http request body
        return "";
    }
}

/*
 * GraphScope Interactive API v0.0.3
 * This is the definition of GraphScope Interactive API, including   - AdminService API   - Vertex/Edge API   - QueryService   AdminService API (with tag AdminService) defines the API for GraphManagement, ProcedureManagement and Service Management.  Vertex/Edge API (with tag GraphService) defines the API for Vertex/Edge management, including creation/updating/delete/retrive.  QueryService API (with tag QueryService) defines the API for procedure_call, Ahodc query.
 *
 * The version of the OpenAPI document: 1.0.0
 * Contact: graphscope@alibaba-inc.com
 *
 * NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).
 * https://openapi-generator.tech
 * Do not edit the class manually.
 */

package com.alibaba.graphscope.interactive;

import com.alibaba.graphscope.ApiCallback;
import com.alibaba.graphscope.ApiClient;
import com.alibaba.graphscope.ApiException;
import com.alibaba.graphscope.ApiResponse;
import com.alibaba.graphscope.Configuration;
import com.alibaba.graphscope.Pair;
import com.google.gson.reflect.TypeToken;

import org.openapitools.client.model.EdgeData;
import org.openapitools.client.model.EdgeRequest;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GraphServiceEdgeManagementApi {
    private ApiClient localVarApiClient;
    private int localHostIndex;
    private String localCustomBaseUrl;

    public GraphServiceEdgeManagementApi() {
        this(Configuration.getDefaultApiClient());
    }

    public GraphServiceEdgeManagementApi(ApiClient apiClient) {
        this.localVarApiClient = apiClient;
    }

    public ApiClient getApiClient() {
        return localVarApiClient;
    }

    public void setApiClient(ApiClient apiClient) {
        this.localVarApiClient = apiClient;
    }

    public int getHostIndex() {
        return localHostIndex;
    }

    public void setHostIndex(int hostIndex) {
        this.localHostIndex = hostIndex;
    }

    public String getCustomBaseUrl() {
        return localCustomBaseUrl;
    }

    public void setCustomBaseUrl(String customBaseUrl) {
        this.localCustomBaseUrl = customBaseUrl;
    }

    /**
     * Build call for addEdge
     * @param graphId  (required)
     * @param edgeRequest  (optional)
     * @param _callback Callback for upload/download progress
     * @return Call to execute
     * @throws ApiException If fail to serialize the request body object
     * @http.response.details
     * <table summary="Response Details" border="1">
     * <tr><td> Status Code </td><td> Description </td><td> Response Headers </td></tr>
     * <tr><td> 200 </td><td> Successfully insert the edge </td><td>  -  </td></tr>
     * <tr><td> 400 </td><td> Invalid input edge </td><td>  -  </td></tr>
     * <tr><td> 409 </td><td> edge already exists </td><td>  -  </td></tr>
     * <tr><td> 500 </td><td> Server internal error </td><td>  -  </td></tr>
     * </table>
     */
    public okhttp3.Call addEdgeCall(
            String graphId, EdgeRequest edgeRequest, final ApiCallback _callback)
            throws ApiException {
        String basePath = null;
        // Operation Servers
        String[] localBasePaths = new String[] {};

        // Determine Base Path to Use
        if (localCustomBaseUrl != null) {
            basePath = localCustomBaseUrl;
        } else if (localBasePaths.length > 0) {
            basePath = localBasePaths[localHostIndex];
        } else {
            basePath = null;
        }

        Object localVarPostBody = edgeRequest;

        // create path and map variables
        String localVarPath =
                "/v1/graph/{graph_id}/edge"
                        .replace(
                                "{" + "graph_id" + "}",
                                localVarApiClient.escapeString(graphId.toString()));

        List<Pair> localVarQueryParams = new ArrayList<Pair>();
        List<Pair> localVarCollectionQueryParams = new ArrayList<Pair>();
        Map<String, String> localVarHeaderParams = new HashMap<String, String>();
        Map<String, String> localVarCookieParams = new HashMap<String, String>();
        Map<String, Object> localVarFormParams = new HashMap<String, Object>();

        final String[] localVarAccepts = {"application/json"};
        final String localVarAccept = localVarApiClient.selectHeaderAccept(localVarAccepts);
        if (localVarAccept != null) {
            localVarHeaderParams.put("Accept", localVarAccept);
        }

        final String[] localVarContentTypes = {"application/json"};
        final String localVarContentType =
                localVarApiClient.selectHeaderContentType(localVarContentTypes);
        if (localVarContentType != null) {
            localVarHeaderParams.put("Content-Type", localVarContentType);
        }

        String[] localVarAuthNames = new String[] {};
        return localVarApiClient.buildCall(
                basePath,
                localVarPath,
                "POST",
                localVarQueryParams,
                localVarCollectionQueryParams,
                localVarPostBody,
                localVarHeaderParams,
                localVarCookieParams,
                localVarFormParams,
                localVarAuthNames,
                _callback);
    }

    @SuppressWarnings("rawtypes")
    private okhttp3.Call addEdgeValidateBeforeCall(
            String graphId, EdgeRequest edgeRequest, final ApiCallback _callback)
            throws ApiException {
        // verify the required parameter 'graphId' is set
        if (graphId == null) {
            throw new ApiException(
                    "Missing the required parameter 'graphId' when calling addEdge(Async)");
        }

        return addEdgeCall(graphId, edgeRequest, _callback);
    }

    /**
     * Add edge to the graph
     * Add the edge to graph.
     * @param graphId  (required)
     * @param edgeRequest  (optional)
     * @return String
     * @throws ApiException If fail to call the API, e.g. server error or cannot deserialize the response body
     * @http.response.details
     * <table summary="Response Details" border="1">
     * <tr><td> Status Code </td><td> Description </td><td> Response Headers </td></tr>
     * <tr><td> 200 </td><td> Successfully insert the edge </td><td>  -  </td></tr>
     * <tr><td> 400 </td><td> Invalid input edge </td><td>  -  </td></tr>
     * <tr><td> 409 </td><td> edge already exists </td><td>  -  </td></tr>
     * <tr><td> 500 </td><td> Server internal error </td><td>  -  </td></tr>
     * </table>
     */
    public String addEdge(String graphId, EdgeRequest edgeRequest) throws ApiException {
        ApiResponse<String> localVarResp = addEdgeWithHttpInfo(graphId, edgeRequest);
        return localVarResp.getData();
    }

    /**
     * Add edge to the graph
     * Add the edge to graph.
     * @param graphId  (required)
     * @param edgeRequest  (optional)
     * @return ApiResponse&lt;String&gt;
     * @throws ApiException If fail to call the API, e.g. server error or cannot deserialize the response body
     * @http.response.details
     * <table summary="Response Details" border="1">
     * <tr><td> Status Code </td><td> Description </td><td> Response Headers </td></tr>
     * <tr><td> 200 </td><td> Successfully insert the edge </td><td>  -  </td></tr>
     * <tr><td> 400 </td><td> Invalid input edge </td><td>  -  </td></tr>
     * <tr><td> 409 </td><td> edge already exists </td><td>  -  </td></tr>
     * <tr><td> 500 </td><td> Server internal error </td><td>  -  </td></tr>
     * </table>
     */
    public ApiResponse<String> addEdgeWithHttpInfo(String graphId, EdgeRequest edgeRequest)
            throws ApiException {
        okhttp3.Call localVarCall = addEdgeValidateBeforeCall(graphId, edgeRequest, null);
        Type localVarReturnType = new TypeToken<String>() {}.getType();
        return localVarApiClient.execute(localVarCall, localVarReturnType);
    }

    /**
     * Add edge to the graph (asynchronously)
     * Add the edge to graph.
     * @param graphId  (required)
     * @param edgeRequest  (optional)
     * @param _callback The callback to be executed when the API call finishes
     * @return The request call
     * @throws ApiException If fail to process the API call, e.g. serializing the request body object
     * @http.response.details
     * <table summary="Response Details" border="1">
     * <tr><td> Status Code </td><td> Description </td><td> Response Headers </td></tr>
     * <tr><td> 200 </td><td> Successfully insert the edge </td><td>  -  </td></tr>
     * <tr><td> 400 </td><td> Invalid input edge </td><td>  -  </td></tr>
     * <tr><td> 409 </td><td> edge already exists </td><td>  -  </td></tr>
     * <tr><td> 500 </td><td> Server internal error </td><td>  -  </td></tr>
     * </table>
     */
    public okhttp3.Call addEdgeAsync(
            String graphId, EdgeRequest edgeRequest, final ApiCallback<String> _callback)
            throws ApiException {

        okhttp3.Call localVarCall = addEdgeValidateBeforeCall(graphId, edgeRequest, _callback);
        Type localVarReturnType = new TypeToken<String>() {}.getType();
        localVarApiClient.executeAsync(localVarCall, localVarReturnType, _callback);
        return localVarCall;
    }
    /**
     * Build call for deleteEdge
     * @param graphId  (required)
     * @param srcLabel The label name of src vertex. (required)
     * @param srcPrimaryKeyValue The primary key value of src vertex. (required)
     * @param dstLabel The label name of dst vertex. (required)
     * @param dstPrimaryKeyValue The primary key value of dst vertex. (required)
     * @param _callback Callback for upload/download progress
     * @return Call to execute
     * @throws ApiException If fail to serialize the request body object
     * @http.response.details
     * <table summary="Response Details" border="1">
     * <tr><td> Status Code </td><td> Description </td><td> Response Headers </td></tr>
     * <tr><td> 200 </td><td> Successfully delete edge </td><td>  -  </td></tr>
     * <tr><td> 400 </td><td> Invalid input edge </td><td>  -  </td></tr>
     * <tr><td> 404 </td><td> Edge not exists or Graph not exits </td><td>  -  </td></tr>
     * <tr><td> 500 </td><td> Server internal error </td><td>  -  </td></tr>
     * </table>
     */
    public okhttp3.Call deleteEdgeCall(
            String graphId,
            String srcLabel,
            Object srcPrimaryKeyValue,
            String dstLabel,
            Object dstPrimaryKeyValue,
            final ApiCallback _callback)
            throws ApiException {
        String basePath = null;
        // Operation Servers
        String[] localBasePaths = new String[] {};

        // Determine Base Path to Use
        if (localCustomBaseUrl != null) {
            basePath = localCustomBaseUrl;
        } else if (localBasePaths.length > 0) {
            basePath = localBasePaths[localHostIndex];
        } else {
            basePath = null;
        }

        Object localVarPostBody = null;

        // create path and map variables
        String localVarPath =
                "/v1/graph/{graph_id}/edge"
                        .replace(
                                "{" + "graph_id" + "}",
                                localVarApiClient.escapeString(graphId.toString()));

        List<Pair> localVarQueryParams = new ArrayList<Pair>();
        List<Pair> localVarCollectionQueryParams = new ArrayList<Pair>();
        Map<String, String> localVarHeaderParams = new HashMap<String, String>();
        Map<String, String> localVarCookieParams = new HashMap<String, String>();
        Map<String, Object> localVarFormParams = new HashMap<String, Object>();

        if (srcLabel != null) {
            localVarQueryParams.addAll(localVarApiClient.parameterToPair("src_label", srcLabel));
        }

        if (srcPrimaryKeyValue != null) {
            localVarQueryParams.addAll(
                    localVarApiClient.parameterToPair("src_primary_key_value", srcPrimaryKeyValue));
        }

        if (dstLabel != null) {
            localVarQueryParams.addAll(localVarApiClient.parameterToPair("dst_label", dstLabel));
        }

        if (dstPrimaryKeyValue != null) {
            localVarQueryParams.addAll(
                    localVarApiClient.parameterToPair("dst_primary_key_value", dstPrimaryKeyValue));
        }

        final String[] localVarAccepts = {"application/json"};
        final String localVarAccept = localVarApiClient.selectHeaderAccept(localVarAccepts);
        if (localVarAccept != null) {
            localVarHeaderParams.put("Accept", localVarAccept);
        }

        final String[] localVarContentTypes = {};
        final String localVarContentType =
                localVarApiClient.selectHeaderContentType(localVarContentTypes);
        if (localVarContentType != null) {
            localVarHeaderParams.put("Content-Type", localVarContentType);
        }

        String[] localVarAuthNames = new String[] {};
        return localVarApiClient.buildCall(
                basePath,
                localVarPath,
                "DELETE",
                localVarQueryParams,
                localVarCollectionQueryParams,
                localVarPostBody,
                localVarHeaderParams,
                localVarCookieParams,
                localVarFormParams,
                localVarAuthNames,
                _callback);
    }

    @SuppressWarnings("rawtypes")
    private okhttp3.Call deleteEdgeValidateBeforeCall(
            String graphId,
            String srcLabel,
            Object srcPrimaryKeyValue,
            String dstLabel,
            Object dstPrimaryKeyValue,
            final ApiCallback _callback)
            throws ApiException {
        // verify the required parameter 'graphId' is set
        if (graphId == null) {
            throw new ApiException(
                    "Missing the required parameter 'graphId' when calling deleteEdge(Async)");
        }

        // verify the required parameter 'srcLabel' is set
        if (srcLabel == null) {
            throw new ApiException(
                    "Missing the required parameter 'srcLabel' when calling deleteEdge(Async)");
        }

        // verify the required parameter 'srcPrimaryKeyValue' is set
        if (srcPrimaryKeyValue == null) {
            throw new ApiException(
                    "Missing the required parameter 'srcPrimaryKeyValue' when calling"
                            + " deleteEdge(Async)");
        }

        // verify the required parameter 'dstLabel' is set
        if (dstLabel == null) {
            throw new ApiException(
                    "Missing the required parameter 'dstLabel' when calling deleteEdge(Async)");
        }

        // verify the required parameter 'dstPrimaryKeyValue' is set
        if (dstPrimaryKeyValue == null) {
            throw new ApiException(
                    "Missing the required parameter 'dstPrimaryKeyValue' when calling"
                            + " deleteEdge(Async)");
        }

        return deleteEdgeCall(
                graphId, srcLabel, srcPrimaryKeyValue, dstLabel, dstPrimaryKeyValue, _callback);
    }

    /**
     * Remove edge from the graph
     * Remove the edge from current graph.
     * @param graphId  (required)
     * @param srcLabel The label name of src vertex. (required)
     * @param srcPrimaryKeyValue The primary key value of src vertex. (required)
     * @param dstLabel The label name of dst vertex. (required)
     * @param dstPrimaryKeyValue The primary key value of dst vertex. (required)
     * @return String
     * @throws ApiException If fail to call the API, e.g. server error or cannot deserialize the response body
     * @http.response.details
     * <table summary="Response Details" border="1">
     * <tr><td> Status Code </td><td> Description </td><td> Response Headers </td></tr>
     * <tr><td> 200 </td><td> Successfully delete edge </td><td>  -  </td></tr>
     * <tr><td> 400 </td><td> Invalid input edge </td><td>  -  </td></tr>
     * <tr><td> 404 </td><td> Edge not exists or Graph not exits </td><td>  -  </td></tr>
     * <tr><td> 500 </td><td> Server internal error </td><td>  -  </td></tr>
     * </table>
     */
    public String deleteEdge(
            String graphId,
            String srcLabel,
            Object srcPrimaryKeyValue,
            String dstLabel,
            Object dstPrimaryKeyValue)
            throws ApiException {
        ApiResponse<String> localVarResp =
                deleteEdgeWithHttpInfo(
                        graphId, srcLabel, srcPrimaryKeyValue, dstLabel, dstPrimaryKeyValue);
        return localVarResp.getData();
    }

    /**
     * Remove edge from the graph
     * Remove the edge from current graph.
     * @param graphId  (required)
     * @param srcLabel The label name of src vertex. (required)
     * @param srcPrimaryKeyValue The primary key value of src vertex. (required)
     * @param dstLabel The label name of dst vertex. (required)
     * @param dstPrimaryKeyValue The primary key value of dst vertex. (required)
     * @return ApiResponse&lt;String&gt;
     * @throws ApiException If fail to call the API, e.g. server error or cannot deserialize the response body
     * @http.response.details
     * <table summary="Response Details" border="1">
     * <tr><td> Status Code </td><td> Description </td><td> Response Headers </td></tr>
     * <tr><td> 200 </td><td> Successfully delete edge </td><td>  -  </td></tr>
     * <tr><td> 400 </td><td> Invalid input edge </td><td>  -  </td></tr>
     * <tr><td> 404 </td><td> Edge not exists or Graph not exits </td><td>  -  </td></tr>
     * <tr><td> 500 </td><td> Server internal error </td><td>  -  </td></tr>
     * </table>
     */
    public ApiResponse<String> deleteEdgeWithHttpInfo(
            String graphId,
            String srcLabel,
            Object srcPrimaryKeyValue,
            String dstLabel,
            Object dstPrimaryKeyValue)
            throws ApiException {
        okhttp3.Call localVarCall =
                deleteEdgeValidateBeforeCall(
                        graphId, srcLabel, srcPrimaryKeyValue, dstLabel, dstPrimaryKeyValue, null);
        Type localVarReturnType = new TypeToken<String>() {}.getType();
        return localVarApiClient.execute(localVarCall, localVarReturnType);
    }

    /**
     * Remove edge from the graph (asynchronously)
     * Remove the edge from current graph.
     * @param graphId  (required)
     * @param srcLabel The label name of src vertex. (required)
     * @param srcPrimaryKeyValue The primary key value of src vertex. (required)
     * @param dstLabel The label name of dst vertex. (required)
     * @param dstPrimaryKeyValue The primary key value of dst vertex. (required)
     * @param _callback The callback to be executed when the API call finishes
     * @return The request call
     * @throws ApiException If fail to process the API call, e.g. serializing the request body object
     * @http.response.details
     * <table summary="Response Details" border="1">
     * <tr><td> Status Code </td><td> Description </td><td> Response Headers </td></tr>
     * <tr><td> 200 </td><td> Successfully delete edge </td><td>  -  </td></tr>
     * <tr><td> 400 </td><td> Invalid input edge </td><td>  -  </td></tr>
     * <tr><td> 404 </td><td> Edge not exists or Graph not exits </td><td>  -  </td></tr>
     * <tr><td> 500 </td><td> Server internal error </td><td>  -  </td></tr>
     * </table>
     */
    public okhttp3.Call deleteEdgeAsync(
            String graphId,
            String srcLabel,
            Object srcPrimaryKeyValue,
            String dstLabel,
            Object dstPrimaryKeyValue,
            final ApiCallback<String> _callback)
            throws ApiException {

        okhttp3.Call localVarCall =
                deleteEdgeValidateBeforeCall(
                        graphId,
                        srcLabel,
                        srcPrimaryKeyValue,
                        dstLabel,
                        dstPrimaryKeyValue,
                        _callback);
        Type localVarReturnType = new TypeToken<String>() {}.getType();
        localVarApiClient.executeAsync(localVarCall, localVarReturnType, _callback);
        return localVarCall;
    }
    /**
     * Build call for getEdge
     * @param graphId  (required)
     * @param edgeLabel The label name of querying edge. (required)
     * @param srcLabel The label name of src vertex. (required)
     * @param srcPrimaryKeyValue The primary key value of src vertex. (required)
     * @param dstLabel The label name of dst vertex. (required)
     * @param dstPrimaryKeyValue The value of dst vertex&#39;s primary key (required)
     * @param _callback Callback for upload/download progress
     * @return Call to execute
     * @throws ApiException If fail to serialize the request body object
     * @http.response.details
     * <table summary="Response Details" border="1">
     * <tr><td> Status Code </td><td> Description </td><td> Response Headers </td></tr>
     * <tr><td> 200 </td><td> Found Edge </td><td>  -  </td></tr>
     * <tr><td> 400 </td><td> Bad input parameter </td><td>  -  </td></tr>
     * <tr><td> 404 </td><td> Edge not found or Graph not found </td><td>  -  </td></tr>
     * <tr><td> 500 </td><td> Server internal error </td><td>  -  </td></tr>
     * </table>
     */
    public okhttp3.Call getEdgeCall(
            String graphId,
            String edgeLabel,
            String srcLabel,
            Object srcPrimaryKeyValue,
            String dstLabel,
            Object dstPrimaryKeyValue,
            final ApiCallback _callback)
            throws ApiException {
        String basePath = null;
        // Operation Servers
        String[] localBasePaths = new String[] {};

        // Determine Base Path to Use
        if (localCustomBaseUrl != null) {
            basePath = localCustomBaseUrl;
        } else if (localBasePaths.length > 0) {
            basePath = localBasePaths[localHostIndex];
        } else {
            basePath = null;
        }

        Object localVarPostBody = null;

        // create path and map variables
        String localVarPath =
                "/v1/graph/{graph_id}/edge"
                        .replace(
                                "{" + "graph_id" + "}",
                                localVarApiClient.escapeString(graphId.toString()));

        List<Pair> localVarQueryParams = new ArrayList<Pair>();
        List<Pair> localVarCollectionQueryParams = new ArrayList<Pair>();
        Map<String, String> localVarHeaderParams = new HashMap<String, String>();
        Map<String, String> localVarCookieParams = new HashMap<String, String>();
        Map<String, Object> localVarFormParams = new HashMap<String, Object>();

        if (edgeLabel != null) {
            localVarQueryParams.addAll(localVarApiClient.parameterToPair("edge_label", edgeLabel));
        }

        if (srcLabel != null) {
            localVarQueryParams.addAll(localVarApiClient.parameterToPair("src_label", srcLabel));
        }

        if (srcPrimaryKeyValue != null) {
            localVarQueryParams.addAll(
                    localVarApiClient.parameterToPair("src_primary_key_value", srcPrimaryKeyValue));
        }

        if (dstLabel != null) {
            localVarQueryParams.addAll(localVarApiClient.parameterToPair("dst_label", dstLabel));
        }

        if (dstPrimaryKeyValue != null) {
            localVarQueryParams.addAll(
                    localVarApiClient.parameterToPair("dst_primary_key_value", dstPrimaryKeyValue));
        }

        final String[] localVarAccepts = {"application/json"};
        final String localVarAccept = localVarApiClient.selectHeaderAccept(localVarAccepts);
        if (localVarAccept != null) {
            localVarHeaderParams.put("Accept", localVarAccept);
        }

        final String[] localVarContentTypes = {};
        final String localVarContentType =
                localVarApiClient.selectHeaderContentType(localVarContentTypes);
        if (localVarContentType != null) {
            localVarHeaderParams.put("Content-Type", localVarContentType);
        }

        String[] localVarAuthNames = new String[] {};
        return localVarApiClient.buildCall(
                basePath,
                localVarPath,
                "GET",
                localVarQueryParams,
                localVarCollectionQueryParams,
                localVarPostBody,
                localVarHeaderParams,
                localVarCookieParams,
                localVarFormParams,
                localVarAuthNames,
                _callback);
    }

    @SuppressWarnings("rawtypes")
    private okhttp3.Call getEdgeValidateBeforeCall(
            String graphId,
            String edgeLabel,
            String srcLabel,
            Object srcPrimaryKeyValue,
            String dstLabel,
            Object dstPrimaryKeyValue,
            final ApiCallback _callback)
            throws ApiException {
        // verify the required parameter 'graphId' is set
        if (graphId == null) {
            throw new ApiException(
                    "Missing the required parameter 'graphId' when calling getEdge(Async)");
        }

        // verify the required parameter 'edgeLabel' is set
        if (edgeLabel == null) {
            throw new ApiException(
                    "Missing the required parameter 'edgeLabel' when calling getEdge(Async)");
        }

        // verify the required parameter 'srcLabel' is set
        if (srcLabel == null) {
            throw new ApiException(
                    "Missing the required parameter 'srcLabel' when calling getEdge(Async)");
        }

        // verify the required parameter 'srcPrimaryKeyValue' is set
        if (srcPrimaryKeyValue == null) {
            throw new ApiException(
                    "Missing the required parameter 'srcPrimaryKeyValue' when calling"
                            + " getEdge(Async)");
        }

        // verify the required parameter 'dstLabel' is set
        if (dstLabel == null) {
            throw new ApiException(
                    "Missing the required parameter 'dstLabel' when calling getEdge(Async)");
        }

        // verify the required parameter 'dstPrimaryKeyValue' is set
        if (dstPrimaryKeyValue == null) {
            throw new ApiException(
                    "Missing the required parameter 'dstPrimaryKeyValue' when calling"
                            + " getEdge(Async)");
        }

        return getEdgeCall(
                graphId,
                edgeLabel,
                srcLabel,
                srcPrimaryKeyValue,
                dstLabel,
                dstPrimaryKeyValue,
                _callback);
    }

    /**
     * Get the edge&#39;s properties with src and dst vertex primary keys.
     * Get the properties for the specified vertex.
     * @param graphId  (required)
     * @param edgeLabel The label name of querying edge. (required)
     * @param srcLabel The label name of src vertex. (required)
     * @param srcPrimaryKeyValue The primary key value of src vertex. (required)
     * @param dstLabel The label name of dst vertex. (required)
     * @param dstPrimaryKeyValue The value of dst vertex&#39;s primary key (required)
     * @return EdgeData
     * @throws ApiException If fail to call the API, e.g. server error or cannot deserialize the response body
     * @http.response.details
     * <table summary="Response Details" border="1">
     * <tr><td> Status Code </td><td> Description </td><td> Response Headers </td></tr>
     * <tr><td> 200 </td><td> Found Edge </td><td>  -  </td></tr>
     * <tr><td> 400 </td><td> Bad input parameter </td><td>  -  </td></tr>
     * <tr><td> 404 </td><td> Edge not found or Graph not found </td><td>  -  </td></tr>
     * <tr><td> 500 </td><td> Server internal error </td><td>  -  </td></tr>
     * </table>
     */
    public EdgeData getEdge(
            String graphId,
            String edgeLabel,
            String srcLabel,
            Object srcPrimaryKeyValue,
            String dstLabel,
            Object dstPrimaryKeyValue)
            throws ApiException {
        ApiResponse<EdgeData> localVarResp =
                getEdgeWithHttpInfo(
                        graphId,
                        edgeLabel,
                        srcLabel,
                        srcPrimaryKeyValue,
                        dstLabel,
                        dstPrimaryKeyValue);
        return localVarResp.getData();
    }

    /**
     * Get the edge&#39;s properties with src and dst vertex primary keys.
     * Get the properties for the specified vertex.
     * @param graphId  (required)
     * @param edgeLabel The label name of querying edge. (required)
     * @param srcLabel The label name of src vertex. (required)
     * @param srcPrimaryKeyValue The primary key value of src vertex. (required)
     * @param dstLabel The label name of dst vertex. (required)
     * @param dstPrimaryKeyValue The value of dst vertex&#39;s primary key (required)
     * @return ApiResponse&lt;EdgeData&gt;
     * @throws ApiException If fail to call the API, e.g. server error or cannot deserialize the response body
     * @http.response.details
     * <table summary="Response Details" border="1">
     * <tr><td> Status Code </td><td> Description </td><td> Response Headers </td></tr>
     * <tr><td> 200 </td><td> Found Edge </td><td>  -  </td></tr>
     * <tr><td> 400 </td><td> Bad input parameter </td><td>  -  </td></tr>
     * <tr><td> 404 </td><td> Edge not found or Graph not found </td><td>  -  </td></tr>
     * <tr><td> 500 </td><td> Server internal error </td><td>  -  </td></tr>
     * </table>
     */
    public ApiResponse<EdgeData> getEdgeWithHttpInfo(
            String graphId,
            String edgeLabel,
            String srcLabel,
            Object srcPrimaryKeyValue,
            String dstLabel,
            Object dstPrimaryKeyValue)
            throws ApiException {
        okhttp3.Call localVarCall =
                getEdgeValidateBeforeCall(
                        graphId,
                        edgeLabel,
                        srcLabel,
                        srcPrimaryKeyValue,
                        dstLabel,
                        dstPrimaryKeyValue,
                        null);
        Type localVarReturnType = new TypeToken<EdgeData>() {}.getType();
        return localVarApiClient.execute(localVarCall, localVarReturnType);
    }

    /**
     * Get the edge&#39;s properties with src and dst vertex primary keys. (asynchronously)
     * Get the properties for the specified vertex.
     * @param graphId  (required)
     * @param edgeLabel The label name of querying edge. (required)
     * @param srcLabel The label name of src vertex. (required)
     * @param srcPrimaryKeyValue The primary key value of src vertex. (required)
     * @param dstLabel The label name of dst vertex. (required)
     * @param dstPrimaryKeyValue The value of dst vertex&#39;s primary key (required)
     * @param _callback The callback to be executed when the API call finishes
     * @return The request call
     * @throws ApiException If fail to process the API call, e.g. serializing the request body object
     * @http.response.details
     * <table summary="Response Details" border="1">
     * <tr><td> Status Code </td><td> Description </td><td> Response Headers </td></tr>
     * <tr><td> 200 </td><td> Found Edge </td><td>  -  </td></tr>
     * <tr><td> 400 </td><td> Bad input parameter </td><td>  -  </td></tr>
     * <tr><td> 404 </td><td> Edge not found or Graph not found </td><td>  -  </td></tr>
     * <tr><td> 500 </td><td> Server internal error </td><td>  -  </td></tr>
     * </table>
     */
    public okhttp3.Call getEdgeAsync(
            String graphId,
            String edgeLabel,
            String srcLabel,
            Object srcPrimaryKeyValue,
            String dstLabel,
            Object dstPrimaryKeyValue,
            final ApiCallback<EdgeData> _callback)
            throws ApiException {

        okhttp3.Call localVarCall =
                getEdgeValidateBeforeCall(
                        graphId,
                        edgeLabel,
                        srcLabel,
                        srcPrimaryKeyValue,
                        dstLabel,
                        dstPrimaryKeyValue,
                        _callback);
        Type localVarReturnType = new TypeToken<EdgeData>() {}.getType();
        localVarApiClient.executeAsync(localVarCall, localVarReturnType, _callback);
        return localVarCall;
    }
    /**
     * Build call for updateEdge
     * @param graphId  (required)
     * @param edgeRequest  (optional)
     * @param _callback Callback for upload/download progress
     * @return Call to execute
     * @throws ApiException If fail to serialize the request body object
     * @http.response.details
     * <table summary="Response Details" border="1">
     * <tr><td> Status Code </td><td> Description </td><td> Response Headers </td></tr>
     * <tr><td> 200 </td><td> Successfully update edge </td><td>  -  </td></tr>
     * <tr><td> 400 </td><td> Invalid input paramters </td><td>  -  </td></tr>
     * <tr><td> 404 </td><td> Edge not exists </td><td>  -  </td></tr>
     * <tr><td> 500 </td><td> Server internal error </td><td>  -  </td></tr>
     * </table>
     */
    public okhttp3.Call updateEdgeCall(
            String graphId, EdgeRequest edgeRequest, final ApiCallback _callback)
            throws ApiException {
        String basePath = null;
        // Operation Servers
        String[] localBasePaths = new String[] {};

        // Determine Base Path to Use
        if (localCustomBaseUrl != null) {
            basePath = localCustomBaseUrl;
        } else if (localBasePaths.length > 0) {
            basePath = localBasePaths[localHostIndex];
        } else {
            basePath = null;
        }

        Object localVarPostBody = edgeRequest;

        // create path and map variables
        String localVarPath =
                "/v1/graph/{graph_id}/edge"
                        .replace(
                                "{" + "graph_id" + "}",
                                localVarApiClient.escapeString(graphId.toString()));

        List<Pair> localVarQueryParams = new ArrayList<Pair>();
        List<Pair> localVarCollectionQueryParams = new ArrayList<Pair>();
        Map<String, String> localVarHeaderParams = new HashMap<String, String>();
        Map<String, String> localVarCookieParams = new HashMap<String, String>();
        Map<String, Object> localVarFormParams = new HashMap<String, Object>();

        final String[] localVarAccepts = {"application/json"};
        final String localVarAccept = localVarApiClient.selectHeaderAccept(localVarAccepts);
        if (localVarAccept != null) {
            localVarHeaderParams.put("Accept", localVarAccept);
        }

        final String[] localVarContentTypes = {"application/json"};
        final String localVarContentType =
                localVarApiClient.selectHeaderContentType(localVarContentTypes);
        if (localVarContentType != null) {
            localVarHeaderParams.put("Content-Type", localVarContentType);
        }

        String[] localVarAuthNames = new String[] {};
        return localVarApiClient.buildCall(
                basePath,
                localVarPath,
                "PUT",
                localVarQueryParams,
                localVarCollectionQueryParams,
                localVarPostBody,
                localVarHeaderParams,
                localVarCookieParams,
                localVarFormParams,
                localVarAuthNames,
                _callback);
    }

    @SuppressWarnings("rawtypes")
    private okhttp3.Call updateEdgeValidateBeforeCall(
            String graphId, EdgeRequest edgeRequest, final ApiCallback _callback)
            throws ApiException {
        // verify the required parameter 'graphId' is set
        if (graphId == null) {
            throw new ApiException(
                    "Missing the required parameter 'graphId' when calling updateEdge(Async)");
        }

        return updateEdgeCall(graphId, edgeRequest, _callback);
    }

    /**
     * Update edge&#39;s property
     * Update the edge on the running graph.
     * @param graphId  (required)
     * @param edgeRequest  (optional)
     * @return String
     * @throws ApiException If fail to call the API, e.g. server error or cannot deserialize the response body
     * @http.response.details
     * <table summary="Response Details" border="1">
     * <tr><td> Status Code </td><td> Description </td><td> Response Headers </td></tr>
     * <tr><td> 200 </td><td> Successfully update edge </td><td>  -  </td></tr>
     * <tr><td> 400 </td><td> Invalid input paramters </td><td>  -  </td></tr>
     * <tr><td> 404 </td><td> Edge not exists </td><td>  -  </td></tr>
     * <tr><td> 500 </td><td> Server internal error </td><td>  -  </td></tr>
     * </table>
     */
    public String updateEdge(String graphId, EdgeRequest edgeRequest) throws ApiException {
        ApiResponse<String> localVarResp = updateEdgeWithHttpInfo(graphId, edgeRequest);
        return localVarResp.getData();
    }

    /**
     * Update edge&#39;s property
     * Update the edge on the running graph.
     * @param graphId  (required)
     * @param edgeRequest  (optional)
     * @return ApiResponse&lt;String&gt;
     * @throws ApiException If fail to call the API, e.g. server error or cannot deserialize the response body
     * @http.response.details
     * <table summary="Response Details" border="1">
     * <tr><td> Status Code </td><td> Description </td><td> Response Headers </td></tr>
     * <tr><td> 200 </td><td> Successfully update edge </td><td>  -  </td></tr>
     * <tr><td> 400 </td><td> Invalid input paramters </td><td>  -  </td></tr>
     * <tr><td> 404 </td><td> Edge not exists </td><td>  -  </td></tr>
     * <tr><td> 500 </td><td> Server internal error </td><td>  -  </td></tr>
     * </table>
     */
    public ApiResponse<String> updateEdgeWithHttpInfo(String graphId, EdgeRequest edgeRequest)
            throws ApiException {
        okhttp3.Call localVarCall = updateEdgeValidateBeforeCall(graphId, edgeRequest, null);
        Type localVarReturnType = new TypeToken<String>() {}.getType();
        return localVarApiClient.execute(localVarCall, localVarReturnType);
    }

    /**
     * Update edge&#39;s property (asynchronously)
     * Update the edge on the running graph.
     * @param graphId  (required)
     * @param edgeRequest  (optional)
     * @param _callback The callback to be executed when the API call finishes
     * @return The request call
     * @throws ApiException If fail to process the API call, e.g. serializing the request body object
     * @http.response.details
     * <table summary="Response Details" border="1">
     * <tr><td> Status Code </td><td> Description </td><td> Response Headers </td></tr>
     * <tr><td> 200 </td><td> Successfully update edge </td><td>  -  </td></tr>
     * <tr><td> 400 </td><td> Invalid input paramters </td><td>  -  </td></tr>
     * <tr><td> 404 </td><td> Edge not exists </td><td>  -  </td></tr>
     * <tr><td> 500 </td><td> Server internal error </td><td>  -  </td></tr>
     * </table>
     */
    public okhttp3.Call updateEdgeAsync(
            String graphId, EdgeRequest edgeRequest, final ApiCallback<String> _callback)
            throws ApiException {

        okhttp3.Call localVarCall = updateEdgeValidateBeforeCall(graphId, edgeRequest, _callback);
        Type localVarReturnType = new TypeToken<String>() {}.getType();
        localVarApiClient.executeAsync(localVarCall, localVarReturnType, _callback);
        return localVarCall;
    }
}

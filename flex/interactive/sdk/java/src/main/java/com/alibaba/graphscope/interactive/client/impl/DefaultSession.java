/*
 * Copyright 2022 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.alibaba.graphscope.interactive.client.impl;

import com.alibaba.graphscope.gaia.proto.IrResult;
import com.alibaba.graphscope.interactive.ApiClient;
import com.alibaba.graphscope.interactive.ApiException;
import com.alibaba.graphscope.interactive.ApiResponse;
import com.alibaba.graphscope.interactive.api.*;
import com.alibaba.graphscope.interactive.client.Session;
import com.alibaba.graphscope.interactive.client.common.Result;
import com.alibaba.graphscope.interactive.client.common.Status;
import com.alibaba.graphscope.interactive.models.*;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.Closeable;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

/***
 * A default implementation of the GraphScope interactive session interface.
 * Based on the code generated by OpenAPI Generator.
 */
public class DefaultSession implements Session {
    private static final int DEFAULT_READ_TIMEOUT = 30000;
    private static final int DEFAULT_WRITE_TIMEOUT = 30000;
    private final AdminServiceGraphManagementApi graphApi;
    private final AdminServiceJobManagementApi jobApi;
    private final AdminServiceProcedureManagementApi procedureApi;
    private final AdminServiceServiceManagementApi serviceApi;
    private final GraphServiceVertexManagementApi vertexApi;
    private final GraphServiceEdgeManagementApi edgeApi;
    private final QueryServiceApi queryApi;
    private final UtilsApi utilsApi;
    private final ApiClient client, queryClient;

    /**
     * Create a default GraphScope Interactive Session.
     *
     * @param uri should be in the format "http://host:port"
     */
    private DefaultSession(String uri, String storedProcUri) {
        client = new ApiClient();
        client.setBasePath(uri);
        client.setReadTimeout(DEFAULT_READ_TIMEOUT);
        client.setWriteTimeout(DEFAULT_WRITE_TIMEOUT);

        graphApi = new AdminServiceGraphManagementApi(client);
        jobApi = new AdminServiceJobManagementApi(client);
        procedureApi = new AdminServiceProcedureManagementApi(client);
        serviceApi = new AdminServiceServiceManagementApi(client);
        vertexApi = new GraphServiceVertexManagementApi(client);
        edgeApi = new GraphServiceEdgeManagementApi(client);

        utilsApi = new UtilsApi(client);

        if (storedProcUri == null) {
            Result<ServiceStatus> status = getServiceStatus();
            if (!status.isOk()) {
                throw new RuntimeException(
                        "Failed to connect to the server: " + status.getStatusMessage());
            }
            // TODO: should construct queryService from a endpoint, not a port
            Integer queryPort = status.getValue().getHqpsPort();

            // Replace the port with the query port, http:://host:port -> http:://host:queryPort
            storedProcUri = uri.replaceFirst(":[0-9]+", ":" + queryPort);
            System.out.println("Query URI: " + storedProcUri);
        }
        queryClient = new ApiClient();
        queryClient.setBasePath(storedProcUri);
        queryClient.setReadTimeout(DEFAULT_READ_TIMEOUT);
        queryClient.setWriteTimeout(DEFAULT_WRITE_TIMEOUT);
        queryApi = new QueryServiceApi(queryClient);
    }

    public static DefaultSession newInstance(String adminUri) {
        return new DefaultSession(adminUri, null);
    }

    public static DefaultSession newInstance(String adminUri, String storedProcUri) {
        return new DefaultSession(adminUri, storedProcUri);
    }

    /**
     * Try to upload the input files if they are specified with a starting @
     * for input files in schema_mapping. Replace the path to the uploaded file with the
     * path returned from the server.
     * <p>
     * The @ can be added to the beginning of data_source.location in schema_mapping.loading_config
     * or added to each file in vertex_mappings and edge_mappings.
     * <p>
     * 1. location: @/path/to/dir
     * inputs:
     * - @/path/to/file1
     * - @/path/to/file2
     * 2. location: /path/to/dir
     * inputs:
     * - @/path/to/file1
     * - @/path/to/file2
     * 3. location: @/path/to/dir
     * inputs:
     * - /path/to/file1
     * - /path/to/file2
     * 4. location: /path/to/dir
     * inputs:
     * - /path/to/file1
     * - /path/to/file2
     * 4. location: None
     * inputs:
     * - @/path/to/file1
     * - @/path/to/file2
     * Among the above 4 cases, only the 1, 3, 5 case are valid, for 2,4 the file will not be uploaded
     *
     * @param schemaMapping schema mapping object
     * @return true if all files are uploaded successfully, false otherwise
     */
    private static Result<SchemaMapping> validateSchemaMapping(SchemaMapping schemaMapping) {
        if (schemaMapping == null) {
            return new Result<SchemaMapping>(Status.badRequest("Schema mapping is null"), null);
        }
        boolean rootLocationMarkedUploaded = false;
        String location = null;
        SchemaMappingLoadingConfig config = schemaMapping.getLoadingConfig();
        if (config != null) {
            if (config.getDataSource() != null && config.getDataSource().getScheme() != null) {
                if (config.getDataSource()
                        .getScheme()
                        .equals(SchemaMappingLoadingConfigDataSource.SchemeEnum.FILE)) {
                    location = config.getDataSource().getLocation();
                } else {
                    return new Result<SchemaMapping>(
                            Status.ok("Only FILE scheme is supported"), schemaMapping);
                }
            }
        }
        if (location != null && location.startsWith("@")) {
            rootLocationMarkedUploaded = true;
        }

        List<String> extractedFiles = new ArrayList<>();
        if (schemaMapping.getVertexMappings() != null) {
            for (VertexMapping item : schemaMapping.getVertexMappings()) {
                if (item.getInputs() != null) {
                    for (int i = 0; i < item.getInputs().size(); ++i) {
                        String input = item.getInputs().get(i);
                        if (location != null && !rootLocationMarkedUploaded) {
                            if (input.startsWith("@")) {
                                return new Result<SchemaMapping>(
                                        Status.badRequest(
                                                "Root location given without @, but the input file"
                                                        + " starts with @"
                                                        + input),
                                        null);
                            }
                        }
                        if (location != null) {
                            input = location + "/" + trimPath(input);
                        }
                        item.getInputs().set(i, input);
                        extractedFiles.add(input);
                    }
                }
            }
        }
        if (schemaMapping.getEdgeMappings() != null) {
            for (EdgeMapping item : schemaMapping.getEdgeMappings()) {
                if (item.getInputs() != null) {
                    for (int i = 0; i < item.getInputs().size(); ++i) {
                        String input = item.getInputs().get(i);
                        if (location != null && !rootLocationMarkedUploaded) {
                            if (input.startsWith("@")) {
                                return new Result<SchemaMapping>(
                                        Status.badRequest(
                                                "Root location given without @, but the input file"
                                                        + " starts with @"
                                                        + input),
                                        null);
                            }
                        }
                        if (location != null) {
                            input = location + "/" + trimPath(input);
                        }
                        item.getInputs().set(i, input);
                        extractedFiles.add(input);
                    }
                }
            }
        }
        {
            int count = 0;
            for (String file : extractedFiles) {
                if (file.startsWith("@")) {
                    count += 1;
                }
            }
            if (count == 0) {
                System.out.println("No files to upload");
                return Result.ok(schemaMapping);
            } else if (count != extractedFiles.size()) {
                System.err.println("Can not mix uploading file and not uploading file");
                return Result.error("Can not mix uploading file and not uploading file");
            }
        }
        return Result.ok(schemaMapping);
    }

    private Result<SchemaMapping> uploadFilesAndUpdate(SchemaMapping schemaMapping) {
        if (schemaMapping.getVertexMappings() != null) {
            for (VertexMapping item : schemaMapping.getVertexMappings()) {
                if (item.getInputs() != null) {
                    for (int i = 0; i < item.getInputs().size(); ++i) {
                        String input = item.getInputs().get(i);
                        if (input.startsWith("@")) {
                            input = input.substring(1);
                            File file = new File(input);
                            if (!file.exists()) {
                                return new Result<SchemaMapping>(
                                        Status.badRequest("File does not exist: " + input),
                                        schemaMapping);
                            }

                            Result<UploadFileResponse> uploadedFile = uploadFile(file);
                            if (!uploadedFile.isOk()) {
                                return new Result<SchemaMapping>(
                                        Status.badRequest(
                                                "Failed to upload file: "
                                                        + input
                                                        + ", "
                                                        + uploadedFile.getStatusMessage()),
                                        schemaMapping);
                            }
                            item.getInputs().set(i, uploadedFile.getValue().getFilePath());
                        }
                    }
                }
            }
        }
        if (schemaMapping.getEdgeMappings() != null) {
            for (EdgeMapping item : schemaMapping.getEdgeMappings()) {
                if (item.getInputs() != null) {
                    for (int i = 0; i < item.getInputs().size(); ++i) {
                        String input = item.getInputs().get(i);
                        if (input.startsWith("@")) {
                            input = input.substring(1);
                            File file = new File(input);
                            if (!file.exists()) {
                                return new Result<SchemaMapping>(
                                        Status.badRequest("File does not exist: " + input),
                                        schemaMapping);
                            }
                            Result<UploadFileResponse> uploadedFile = uploadFile(file);
                            if (!uploadedFile.isOk()) {
                                return new Result<SchemaMapping>(
                                        Status.badRequest(
                                                "Failed to upload file: "
                                                        + input
                                                        + ", "
                                                        + uploadedFile.getStatusMessage()),
                                        schemaMapping);
                            }
                            item.getInputs().set(i, uploadedFile.getValue().getFilePath());
                        }
                    }
                }
            }
        }
        return Result.ok(schemaMapping);
    }

    /**
     * Remove the @ if it is present in the path
     *
     * @param path
     * @return
     */
    private static String trimPath(String path) {
        if (path == null) {
            throw new IllegalArgumentException("path cannot be null");
        }
        return path.startsWith("@") ? path.substring(1) : path;
    }

    private Result<SchemaMapping> tryUploadFile(SchemaMapping schemaMapping) {
        Result<SchemaMapping> validateResult = validateSchemaMapping(schemaMapping);
        if (!validateResult.isOk()) {
            return new Result<SchemaMapping>(
                    Status.badRequest("validation failed for schema mapping"), schemaMapping);
        }
        SchemaMapping validatedSchemaMapping = validateResult.getValue();
        System.out.println("Schema mapping validated successfully");
        Result<SchemaMapping> uploadResult = uploadFilesAndUpdate(validatedSchemaMapping);
        if (!uploadResult.isOk()) {
            return new Result<SchemaMapping>(
                    Status.badRequest("upload failed for schema mapping"), schemaMapping);
        }
        return uploadResult;
    }

    @Override
    public Result<EdgeData> getEdge(
            String graphName,
            String edgeLabel,
            String srcLabel,
            Object srcPrimaryKeyValue,
            String dstLabel,
            Object dstPrimaryKeyValue) {
        try {
            ApiResponse<EdgeData> response =
                    edgeApi.getEdgeWithHttpInfo(
                            graphName,
                            edgeLabel,
                            srcLabel,
                            srcPrimaryKeyValue,
                            dstLabel,
                            dstPrimaryKeyValue);
            return Result.fromResponse(response);
        } catch (ApiException e) {
            e.printStackTrace();
            return Result.fromException(e);
        }
    }

    @Override
    public Result<String> addEdge(String graphName, EdgeRequest edgeRequest) {
        try {
            ApiResponse<String> response = edgeApi.addEdgeWithHttpInfo(graphName, edgeRequest);
            return Result.fromResponse(response);
        } catch (ApiException e) {
            e.printStackTrace();
            return Result.fromException(e);
        }
    }

    @Override
    public Result<String> deleteEdge(
            String graphName,
            String srcLabel,
            Object srcPrimaryKeyValue,
            String dstLabel,
            Object dstPrimaryKeyValue) {
        try {
            ApiResponse<String> response =
                    edgeApi.deleteEdgeWithHttpInfo(
                            graphName, srcLabel, srcPrimaryKeyValue, dstLabel, dstPrimaryKeyValue);
            return Result.fromResponse(response);
        } catch (ApiException e) {
            e.printStackTrace();
            return Result.fromException(e);
        }
    }

    @Override
    public Result<String> updateEdge(String graphName, EdgeRequest edgeRequest) {
        try {
            ApiResponse<String> response = edgeApi.updateEdgeWithHttpInfo(graphName, edgeRequest);
            return Result.fromResponse(response);
        } catch (ApiException e) {
            e.printStackTrace();
            return Result.fromException(e);
        }
    }

    @Override
    public Result<JobResponse> bulkLoading(String graphId, SchemaMapping mapping) {
        Result<SchemaMapping> result = tryUploadFile(mapping);
        if (!result.isOk()) {
            System.out.println("Failed to upload files" + result.getStatusMessage());
            return new Result<JobResponse>(result.getStatus(), null);
        }
        try {
            ApiResponse<JobResponse> response =
                    graphApi.createDataloadingJobWithHttpInfo(graphId, result.getValue());
            return Result.fromResponse(response);
        } catch (ApiException e) {
            e.printStackTrace();
            return Result.fromException(e);
        }
    }

    @Override
    public Result<CreateGraphResponse> createGraph(CreateGraphRequest graph) {
        try {
            ApiResponse<CreateGraphResponse> response = graphApi.createGraphWithHttpInfo(graph);
            return Result.fromResponse(response);
        } catch (ApiException e) {
            e.printStackTrace();
            return Result.fromException(e);
        }
    }

    @Override
    public Result<String> deleteGraph(String graphId) {
        try {
            ApiResponse<String> response = graphApi.deleteGraphWithHttpInfo(graphId);
            return Result.fromResponse(response);
        } catch (ApiException e) {
            e.printStackTrace();
            return Result.fromException(e);
        }
    }

    @Override
    public Result<GetGraphSchemaResponse> getGraphSchema(String graphId) {
        try {
            ApiResponse<GetGraphSchemaResponse> response = graphApi.getSchemaWithHttpInfo(graphId);
            return Result.fromResponse(response);
        } catch (ApiException e) {
            e.printStackTrace();
            return Result.fromException(e);
        }
    }

    @Override
    public Result<GetGraphStatisticsResponse> getGraphStatistics(String graphId) {
        try {
            ApiResponse<GetGraphStatisticsResponse> response =
                    graphApi.getGraphStatisticWithHttpInfo(graphId);
            return Result.fromResponse(response);
        } catch (ApiException e) {
            e.printStackTrace();
            return Result.fromException(e);
        }
    }

    @Override
    public Result<GetGraphResponse> getGraphMeta(String graphId) {
        try {
            ApiResponse<GetGraphResponse> response = graphApi.getGraphWithHttpInfo(graphId);
            return Result.fromResponse(response);
        } catch (ApiException e) {
            e.printStackTrace();
            return Result.fromException(e);
        }
    }

    @Override
    public Result<List<GetGraphResponse>> getAllGraphs() {
        try {
            ApiResponse<List<GetGraphResponse>> response = graphApi.listGraphsWithHttpInfo();
            return Result.fromResponse(response);
        } catch (ApiException e) {
            e.printStackTrace();
            return Result.fromException(e);
        }
    }

    @Override
    public Result<String> cancelJob(String jobId) {
        try {
            ApiResponse<String> response = jobApi.deleteJobByIdWithHttpInfo(jobId);
            return Result.fromResponse(response);
        } catch (ApiException e) {
            e.printStackTrace();
            return Result.fromException(e);
        }
    }

    @Override
    public Result<JobStatus> getJobStatus(String jobId) {
        try {
            ApiResponse<JobStatus> response = jobApi.getJobByIdWithHttpInfo(jobId);
            return Result.fromResponse(response);
        } catch (ApiException e) {
            e.printStackTrace();
            return Result.fromException(e);
        }
    }

    @Override
    public Result<List<JobStatus>> listJobs() {
        try {
            ApiResponse<List<JobStatus>> response = jobApi.listJobsWithHttpInfo();
            return Result.fromResponse(response);
        } catch (ApiException e) {
            e.printStackTrace();
            return Result.fromException(e);
        }
    }

    @Override
    public Result<CreateProcedureResponse> createProcedure(
            String graphId, CreateProcedureRequest procedure) {
        try {
            ApiResponse<CreateProcedureResponse> response =
                    procedureApi.createProcedureWithHttpInfo(graphId, procedure);
            return Result.fromResponse(response);
        } catch (ApiException e) {
            e.printStackTrace();
            return Result.fromException(e);
        }
    }

    @Override
    public Result<String> deleteProcedure(String graphId, String procedureName) {
        try {
            ApiResponse<String> response =
                    procedureApi.deleteProcedureWithHttpInfo(graphId, procedureName);
            return Result.fromResponse(response);
        } catch (ApiException e) {
            e.printStackTrace();
            return Result.fromException(e);
        }
    }

    @Override
    public Result<GetProcedureResponse> getProcedure(String graphId, String procedureId) {
        try {
            ApiResponse<GetProcedureResponse> response =
                    procedureApi.getProcedureWithHttpInfo(graphId, procedureId);
            return Result.fromResponse(response);
        } catch (ApiException e) {
            e.printStackTrace();
            return Result.fromException(e);
        }
    }

    @Override
    public Result<List<GetProcedureResponse>> listProcedures(String graphId) {
        try {
            ApiResponse<List<GetProcedureResponse>> response =
                    procedureApi.listProceduresWithHttpInfo(graphId);
            return Result.fromResponse(response);
        } catch (ApiException e) {
            e.printStackTrace();
            return Result.fromException(e);
        }
    }

    @Override
    public Result<String> updateProcedure(
            String graphId, String procedureId, UpdateProcedureRequest procedure) {
        try {
            ApiResponse<String> response =
                    procedureApi.updateProcedureWithHttpInfo(graphId, procedureId, procedure);
            return Result.fromResponse(response);
        } catch (ApiException e) {
            e.printStackTrace();
            return Result.fromException(e);
        }
    }

    @Override
    public Result<IrResult.CollectiveResults> callProcedure(
            String graphName, QueryRequest request) {
        try {
            // Interactive currently support four type of inputformat, see
            // flex/engines/graph_db/graph_db_session.h
            // Here we add byte of value 1 to denote the input format is in JSON format.
            ApiResponse<byte[]> response =
                    queryApi.procCallWithHttpInfo(
                            graphName, request.toJson().getBytes());
            if (response.getStatusCode() != 200) {
                return Result.fromException(
                        new ApiException(response.getStatusCode(), "Failed to call procedure"));
            }
            IrResult.CollectiveResults results =
                    IrResult.CollectiveResults.parseFrom(response.getData());
            return new Result<>(results);
        } catch (ApiException e) {
            e.printStackTrace();
            return Result.fromException(e);
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
            return Result.error(e.getMessage());
        }
    }

    @Override
    public Result<IrResult.CollectiveResults> callProcedure(QueryRequest request) {
        try {
            // Interactive currently support four type of inputformat, see
            // flex/engines/graph_db/graph_db_session.h
            // Here we add byte of value 1 to denote the input format is in JSON format.
            ApiResponse<byte[]> response =
                    queryApi.procCallCurrentWithHttpInfo(
                             request.toJson().getBytes());
            if (response.getStatusCode() != 200) {
                return Result.fromException(
                        new ApiException(response.getStatusCode(), "Failed to call procedure"));
            }
            IrResult.CollectiveResults results =
                    IrResult.CollectiveResults.parseFrom(response.getData());
            return new Result<>(results);
        } catch (ApiException e) {
            e.printStackTrace();
            return Result.fromException(e);
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
            return Result.error(e.getMessage());
        }
    }

    @Override
    public Result<byte[]> callProcedureRaw(String graphName, byte[] request) {
        try {
            // Interactive currently support four type of inputformat, see
            // flex/engines/graph_db/graph_db_session.h
            // Here we add byte of value 0 to denote the input format is in raw encoder/decoder
            // format.
            ApiResponse<byte[]> response =
                    queryApi.procCallWithHttpInfo(graphName, ENCODER_FORMAT_STRING, request);
            if (response.getStatusCode() != 200) {
                return Result.fromException(
                        new ApiException(response.getStatusCode(), "Failed to call procedure"));
            }
            return new Result<byte[]>(response.getData());
        } catch (ApiException e) {
            e.printStackTrace();
            return Result.fromException(e);
        }
    }

    @Override
    public Result<byte[]> callProcedureRaw(byte[] request) {
        try {
            // Interactive currently support four type of inputformat, see
            // flex/engines/graph_db/graph_db_session.h
            // Here we add byte of value 0 to denote the input format is in raw encoder/decoder
            // format.
            ApiResponse<byte[]> response =
                    queryApi.procCallCurrentWithHttpInfo(ENCODER_FORMAT_STRING, request);
            if (response.getStatusCode() != 200) {
                return Result.fromException(
                        new ApiException(response.getStatusCode(), "Failed to call procedure"));
            }
            return new Result<byte[]>(response.getData());
        } catch (ApiException e) {
            e.printStackTrace();
            return Result.fromException(e);
        }
    }

    @Override
    public Result<ServiceStatus> getServiceStatus() {
        try {
            ApiResponse<ServiceStatus> response = serviceApi.getServiceStatusWithHttpInfo();
            return Result.fromResponse(response);
        } catch (ApiException e) {
            e.printStackTrace();
            return Result.fromException(e);
        }
    }

    @Override
    public Result<String> restartService() {
        try {
            ApiResponse<String> response = serviceApi.restartServiceWithHttpInfo();
            return Result.fromResponse(response);
        } catch (ApiException e) {
            e.printStackTrace();
            return Result.fromException(e);
        }
    }

    @Override
    public Result<String> startService(StartServiceRequest service) {
        try {
            ApiResponse<String> response = serviceApi.startServiceWithHttpInfo(service);
            return Result.fromResponse(response);
        } catch (ApiException e) {
            e.printStackTrace();
            return Result.fromException(e);
        }
    }

    @Override
    public Result<String> stopService() {
        try {
            ApiResponse<String> response = serviceApi.stopServiceWithHttpInfo();
            return Result.fromResponse(response);
        } catch (ApiException e) {
            e.printStackTrace();
            return Result.fromException(e);
        }
    }

    @Override
    public Result<String> addVertex(String graphId, VertexRequest request) {
        try {
            ApiResponse<String> response = vertexApi.addVertexWithHttpInfo(graphId, request);
            return Result.fromResponse(response);
        } catch (ApiException e) {
            e.printStackTrace();
            return Result.fromException(e);
        }
    }

    @Override
    public Result<String> updateVertex(String graphId, VertexRequest request) {
        try {
            ApiResponse<String> response = vertexApi.updateVertexWithHttpInfo(graphId, request);
            return Result.fromResponse(response);
        } catch (ApiException e) {
            e.printStackTrace();
            return Result.fromException(e);
        }
    }

    @Override
    public Result<VertexData> getVertex(String graphId, String label, Object primaryKey) {
        try {
            ApiResponse<VertexData> response =
                    vertexApi.getVertexWithHttpInfo(graphId, label, primaryKey);
            return Result.fromResponse(response);
        } catch (ApiException e) {
            e.printStackTrace();
            return Result.fromException(e);
        }
    }

    @Override
    public Result<String> deleteVertex(String graphId, String label, Object primaryKey) {
        try {
            ApiResponse<String> response =
                    vertexApi.deleteVertexWithHttpInfo(graphId, label, primaryKey);
            return Result.fromResponse(response);
        } catch (ApiException e) {
            e.printStackTrace();
            return Result.fromException(e);
        }
    }

    /**
     * Closes this resource, relinquishing any underlying resources.
     * This method is invoked automatically on objects managed by the
     * {@code try}-with-resources statement.
     *
     * <p>While this interface method is declared to throw {@code
     * Exception}, implementers are <em>strongly</em> encouraged to
     * declare concrete implementations of the {@code close} method to
     * throw more specific exceptions, or to throw no exception at all
     * if the close operation cannot fail.
     *
     * <p> Cases where the close operation may fail require careful
     * attention by implementers. It is strongly advised to relinquish
     * the underlying resources and to internally <em>mark</em> the
     * resource as closed, prior to throwing the exception. The {@code
     * close} method is unlikely to be invoked more than once and so
     * this ensures that the resources are released in a timely manner.
     * Furthermore it reduces problems that could arise when the resource
     * wraps, or is wrapped, by another resource.
     *
     * <p><em>Implementers of this interface are also strongly advised
     * to not have the {@code close} method throw {@link
     * InterruptedException}.</em>
     * <p>
     * This exception interacts with a thread's interrupted status,
     * and runtime misbehavior is likely to occur if an {@code
     * InterruptedException} is {@linkplain Throwable#addSuppressed
     * suppressed}.
     * <p>
     * More generally, if it would cause problems for an
     * exception to be suppressed, the {@code AutoCloseable.close}
     * method should not throw it.
     *
     * <p>Note that unlike the {@link Closeable#close close}
     * method of {@link Closeable}, this {@code close} method
     * is <em>not</em> required to be idempotent.  In other words,
     * calling this {@code close} method more than once may have some
     * visible side effect, unlike {@code Closeable.close} which is
     * required to have no effect if called more than once.
     * <p>
     * However, implementers of this interface are strongly encouraged
     * to make their {@code close} methods idempotent.
     *
     * @throws Exception if this resource cannot be closed
     */
    @Override
    public void close() throws Exception {}

    /**
     * Upload a file to the server.
     *
     * @param fileStorage the file to upload
     *                    (required)
     * @return the response from the server
     */
    @Override
    public Result<UploadFileResponse> uploadFile(File fileStorage) {
        try {
            ApiResponse<UploadFileResponse> response = utilsApi.uploadFileWithHttpInfo(fileStorage);
            return Result.fromResponse(response);
        } catch (ApiException e) {
            e.printStackTrace();
            return Result.fromException(e);
        }
    }
}

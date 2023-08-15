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
package com.alibaba.graphscope.groot.sdk;

import com.alibaba.graphscope.groot.sdk.schema.Edge;
import com.alibaba.graphscope.groot.sdk.schema.Schema;
import com.alibaba.graphscope.groot.sdk.schema.Vertex;
import com.alibaba.graphscope.proto.groot.*;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class GrootClient {
    private final ClientGrpc.ClientBlockingStub clientStub;
    private final ClientWriteGrpc.ClientWriteBlockingStub writeStub;
    private final ClientWriteGrpc.ClientWriteStub asyncWriteStub;
    private final ClientBackupGrpc.ClientBackupBlockingStub backupStub;

    private final GrootDdlServiceGrpc.GrootDdlServiceBlockingStub ddlStub;

    private GrootClient(
            ClientGrpc.ClientBlockingStub clientBlockingStub,
            ClientWriteGrpc.ClientWriteBlockingStub clientWriteBlockingStub,
            ClientWriteGrpc.ClientWriteStub clientWriteStub,
            ClientBackupGrpc.ClientBackupBlockingStub clientBackupBlockingStub,
            GrootDdlServiceGrpc.GrootDdlServiceBlockingStub ddlServiceBlockingStub) {
        this.clientStub = clientBlockingStub;
        this.writeStub = clientWriteBlockingStub;
        this.asyncWriteStub = clientWriteStub;
        this.backupStub = clientBackupBlockingStub;
        this.ddlStub = ddlServiceBlockingStub;
    }

    public void close() {}

    public com.alibaba.graphscope.proto.GraphDefPb submitSchema(Schema schema) {
        BatchSubmitRequest request = schema.toProto();
        BatchSubmitResponse response = ddlStub.batchSubmit(request);
        return response.getGraphDef();
    }

    public com.alibaba.graphscope.proto.GraphDefPb submitSchema(Schema.Builder schema) {
        return submitSchema(schema.build());
    }

    private BatchWriteRequest.Builder getNewWriteBuilder() {
        String clientId = writeStub.getClientId(GetClientIdRequest.newBuilder().build()).getClientId();
        return BatchWriteRequest.newBuilder().setClientId(clientId);
    }



    /**
     * Block until this snapshot becomes available.
     * @param snapshotId the snapshot id to be flushed
     */
    public void remoteFlush(long snapshotId) {
        if (snapshotId == 0) {
            return;
        }
        this.writeStub.remoteFlush(
                RemoteFlushRequest.newBuilder().setSnapshotId(snapshotId).build());
    }

    /**
     * Add vertex by realtime write
     * @param vertex vertex that contains label and pk properties and other properties
     */
    public long addVertex(Vertex vertex) {
        WriteRequestPb request = vertex.toWriteRequest(WriteTypePb.INSERT);
        return submit(request);
    }

    public void addVertex(Vertex vertex, StreamObserver<BatchWriteResponse> callback) {
        WriteRequestPb request = vertex.toWriteRequest(WriteTypePb.INSERT);
        submit(request, callback);
    }

    public long addVertex(String label, Map<String, String> properties) {
        return addVertex(new Vertex(label, properties));
    }

    public long addVertices(List<Vertex> vertices) {
        List<WriteRequestPb> requests = getVertexWriteRequestPbs(vertices, WriteTypePb.INSERT);
        return submit(requests);
    }

    /**
     * Update existed vertex by realtime write
     * @param vertex vertex that contains label and pk properties and other properties
     */
    public long updateVertex(Vertex vertex) {
        WriteRequestPb request = vertex.toWriteRequest(WriteTypePb.UPDATE);
        return submit(request);
    }

    public long updateVertex(String label, Map<String, String> properties) {
        return updateVertex(new Vertex(label, properties));
    }

    public long updateVertices(List<Vertex> vertices) {
        List<WriteRequestPb> requests = getVertexWriteRequestPbs(vertices, WriteTypePb.UPDATE);
        return submit(requests);
    }

    /**
     * Delete vertex by its primary key
     * @param vertex vertex that contains label and primary key properties
     */
    public long deleteVertex(Vertex vertex) {
        WriteRequestPb request = vertex.toWriteRequest(WriteTypePb.DELETE);
        return submit(request);
    }

    public long deleteVertex(String label, Map<String, String> pk_properties) {
        return deleteVertex(new Vertex(label, pk_properties));
    }

    public long deleteVertices(List<Vertex> vertices) {
        List<WriteRequestPb> requests = getVertexWriteRequestPbs(vertices, WriteTypePb.DELETE);
        return submit(requests);
    }

    /**
     * Add edge by realtime write
     * @param edge edge that contains label, src vertex label and pk, dst label and pk, and properties
     */
    public long addEdge(Edge edge) {
        WriteRequestPb request = edge.toWriteRequest(WriteTypePb.INSERT);
        return submit(request);
    }

    public long addEdge(
            String label,
            String srcLabel,
            String dstLabel,
            Map<String, String> srcPk,
            Map<String, String> dstPk,
            Map<String, String> properties) {
        return addEdge(new Edge(label, srcLabel, dstLabel, srcPk, dstPk, properties));
    }


    public long addEdges(List<Edge> edges) {
        List<WriteRequestPb> requests = getEdgeWriteRequestPbs(edges, WriteTypePb.INSERT);
        return submit(requests);
    }

    /**
     * Update existed edge by realtime write
     * @param edge edge that contains label, src vertex label and pk, dst label and pk, and properties
     */
    public long updateEdge(Edge edge) {
        WriteRequestPb request = edge.toWriteRequest(WriteTypePb.UPDATE);
        return submit(request);
    }

    public long updateEdge(
            String label,
            String srcLabel,
            String dstLabel,
            Map<String, String> srcPk,
            Map<String, String> dstPk,
            Map<String, String> properties) {
        return updateEdge(new Edge(label, srcLabel, dstLabel, srcPk, dstPk, properties));
    }

    public long updateEdges(List<Edge> edges) {
        List<WriteRequestPb> requests = getEdgeWriteRequestPbs(edges, WriteTypePb.UPDATE);
        return submit(requests);
    }

    /**
     * Delete an edge by realtime write
     * @param edge edge that contains label, src vertex label and pk, dst label and pk, no properties required
     */
    public long deleteEdge(Edge edge) {
        WriteRequestPb request = edge.toWriteRequest(WriteTypePb.DELETE);
        return submit(request);
    }

    public long deleteEdge(
            String label,
            String srcLabel,
            String dstLabel,
            Map<String, String> srcPk,
            Map<String, String> dstPk) {
        return addEdge(new Edge(label, srcLabel, dstLabel, srcPk, dstPk));
    }

    public long deleteEdges(List<Edge> edges) {
        List<WriteRequestPb> requests = getEdgeWriteRequestPbs(edges, WriteTypePb.DELETE);
        return submit(requests);
    }

    /**
     * Commit the realtime write transaction.
     * @return The snapshot_id. The data committed would be available after a while, or you could remoteFlush(snapshot_id)
     * and wait for its return.
     */
    private long submit(WriteRequestPb request) {
        BatchWriteRequest.Builder batchWriteBuilder = getNewWriteBuilder();
        batchWriteBuilder.addWriteRequests(request);
        return writeStub.batchWrite(batchWriteBuilder.build()).getSnapshotId();
    }

    private long submit(List<WriteRequestPb> requests) {
        if (requests.isEmpty()) {
            return 0;
        }
        BatchWriteRequest.Builder batchWriteBuilder = getNewWriteBuilder();
        batchWriteBuilder.addAllWriteRequests(requests);
        return writeStub.batchWrite(batchWriteBuilder.build()).getSnapshotId();
    }

    private void submit(WriteRequestPb request, StreamObserver<BatchWriteResponse> callback) {
        BatchWriteRequest.Builder batchWriteBuilder = getNewWriteBuilder();
        batchWriteBuilder.addWriteRequests(request);
        asyncWriteStub.batchWrite(batchWriteBuilder.build(), callback);
    }

    public GraphDefPb getSchema() {
        GetSchemaResponse response =
                this.clientStub.getSchema(GetSchemaRequest.newBuilder().build());
        return response.getGraphDef();
    }

    public GraphDefPb dropSchema() {
        DropSchemaResponse response =
                this.clientStub.dropSchema(DropSchemaRequest.newBuilder().build());
        return response.getGraphDef();
    }

    public GraphDefPb prepareDataLoad(List<DataLoadTargetPb> targets) {
        PrepareDataLoadRequest.Builder builder = PrepareDataLoadRequest.newBuilder();

        for (DataLoadTargetPb target : targets) {
            builder.addDataLoadTargets(target);
        }
        PrepareDataLoadResponse response = this.clientStub.prepareDataLoad(builder.build());
        return response.getGraphDef();
    }

    public void commitDataLoad(Map<Long, DataLoadTargetPb> tableToTarget, String path) {
        CommitDataLoadRequest.Builder builder = CommitDataLoadRequest.newBuilder();
        tableToTarget.forEach(builder::putTableToTarget);
        builder.setPath(path);
        CommitDataLoadResponse response = this.clientStub.commitDataLoad(builder.build());
    }

    public String getMetrics(String roleNames) {
        GetMetricsResponse response =
                this.clientStub.getMetrics(
                        GetMetricsRequest.newBuilder().setRoleNames(roleNames).build());
        return response.getMetricsJson();
    }

    public void ingestData(String path) {
        this.clientStub.ingestData(IngestDataRequest.newBuilder().setDataPath(path).build());
    }

    public void ingestData(String path, Map<String, String> config) {
        IngestDataRequest.Builder builder = IngestDataRequest.newBuilder();
        builder.setDataPath(path);
        if (config != null) {
            builder.putAllConfig(config);
        }
        this.clientStub.ingestData(builder.build());
    }

    public String loadJsonSchema(Path jsonFile) throws IOException {
        String json = new String(Files.readAllBytes(jsonFile), StandardCharsets.UTF_8);
        return loadJsonSchema(json);
    }

    public String loadJsonSchema(String json) {
        LoadJsonSchemaResponse response =
                this.clientStub.loadJsonSchema(
                        LoadJsonSchemaRequest.newBuilder().setSchemaJson(json).build());
        return response.getGraphDef().toString();
    }

    public int getPartitionNum() {
        GetPartitionNumResponse response =
                this.clientStub.getPartitionNum(GetPartitionNumRequest.newBuilder().build());
        return response.getPartitionNum();
    }

    public int createNewGraphBackup() {
        CreateNewGraphBackupResponse response =
                this.backupStub.createNewGraphBackup(
                        CreateNewGraphBackupRequest.newBuilder().build());
        return response.getBackupId();
    }

    public void deleteGraphBackup(int backupId) {
        this.backupStub.deleteGraphBackup(
                DeleteGraphBackupRequest.newBuilder().setBackupId(backupId).build());
    }

    public void purgeOldGraphBackups(int keepAliveNumber) {
        this.backupStub.purgeOldGraphBackups(
                PurgeOldGraphBackupsRequest.newBuilder()
                        .setKeepAliveNumber(keepAliveNumber)
                        .build());
    }

    public void restoreFromGraphBackup(
            int backupId, String metaRestorePath, String storeRestorePath) {
        this.backupStub.restoreFromGraphBackup(
                RestoreFromGraphBackupRequest.newBuilder()
                        .setBackupId(backupId)
                        .setMetaRestorePath(metaRestorePath)
                        .setStoreRestorePath(storeRestorePath)
                        .build());
    }

    public boolean verifyGraphBackup(int backupId) {
        VerifyGraphBackupResponse response =
                this.backupStub.verifyGraphBackup(
                        VerifyGraphBackupRequest.newBuilder().setBackupId(backupId).build());
        boolean suc = response.getIsOk();
        if (!suc) {
            System.err.println("verify backup [" + backupId + "] failed, " + response.getErrMsg());
        }
        return suc;
    }

    public List<BackupInfoPb> getGraphBackupInfo() {
        GetGraphBackupInfoResponse response =
                this.backupStub.getGraphBackupInfo(GetGraphBackupInfoRequest.newBuilder().build());
        return new ArrayList<>(response.getBackupInfoListList());
    }

    public void clearIngest(String dataPath) {
        this.clientStub.clearIngest(ClearIngestRequest.newBuilder().setDataPath(dataPath).build());
    }

    public static GrootClientBuilder newBuilder() {
        return new GrootClientBuilder();
    }

    public static class GrootClientBuilder {
        private List<SocketAddress> addrs;
        private String username;
        private String password;

        private GrootClientBuilder() {
            this.addrs = new ArrayList<>();
        }

        public GrootClientBuilder addAddress(SocketAddress address) {
            this.addrs.add(address);
            return this;
        }

        public GrootClientBuilder addHost(String host, int port) {
            return this.addAddress(new InetSocketAddress(host, port));
        }

        public GrootClientBuilder setHosts(String hosts) {
            List<SocketAddress> addresses = new ArrayList<>();
            for (String host : hosts.split(",")) {
                String[] items = host.split(":");
                addresses.add(new InetSocketAddress(items[0], Integer.valueOf(items[1])));
            }
            this.addrs = addresses;
            return this;
        }

        public GrootClientBuilder setUsername(String username) {
            this.username = username;
            return this;
        }

        public GrootClientBuilder setPassword(String password) {
            this.password = password;
            return this;
        }

        public GrootClient build() {
            MultiAddrResovlerFactory multiAddrResovlerFactory =
                    new MultiAddrResovlerFactory(this.addrs);
            ManagedChannel channel =
                    ManagedChannelBuilder.forTarget("hosts")
                            .nameResolverFactory(multiAddrResovlerFactory)
                            .defaultLoadBalancingPolicy("round_robin")
                            .usePlaintext()
                            .build();
            ClientGrpc.ClientBlockingStub clientBlockingStub = ClientGrpc.newBlockingStub(channel);
            ClientWriteGrpc.ClientWriteBlockingStub clientWriteBlockingStub =
                    ClientWriteGrpc.newBlockingStub(channel);
            ClientWriteGrpc.ClientWriteStub clientWriteStub = ClientWriteGrpc.newStub(channel);
            ClientBackupGrpc.ClientBackupBlockingStub clientBackupBlockingStub =
                    ClientBackupGrpc.newBlockingStub(channel);
            GrootDdlServiceGrpc.GrootDdlServiceBlockingStub ddlServiceBlockingStub =
                    GrootDdlServiceGrpc.newBlockingStub(channel);
            if (username != null && password != null) {
                BasicAuth basicAuth = new BasicAuth(username, password);
                clientBlockingStub = clientBlockingStub.withCallCredentials(basicAuth);
                clientWriteBlockingStub = clientWriteBlockingStub.withCallCredentials(basicAuth);
                clientWriteStub = clientWriteStub.withCallCredentials(basicAuth);
                clientBackupBlockingStub = clientBackupBlockingStub.withCallCredentials(basicAuth);
                ddlServiceBlockingStub = ddlServiceBlockingStub.withCallCredentials(basicAuth);
            }
            return new GrootClient(
                    clientBlockingStub,
                    clientWriteBlockingStub,
                    clientWriteStub,
                    clientBackupBlockingStub,
                    ddlServiceBlockingStub);
        }
    }


    private List<WriteRequestPb> getVertexWriteRequestPbs(List<Vertex> vertices, WriteTypePb writeType) {
        return vertices.stream().map(element -> element.toWriteRequest(writeType)).collect(Collectors.toList());
    }

    private List<WriteRequestPb> getEdgeWriteRequestPbs(List<Edge> edges, WriteTypePb writeType) {
        return edges.stream().map(element -> element.toWriteRequest(writeType)).collect(Collectors.toList());
    }
}

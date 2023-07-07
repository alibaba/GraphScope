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

import com.alibaba.graphscope.proto.DataLoadTargetPb;
import com.alibaba.graphscope.proto.groot.*;
import com.alibaba.graphscope.proto.write.*;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class GrootClient {
    private final ClientGrpc.ClientBlockingStub stub;
    private final ClientWriteGrpc.ClientWriteBlockingStub writeStub;
    private final ClientBackupGrpc.ClientBackupBlockingStub backupStub;
    private String clientId = "DEFAULT";

    private BatchWriteRequest.Builder batchWriteBuilder;

    private GrootClient(
            ClientGrpc.ClientBlockingStub clientBlockingStub,
            ClientWriteGrpc.ClientWriteBlockingStub clientWriteBlockingStub,
            ClientBackupGrpc.ClientBackupBlockingStub clientBackupBlockingStub) {
        this.stub = clientBlockingStub;
        this.writeStub = clientWriteBlockingStub;
        this.backupStub = clientBackupBlockingStub;
        this.reset();
    }

    public void close() {}

    private void reset() {
        this.batchWriteBuilder = BatchWriteRequest.newBuilder().setClientId(this.clientId);
    }

    public void initWriteSession() {
        this.clientId =
                this.writeStub.getClientId(GetClientIdRequest.newBuilder().build()).getClientId();
        this.reset();
    }

    private VertexRecordKeyPb getVertexRecordKeyPb(String label, Map<String, String> properties) {
        VertexRecordKeyPb.Builder builder = VertexRecordKeyPb.newBuilder().setLabel(label);
        if (properties != null) {
            builder.putAllPkProperties(properties);
        }
        return builder.build();
    }

    private EdgeRecordKeyPb getEdgeRecordKeyPb(
            String label, VertexRecordKeyPb src, VertexRecordKeyPb dst) {
        return EdgeRecordKeyPb.newBuilder()
                .setLabel(label)
                .setSrcVertexKey(src)
                .setDstVertexKey(dst)
                .build();
    }

    private DataRecordPb getDataRecordPb(VertexRecordKeyPb key, Map<String, String> properties) {
        DataRecordPb.Builder builder = DataRecordPb.newBuilder().setVertexRecordKey(key);
        if (properties != null) {
            builder.putAllProperties(properties);
        }
        return builder.build();
    }

    private DataRecordPb getDataRecordPb(EdgeRecordKeyPb key, Map<String, String> properties) {
        DataRecordPb.Builder builder = DataRecordPb.newBuilder().setEdgeRecordKey(key);
        if (properties != null) {
            builder.putAllProperties(properties);
        }
        return builder.build();
    }

    private DataRecordPb getVertexDataRecord(String label, Map<String, String> properties) {
        VertexRecordKeyPb vertexRecordKey = getVertexRecordKeyPb(label, null);
        return getDataRecordPb(vertexRecordKey, properties);
    }

    private DataRecordPb getEdgeDataRecord(
            String label,
            String srcLabel,
            String dstLabel,
            Map<String, String> srcPk,
            Map<String, String> dstPk,
            Map<String, String> properties) {
        VertexRecordKeyPb src = getVertexRecordKeyPb(srcLabel, srcPk);
        VertexRecordKeyPb dst = getVertexRecordKeyPb(dstLabel, dstPk);
        EdgeRecordKeyPb edgeRecordKeyPb = getEdgeRecordKeyPb(label, src, dst);
        return getDataRecordPb(edgeRecordKeyPb, properties);
    }

    private WriteRequestPb getWriteRequestPb(DataRecordPb record, WriteTypePb writeType) {
        return WriteRequestPb.newBuilder().setWriteType(writeType).setDataRecord(record).build();
    }

    /**
     * Add vertex by realtime write
     * @param label vertex label
     * @param properties properties, including the primary key
     */
    public void addVertex(String label, Map<String, String> properties) {
        DataRecordPb record = getVertexDataRecord(label, properties);
        WriteRequestPb request = getWriteRequestPb(record, WriteTypePb.INSERT);
        this.batchWriteBuilder.addWriteRequests(request);
    }

    /**
     * Update existed vertex by realtime write
     * @param label vertex label
     * @param properties properties, including the primary key
     */
    public void updateVertex(String label, Map<String, String> properties) {
        DataRecordPb record = getVertexDataRecord(label, properties);
        WriteRequestPb request = getWriteRequestPb(record, WriteTypePb.UPDATE);
        this.batchWriteBuilder.addWriteRequests(request);
    }

    /**
     * Delete vertex by its primary key
     * @param label vertex label
     * @param properties properties, contains only the primary key
     */
    public void deleteVertex(String label, Map<String, String> properties) {
        DataRecordPb record = getVertexDataRecord(label, properties);
        WriteRequestPb request = getWriteRequestPb(record, WriteTypePb.DELETE);
        this.batchWriteBuilder.addWriteRequests(request);
    }

    /**
     * Add edge by realtime write
     * @param label edge label
     * @param srcLabel source vertex label
     * @param dstLabel destination vertex label
     * @param srcPk source primary keys
     * @param dstPk destination primary keys
     * @param properties edge properties
     */
    public void addEdge(
            String label,
            String srcLabel,
            String dstLabel,
            Map<String, String> srcPk,
            Map<String, String> dstPk,
            Map<String, String> properties) {
        DataRecordPb record =
                getEdgeDataRecord(label, srcLabel, dstLabel, srcPk, dstPk, properties);
        WriteRequestPb request = getWriteRequestPb(record, WriteTypePb.INSERT);
        this.batchWriteBuilder.addWriteRequests(request);
    }

    /**
     * Update existed edge by realtime write
     * @param label edge label
     * @param srcLabel source vertex label
     * @param dstLabel destination vertex label
     * @param srcPk source primary keys
     * @param dstPk destination primary keys
     * @param properties edge properties
     */
    public void updateEdge(
            String label,
            String srcLabel,
            String dstLabel,
            Map<String, String> srcPk,
            Map<String, String> dstPk,
            Map<String, String> properties) {
        DataRecordPb record =
                getEdgeDataRecord(label, srcLabel, dstLabel, srcPk, dstPk, properties);
        WriteRequestPb request = getWriteRequestPb(record, WriteTypePb.INSERT);
        this.batchWriteBuilder.addWriteRequests(request);
    }

    /**
     * Delete an edge by realtime write
     * @param label edge label
     * @param srcLabel source vertex label
     * @param dstLabel destination vertex label
     * @param srcPk source primary keys
     * @param dstPk destination primary keys
     */
    public void deleteEdge(
            String label,
            String srcLabel,
            String dstLabel,
            Map<String, String> srcPk,
            Map<String, String> dstPk) {
        DataRecordPb record = getEdgeDataRecord(label, srcLabel, dstLabel, srcPk, dstPk, null);
        WriteRequestPb request = getWriteRequestPb(record, WriteTypePb.INSERT);
        this.batchWriteBuilder.addWriteRequests(request);
    }

    /**
     * Commit the realtime write transaction.
     * @return The snapshot_id. The data committed would be available after a while, or you could remoteFlush(snapshot_id)
     * and wait for its return.
     */
    public long commit() {
        long snapshotId = 0L;
        if (this.batchWriteBuilder.getWriteRequestsCount() > 0) {
            BatchWriteResponse response = this.writeStub.batchWrite(this.batchWriteBuilder.build());
            snapshotId = response.getSnapshotId();
        }
        this.reset();
        return snapshotId;
    }

    /**
     * Block until this snapshot becomes available.
     * @param snapshotId the snapshot id to be flushed
     */
    public void remoteFlush(long snapshotId) {
        this.writeStub.remoteFlush(
                RemoteFlushRequest.newBuilder().setSnapshotId(snapshotId).build());
    }

    public GraphDefPb getSchema() {
        GetSchemaResponse response = this.stub.getSchema(GetSchemaRequest.newBuilder().build());
        return response.getGraphDef();
    }

    public GraphDefPb dropSchema() {
        DropSchemaResponse response = this.stub.dropSchema(DropSchemaRequest.newBuilder().build());
        return response.getGraphDef();
    }

    public GraphDefPb prepareDataLoad(List<DataLoadTargetPb> targets) {
        PrepareDataLoadRequest.Builder builder = PrepareDataLoadRequest.newBuilder();
        for (DataLoadTargetPb target : targets) {
            builder.addDataLoadTargets(target);
        }
        PrepareDataLoadResponse response = this.stub.prepareDataLoad(builder.build());
        return response.getGraphDef();
    }

    public void commitDataLoad(Map<Long, DataLoadTargetPb> tableToTarget, String path) {
        CommitDataLoadRequest.Builder builder = CommitDataLoadRequest.newBuilder();
        tableToTarget.forEach(builder::putTableToTarget);
        builder.setPath(path);
        CommitDataLoadResponse response = this.stub.commitDataLoad(builder.build());
    }

    public String getMetrics(String roleNames) {
        GetMetricsResponse response =
                this.stub.getMetrics(
                        GetMetricsRequest.newBuilder().setRoleNames(roleNames).build());
        return response.getMetricsJson();
    }

    public void ingestData(String path) {
        this.stub.ingestData(IngestDataRequest.newBuilder().setDataPath(path).build());
    }

    public void ingestData(String path, Map<String, String> config) {
        IngestDataRequest.Builder builder = IngestDataRequest.newBuilder();
        builder.setDataPath(path);
        if (config != null) {
            builder.putAllConfig(config);
        }
        this.stub.ingestData(builder.build());
    }

    public String loadJsonSchema(Path jsonFile) throws IOException {
        String json = new String(Files.readAllBytes(jsonFile), StandardCharsets.UTF_8);
        return loadJsonSchema(json);
    }

    public String loadJsonSchema(String json) {
        LoadJsonSchemaResponse response =
                this.stub.loadJsonSchema(
                        LoadJsonSchemaRequest.newBuilder().setSchemaJson(json).build());
        return response.getGraphDef().toString();
    }

    public int getPartitionNum() {
        GetPartitionNumResponse response =
                this.stub.getPartitionNum(GetPartitionNumRequest.newBuilder().build());
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
        this.stub.clearIngest(ClearIngestRequest.newBuilder().setDataPath(dataPath).build());
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
            ClientBackupGrpc.ClientBackupBlockingStub clientBackupBlockingStub =
                    ClientBackupGrpc.newBlockingStub(channel);
            if (username != null && password != null) {
                BasicAuth basicAuth = new BasicAuth(username, password);
                clientBlockingStub = clientBlockingStub.withCallCredentials(basicAuth);
                clientWriteBlockingStub = clientWriteBlockingStub.withCallCredentials(basicAuth);
                clientBackupBlockingStub = clientBackupBlockingStub.withCallCredentials(basicAuth);
            }
            return new GrootClient(
                    clientBlockingStub, clientWriteBlockingStub, clientBackupBlockingStub);
        }
    }
}

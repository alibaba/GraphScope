/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.v2.sdk;

import com.alibaba.graphscope.proto.write.*;
import com.alibaba.maxgraph.proto.v2.*;
import com.alibaba.maxgraph.proto.v2.RemoteFlushRequest;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.common.schema.GraphDef;
import com.alibaba.maxgraph.v2.frontend.write.EdgeRecordKey;
import com.alibaba.maxgraph.v2.frontend.write.VertexRecordKey;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Client implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(Client.class);

    private ClientGrpc.ClientBlockingStub stub;
    private ClientWriteGrpc.ClientWriteBlockingStub writeStub;
    private ManagedChannel channel;
    private String name = "";

    private BatchWriteRequest.Builder batchWriteBuilder;

    public Client(String hosts, String name) {
        this.name = name;
        List<SocketAddress> addrList = new ArrayList<>();
        for (String host : hosts.split(",")) {
            String[] items = host.split(":");
            addrList.add(new InetSocketAddress(items[0], Integer.valueOf(items[1])));
        }
        MultiAddrResovlerFactory resovlerFactory = new MultiAddrResovlerFactory(addrList);
        ManagedChannel channel = ManagedChannelBuilder.forTarget("hosts")
                .nameResolverFactory(resovlerFactory)
                .defaultLoadBalancingPolicy("round_robin")
                .usePlaintext()
                .build();
        this.channel = channel;
        this.stub = ClientGrpc.newBlockingStub(this.channel);
        this.writeStub = ClientWriteGrpc.newBlockingStub(this.channel);
        this.init();
    }

    public Client(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build());
    }

    public Client(ManagedChannel channel) {
        this.channel = channel;
        this.stub = ClientGrpc.newBlockingStub(this.channel);
        this.writeStub = ClientWriteGrpc.newBlockingStub(this.channel);
        this.init();
    }

    private void init() {
        this.batchWriteBuilder = BatchWriteRequest.newBuilder().setClientId("0-1");
    }

    public void addVertex(String label, Map<String, String> properties) {
        VertexRecordKey vertexRecordKey = new VertexRecordKey(label);
        WriteRequestPb writeRequest = WriteRequestPb.newBuilder()
                .setWriteType(WriteTypePb.INSERT)
                .setDataRecord(DataRecordPb.newBuilder()
                        .setVertexRecordKey(vertexRecordKey.toProto())
                        .putAllProperties(properties)
                        .build())
                .build();
        this.batchWriteBuilder.addWriteRequests(writeRequest);
    }

    public void addEdge(String label, String srcLabel, String dstLabel, Map<String, String> srcPk,
                        Map<String, String> dstPk, Map<String, String> properties) {
        VertexRecordKey srcVertexKey = new VertexRecordKey(srcLabel, Collections.unmodifiableMap(srcPk));
        VertexRecordKey dstVertexKey = new VertexRecordKey(dstLabel, Collections.unmodifiableMap(dstPk));
        EdgeRecordKey edgeRecordKey = new EdgeRecordKey(label, srcVertexKey, dstVertexKey);
        WriteRequestPb writeRequest = WriteRequestPb.newBuilder()
                .setWriteType(WriteTypePb.INSERT)
                .setDataRecord(DataRecordPb.newBuilder()
                        .setEdgeRecordKey(edgeRecordKey.toProto())
                        .putAllProperties(properties)
                        .build())
                .build();
        this.batchWriteBuilder.addWriteRequests(writeRequest);
    }

    public long commit() {
        long snapshotId = 0L;
        if (this.batchWriteBuilder.getWriteRequestsCount() > 0) {
            BatchWriteResponse response = this.writeStub.batchWrite(this.batchWriteBuilder.build());
            snapshotId = response.getSnapshotId();
        }
        this.init();
        return snapshotId;
    }

    public void remoteFlush(long snapshotId) {
        this.stub.remoteFlush(RemoteFlushRequest.newBuilder().setSnapshotId(snapshotId).build());
    }

    public GraphSchema getSchema() {
        GetSchemaResponse response = this.stub.getSchema(GetSchemaRequest.newBuilder().build());
        return GraphDef.parseProto(response.getGraphDef());
    }

    public GraphSchema dropSchema() {
        DropSchemaResponse response = this.stub.dropSchema(DropSchemaRequest.newBuilder().build());
        return GraphDef.parseProto(response.getGraphDef());
    }

    public GraphSchema prepareDataLoad(List<DataLoadTarget> targets) {
        PrepareDataLoadRequest.Builder builder = PrepareDataLoadRequest.newBuilder();
        for (DataLoadTarget target : targets) {
            builder.addDataLoadTargets(target.toProto());
        }
        PrepareDataLoadResponse response = this.stub.prepareDataLoad(builder.build());
        return GraphDef.parseProto(response.getGraphDef());
    }

    public void commitDataLoad(Map<Long, DataLoadTarget> tableToTarget) {
        CommitDataLoadRequest.Builder builder = CommitDataLoadRequest.newBuilder();
        tableToTarget.forEach((tableId, target) -> {
            builder.putTableToTarget(tableId, target.toProto());
        });
        CommitDataLoadResponse response = this.stub.commitDataLoad(builder.build());

    }

    public String getMetrics(String roleNames) {
        GetMetricsResponse response =
                this.stub.getMetrics(GetMetricsRequest.newBuilder().setRoleNames(roleNames).build());
        return response.getMetricsJson();
    }

    public void ingestData(String path) {
        this.stub.ingestData(IngestDataRequest.newBuilder().setDataPath(path).build());
    }

    public String loadJsonSchema(Path jsonFile) throws IOException {
        String json = new String(Files.readAllBytes(jsonFile), StandardCharsets.UTF_8);
        LoadJsonSchemaResponse response = this.stub.loadJsonSchema(LoadJsonSchemaRequest.newBuilder()
                .setSchemaJson(json).build());
        return response.getGraphDef().toString();
    }

    public int getPartitionNum() {
        GetPartitionNumResponse response = this.stub.getPartitionNum(GetPartitionNumRequest.newBuilder().build());
        return response.getPartitionNum();
    }

    @Override
    public void close() {
        this.channel.shutdown();
        try {
            this.channel.awaitTermination(3000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            // Ignore
        }
    }
}

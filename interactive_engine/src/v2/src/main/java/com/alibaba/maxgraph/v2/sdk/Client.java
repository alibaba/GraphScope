package com.alibaba.maxgraph.v2.sdk;

import com.alibaba.maxgraph.proto.v2.*;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.common.schema.GraphDef;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Client implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(Client.class);

    private ClientGrpc.ClientBlockingStub stub;
    private ManagedChannel channel;
    private String name = "";

    private AddVerticesRequest.Builder verticesBuilder;
    private AddEdgesRequest.Builder edgesBuilder;

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
        this.init();
    }

    private void init() {
        this.verticesBuilder = AddVerticesRequest.newBuilder().setSession(this.name);
        this.edgesBuilder = AddEdgesRequest.newBuilder().setSession(this.name);
    }

    public void addVertex(String label, Map<String, String> properties) {
        this.verticesBuilder.addDataList(VertexDataPb.newBuilder()
                .setLabel(label)
                .putAllProperties(properties)
                .build());
    }

    public void addEdge(String label, String srcLabel, String dstLabel, Map<String, String> srcPk,
                        Map<String, String> dstPk, Map<String, String> properties) {
        this.edgesBuilder.addDataList(EdgeDataPb.newBuilder()
                .setLabel(label)
                .setSrcLabel(srcLabel)
                .setDstLabel(dstLabel)
                .putAllSrcPk(srcPk)
                .putAllDstPk(dstPk)
                .putAllProperties(properties)
                .build());
    }

    public long commit() {
        long snapshotId = 0L;
        if (this.verticesBuilder.getDataListCount() > 0) {
            AddVerticesResponse verticesResponse = this.stub.addVertices(this.verticesBuilder.build());
            snapshotId = verticesResponse.getSnapshotId();
        }
        if (this.edgesBuilder.getDataListCount() > 0) {
            AddEdgesResponse edgesResponse = this.stub.addEdges(this.edgesBuilder.build());
            snapshotId = edgesResponse.getSnapshotId();
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

    public String getMetrics(String roleNames) {
        GetMetricsResponse response =
                this.stub.getMetrics(GetMetricsRequest.newBuilder().setRoleNames(roleNames).build());
        return response.getMetricsJson();
    }

    public void ingestData(String path) {
        this.stub.ingestData(IngestDataRequest.newBuilder().setDataPath(path).build());
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

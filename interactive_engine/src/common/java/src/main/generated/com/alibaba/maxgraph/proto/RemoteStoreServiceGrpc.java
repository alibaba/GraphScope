package com.alibaba.maxgraph.proto;

import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.asyncClientStreamingCall;
import static io.grpc.stub.ClientCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncClientStreamingCall;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall;

/**
 * <pre>
 * The store service definition.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.0.0)",
    comments = "Source: remote_api.proto")
public class RemoteStoreServiceGrpc {

  private RemoteStoreServiceGrpc() {}

  public static final String SERVICE_NAME = "tinkerpop.RemoteStoreService";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.RemoteApi.BatchVerticesEdgesRequest,
      com.alibaba.maxgraph.proto.RemoteApi.BatchVerticesEdgesResponse> METHOD_GET_BATCH_OUT =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING,
          generateFullMethodName(
              "tinkerpop.RemoteStoreService", "getBatchOut"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.RemoteApi.BatchVerticesEdgesRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.RemoteApi.BatchVerticesEdgesResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.RemoteApi.BatchVerticesEdgesRequest,
      com.alibaba.maxgraph.proto.RemoteApi.BatchVerticesEdgesResponse> METHOD_GET_BATCH_IN =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING,
          generateFullMethodName(
              "tinkerpop.RemoteStoreService", "getBatchIn"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.RemoteApi.BatchVerticesEdgesRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.RemoteApi.BatchVerticesEdgesResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.RemoteApi.BatchVerticesEdgesRequest,
      com.alibaba.maxgraph.proto.RemoteApi.BatchVerticesCountResponse> METHOD_GET_BATCH_OUT_CNT =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING,
          generateFullMethodName(
              "tinkerpop.RemoteStoreService", "getBatchOutCnt"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.RemoteApi.BatchVerticesEdgesRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.RemoteApi.BatchVerticesCountResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.RemoteApi.BatchVerticesEdgesRequest,
      com.alibaba.maxgraph.proto.RemoteApi.BatchVerticesCountResponse> METHOD_GET_BATCH_IN_CNT =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING,
          generateFullMethodName(
              "tinkerpop.RemoteStoreService", "getBatchInCnt"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.RemoteApi.BatchVerticesEdgesRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.RemoteApi.BatchVerticesCountResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.RemoteApi.VerticesRequest,
      com.alibaba.maxgraph.proto.GremlinQuery.VertexResponse> METHOD_GET_VERTICES =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING,
          generateFullMethodName(
              "tinkerpop.RemoteStoreService", "getVertices"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.RemoteApi.VerticesRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.GremlinQuery.VertexResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.RemoteApi.GraphEdgesRequest,
      com.alibaba.maxgraph.proto.StoreApi.GraphEdgeReponse> METHOD_GET_GRAPH_EDGES =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING,
          generateFullMethodName(
              "tinkerpop.RemoteStoreService", "getGraphEdges"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.RemoteApi.GraphEdgesRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.StoreApi.GraphEdgeReponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.RemoteApi.QueryRequest,
      com.alibaba.maxgraph.proto.GremlinQuery.VertexResponse> METHOD_QUERY_VERTICES =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING,
          generateFullMethodName(
              "tinkerpop.RemoteStoreService", "query_vertices"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.RemoteApi.QueryRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.GremlinQuery.VertexResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.RemoteApi.QueryRequest,
      com.alibaba.maxgraph.proto.StoreApi.GraphEdgeReponse> METHOD_QUERY_EDGES =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING,
          generateFullMethodName(
              "tinkerpop.RemoteStoreService", "query_edges"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.RemoteApi.QueryRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.StoreApi.GraphEdgeReponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.RemoteApi.QueryCountRequest,
      com.alibaba.maxgraph.proto.RemoteApi.CountResponse> METHOD_VERTEX_COUNT =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "tinkerpop.RemoteStoreService", "vertex_count"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.RemoteApi.QueryCountRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.RemoteApi.CountResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.RemoteApi.QueryCountRequest,
      com.alibaba.maxgraph.proto.RemoteApi.CountResponse> METHOD_EDGE_COUNT =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "tinkerpop.RemoteStoreService", "edge_count"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.RemoteApi.QueryCountRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.RemoteApi.CountResponse.getDefaultInstance()));

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static RemoteStoreServiceStub newStub(io.grpc.Channel channel) {
    return new RemoteStoreServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static RemoteStoreServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new RemoteStoreServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary and streaming output calls on the service
   */
  public static RemoteStoreServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new RemoteStoreServiceFutureStub(channel);
  }

  /**
   * <pre>
   * The store service definition.
   * </pre>
   */
  public static abstract class RemoteStoreServiceImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     * batch requests
     * </pre>
     */
    public void getBatchOut(com.alibaba.maxgraph.proto.RemoteApi.BatchVerticesEdgesRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.RemoteApi.BatchVerticesEdgesResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_BATCH_OUT, responseObserver);
    }

    /**
     */
    public void getBatchIn(com.alibaba.maxgraph.proto.RemoteApi.BatchVerticesEdgesRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.RemoteApi.BatchVerticesEdgesResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_BATCH_IN, responseObserver);
    }

    /**
     */
    public void getBatchOutCnt(com.alibaba.maxgraph.proto.RemoteApi.BatchVerticesEdgesRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.RemoteApi.BatchVerticesCountResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_BATCH_OUT_CNT, responseObserver);
    }

    /**
     */
    public void getBatchInCnt(com.alibaba.maxgraph.proto.RemoteApi.BatchVerticesEdgesRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.RemoteApi.BatchVerticesCountResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_BATCH_IN_CNT, responseObserver);
    }

    /**
     */
    public void getVertices(com.alibaba.maxgraph.proto.RemoteApi.VerticesRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.GremlinQuery.VertexResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_VERTICES, responseObserver);
    }

    /**
     */
    public void getGraphEdges(com.alibaba.maxgraph.proto.RemoteApi.GraphEdgesRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.StoreApi.GraphEdgeReponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_GRAPH_EDGES, responseObserver);
    }

    /**
     */
    public void queryVertices(com.alibaba.maxgraph.proto.RemoteApi.QueryRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.GremlinQuery.VertexResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_QUERY_VERTICES, responseObserver);
    }

    /**
     */
    public void queryEdges(com.alibaba.maxgraph.proto.RemoteApi.QueryRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.StoreApi.GraphEdgeReponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_QUERY_EDGES, responseObserver);
    }

    /**
     */
    public void vertexCount(com.alibaba.maxgraph.proto.RemoteApi.QueryCountRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.RemoteApi.CountResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_VERTEX_COUNT, responseObserver);
    }

    /**
     */
    public void edgeCount(com.alibaba.maxgraph.proto.RemoteApi.QueryCountRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.RemoteApi.CountResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_EDGE_COUNT, responseObserver);
    }

    @java.lang.Override public io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            METHOD_GET_BATCH_OUT,
            asyncServerStreamingCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.RemoteApi.BatchVerticesEdgesRequest,
                com.alibaba.maxgraph.proto.RemoteApi.BatchVerticesEdgesResponse>(
                  this, METHODID_GET_BATCH_OUT)))
          .addMethod(
            METHOD_GET_BATCH_IN,
            asyncServerStreamingCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.RemoteApi.BatchVerticesEdgesRequest,
                com.alibaba.maxgraph.proto.RemoteApi.BatchVerticesEdgesResponse>(
                  this, METHODID_GET_BATCH_IN)))
          .addMethod(
            METHOD_GET_BATCH_OUT_CNT,
            asyncServerStreamingCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.RemoteApi.BatchVerticesEdgesRequest,
                com.alibaba.maxgraph.proto.RemoteApi.BatchVerticesCountResponse>(
                  this, METHODID_GET_BATCH_OUT_CNT)))
          .addMethod(
            METHOD_GET_BATCH_IN_CNT,
            asyncServerStreamingCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.RemoteApi.BatchVerticesEdgesRequest,
                com.alibaba.maxgraph.proto.RemoteApi.BatchVerticesCountResponse>(
                  this, METHODID_GET_BATCH_IN_CNT)))
          .addMethod(
            METHOD_GET_VERTICES,
            asyncServerStreamingCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.RemoteApi.VerticesRequest,
                com.alibaba.maxgraph.proto.GremlinQuery.VertexResponse>(
                  this, METHODID_GET_VERTICES)))
          .addMethod(
            METHOD_GET_GRAPH_EDGES,
            asyncServerStreamingCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.RemoteApi.GraphEdgesRequest,
                com.alibaba.maxgraph.proto.StoreApi.GraphEdgeReponse>(
                  this, METHODID_GET_GRAPH_EDGES)))
          .addMethod(
            METHOD_QUERY_VERTICES,
            asyncServerStreamingCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.RemoteApi.QueryRequest,
                com.alibaba.maxgraph.proto.GremlinQuery.VertexResponse>(
                  this, METHODID_QUERY_VERTICES)))
          .addMethod(
            METHOD_QUERY_EDGES,
            asyncServerStreamingCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.RemoteApi.QueryRequest,
                com.alibaba.maxgraph.proto.StoreApi.GraphEdgeReponse>(
                  this, METHODID_QUERY_EDGES)))
          .addMethod(
            METHOD_VERTEX_COUNT,
            asyncUnaryCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.RemoteApi.QueryCountRequest,
                com.alibaba.maxgraph.proto.RemoteApi.CountResponse>(
                  this, METHODID_VERTEX_COUNT)))
          .addMethod(
            METHOD_EDGE_COUNT,
            asyncUnaryCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.RemoteApi.QueryCountRequest,
                com.alibaba.maxgraph.proto.RemoteApi.CountResponse>(
                  this, METHODID_EDGE_COUNT)))
          .build();
    }
  }

  /**
   * <pre>
   * The store service definition.
   * </pre>
   */
  public static final class RemoteStoreServiceStub extends io.grpc.stub.AbstractStub<RemoteStoreServiceStub> {
    private RemoteStoreServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private RemoteStoreServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RemoteStoreServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new RemoteStoreServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * batch requests
     * </pre>
     */
    public void getBatchOut(com.alibaba.maxgraph.proto.RemoteApi.BatchVerticesEdgesRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.RemoteApi.BatchVerticesEdgesResponse> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(METHOD_GET_BATCH_OUT, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getBatchIn(com.alibaba.maxgraph.proto.RemoteApi.BatchVerticesEdgesRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.RemoteApi.BatchVerticesEdgesResponse> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(METHOD_GET_BATCH_IN, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getBatchOutCnt(com.alibaba.maxgraph.proto.RemoteApi.BatchVerticesEdgesRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.RemoteApi.BatchVerticesCountResponse> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(METHOD_GET_BATCH_OUT_CNT, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getBatchInCnt(com.alibaba.maxgraph.proto.RemoteApi.BatchVerticesEdgesRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.RemoteApi.BatchVerticesCountResponse> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(METHOD_GET_BATCH_IN_CNT, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getVertices(com.alibaba.maxgraph.proto.RemoteApi.VerticesRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.GremlinQuery.VertexResponse> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(METHOD_GET_VERTICES, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getGraphEdges(com.alibaba.maxgraph.proto.RemoteApi.GraphEdgesRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.StoreApi.GraphEdgeReponse> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(METHOD_GET_GRAPH_EDGES, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void queryVertices(com.alibaba.maxgraph.proto.RemoteApi.QueryRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.GremlinQuery.VertexResponse> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(METHOD_QUERY_VERTICES, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void queryEdges(com.alibaba.maxgraph.proto.RemoteApi.QueryRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.StoreApi.GraphEdgeReponse> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(METHOD_QUERY_EDGES, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void vertexCount(com.alibaba.maxgraph.proto.RemoteApi.QueryCountRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.RemoteApi.CountResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_VERTEX_COUNT, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void edgeCount(com.alibaba.maxgraph.proto.RemoteApi.QueryCountRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.RemoteApi.CountResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_EDGE_COUNT, getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   * The store service definition.
   * </pre>
   */
  public static final class RemoteStoreServiceBlockingStub extends io.grpc.stub.AbstractStub<RemoteStoreServiceBlockingStub> {
    private RemoteStoreServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private RemoteStoreServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RemoteStoreServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new RemoteStoreServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * batch requests
     * </pre>
     */
    public java.util.Iterator<com.alibaba.maxgraph.proto.RemoteApi.BatchVerticesEdgesResponse> getBatchOut(
        com.alibaba.maxgraph.proto.RemoteApi.BatchVerticesEdgesRequest request) {
      return blockingServerStreamingCall(
          getChannel(), METHOD_GET_BATCH_OUT, getCallOptions(), request);
    }

    /**
     */
    public java.util.Iterator<com.alibaba.maxgraph.proto.RemoteApi.BatchVerticesEdgesResponse> getBatchIn(
        com.alibaba.maxgraph.proto.RemoteApi.BatchVerticesEdgesRequest request) {
      return blockingServerStreamingCall(
          getChannel(), METHOD_GET_BATCH_IN, getCallOptions(), request);
    }

    /**
     */
    public java.util.Iterator<com.alibaba.maxgraph.proto.RemoteApi.BatchVerticesCountResponse> getBatchOutCnt(
        com.alibaba.maxgraph.proto.RemoteApi.BatchVerticesEdgesRequest request) {
      return blockingServerStreamingCall(
          getChannel(), METHOD_GET_BATCH_OUT_CNT, getCallOptions(), request);
    }

    /**
     */
    public java.util.Iterator<com.alibaba.maxgraph.proto.RemoteApi.BatchVerticesCountResponse> getBatchInCnt(
        com.alibaba.maxgraph.proto.RemoteApi.BatchVerticesEdgesRequest request) {
      return blockingServerStreamingCall(
          getChannel(), METHOD_GET_BATCH_IN_CNT, getCallOptions(), request);
    }

    /**
     */
    public java.util.Iterator<com.alibaba.maxgraph.proto.GremlinQuery.VertexResponse> getVertices(
        com.alibaba.maxgraph.proto.RemoteApi.VerticesRequest request) {
      return blockingServerStreamingCall(
          getChannel(), METHOD_GET_VERTICES, getCallOptions(), request);
    }

    /**
     */
    public java.util.Iterator<com.alibaba.maxgraph.proto.StoreApi.GraphEdgeReponse> getGraphEdges(
        com.alibaba.maxgraph.proto.RemoteApi.GraphEdgesRequest request) {
      return blockingServerStreamingCall(
          getChannel(), METHOD_GET_GRAPH_EDGES, getCallOptions(), request);
    }

    /**
     */
    public java.util.Iterator<com.alibaba.maxgraph.proto.GremlinQuery.VertexResponse> queryVertices(
        com.alibaba.maxgraph.proto.RemoteApi.QueryRequest request) {
      return blockingServerStreamingCall(
          getChannel(), METHOD_QUERY_VERTICES, getCallOptions(), request);
    }

    /**
     */
    public java.util.Iterator<com.alibaba.maxgraph.proto.StoreApi.GraphEdgeReponse> queryEdges(
        com.alibaba.maxgraph.proto.RemoteApi.QueryRequest request) {
      return blockingServerStreamingCall(
          getChannel(), METHOD_QUERY_EDGES, getCallOptions(), request);
    }

    /**
     */
    public com.alibaba.maxgraph.proto.RemoteApi.CountResponse vertexCount(com.alibaba.maxgraph.proto.RemoteApi.QueryCountRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_VERTEX_COUNT, getCallOptions(), request);
    }

    /**
     */
    public com.alibaba.maxgraph.proto.RemoteApi.CountResponse edgeCount(com.alibaba.maxgraph.proto.RemoteApi.QueryCountRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_EDGE_COUNT, getCallOptions(), request);
    }
  }

  /**
   * <pre>
   * The store service definition.
   * </pre>
   */
  public static final class RemoteStoreServiceFutureStub extends io.grpc.stub.AbstractStub<RemoteStoreServiceFutureStub> {
    private RemoteStoreServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private RemoteStoreServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RemoteStoreServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new RemoteStoreServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.alibaba.maxgraph.proto.RemoteApi.CountResponse> vertexCount(
        com.alibaba.maxgraph.proto.RemoteApi.QueryCountRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_VERTEX_COUNT, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.alibaba.maxgraph.proto.RemoteApi.CountResponse> edgeCount(
        com.alibaba.maxgraph.proto.RemoteApi.QueryCountRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_EDGE_COUNT, getCallOptions()), request);
    }
  }

  private static final int METHODID_GET_BATCH_OUT = 0;
  private static final int METHODID_GET_BATCH_IN = 1;
  private static final int METHODID_GET_BATCH_OUT_CNT = 2;
  private static final int METHODID_GET_BATCH_IN_CNT = 3;
  private static final int METHODID_GET_VERTICES = 4;
  private static final int METHODID_GET_GRAPH_EDGES = 5;
  private static final int METHODID_QUERY_VERTICES = 6;
  private static final int METHODID_QUERY_EDGES = 7;
  private static final int METHODID_VERTEX_COUNT = 8;
  private static final int METHODID_EDGE_COUNT = 9;

  private static class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final RemoteStoreServiceImplBase serviceImpl;
    private final int methodId;

    public MethodHandlers(RemoteStoreServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_GET_BATCH_OUT:
          serviceImpl.getBatchOut((com.alibaba.maxgraph.proto.RemoteApi.BatchVerticesEdgesRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.RemoteApi.BatchVerticesEdgesResponse>) responseObserver);
          break;
        case METHODID_GET_BATCH_IN:
          serviceImpl.getBatchIn((com.alibaba.maxgraph.proto.RemoteApi.BatchVerticesEdgesRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.RemoteApi.BatchVerticesEdgesResponse>) responseObserver);
          break;
        case METHODID_GET_BATCH_OUT_CNT:
          serviceImpl.getBatchOutCnt((com.alibaba.maxgraph.proto.RemoteApi.BatchVerticesEdgesRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.RemoteApi.BatchVerticesCountResponse>) responseObserver);
          break;
        case METHODID_GET_BATCH_IN_CNT:
          serviceImpl.getBatchInCnt((com.alibaba.maxgraph.proto.RemoteApi.BatchVerticesEdgesRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.RemoteApi.BatchVerticesCountResponse>) responseObserver);
          break;
        case METHODID_GET_VERTICES:
          serviceImpl.getVertices((com.alibaba.maxgraph.proto.RemoteApi.VerticesRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.GremlinQuery.VertexResponse>) responseObserver);
          break;
        case METHODID_GET_GRAPH_EDGES:
          serviceImpl.getGraphEdges((com.alibaba.maxgraph.proto.RemoteApi.GraphEdgesRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.StoreApi.GraphEdgeReponse>) responseObserver);
          break;
        case METHODID_QUERY_VERTICES:
          serviceImpl.queryVertices((com.alibaba.maxgraph.proto.RemoteApi.QueryRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.GremlinQuery.VertexResponse>) responseObserver);
          break;
        case METHODID_QUERY_EDGES:
          serviceImpl.queryEdges((com.alibaba.maxgraph.proto.RemoteApi.QueryRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.StoreApi.GraphEdgeReponse>) responseObserver);
          break;
        case METHODID_VERTEX_COUNT:
          serviceImpl.vertexCount((com.alibaba.maxgraph.proto.RemoteApi.QueryCountRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.RemoteApi.CountResponse>) responseObserver);
          break;
        case METHODID_EDGE_COUNT:
          serviceImpl.edgeCount((com.alibaba.maxgraph.proto.RemoteApi.QueryCountRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.RemoteApi.CountResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    return new io.grpc.ServiceDescriptor(SERVICE_NAME,
        METHOD_GET_BATCH_OUT,
        METHOD_GET_BATCH_IN,
        METHOD_GET_BATCH_OUT_CNT,
        METHOD_GET_BATCH_IN_CNT,
        METHOD_GET_VERTICES,
        METHOD_GET_GRAPH_EDGES,
        METHOD_QUERY_VERTICES,
        METHOD_QUERY_EDGES,
        METHOD_VERTEX_COUNT,
        METHOD_EDGE_COUNT);
  }

}

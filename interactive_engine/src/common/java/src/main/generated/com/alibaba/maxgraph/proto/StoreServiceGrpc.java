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
    comments = "Source: store_api.proto")
public class StoreServiceGrpc {

  private StoreServiceGrpc() {}

  public static final String SERVICE_NAME = "tinkerpop.StoreService";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.StoreApi.GetOutEdgesRequest,
      com.alibaba.maxgraph.proto.StoreApi.GraphEdgeReponse> METHOD_GET_OUT_EDGES =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING,
          generateFullMethodName(
              "tinkerpop.StoreService", "getOutEdges"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.StoreApi.GetOutEdgesRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.StoreApi.GraphEdgeReponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.StoreApi.BatchVertexEdgeRequest,
      com.alibaba.maxgraph.proto.StoreApi.BatchVertexEdgeResponse> METHOD_GET_BATCH_OUT_TARGET =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING,
          generateFullMethodName(
              "tinkerpop.StoreService", "getBatchOutTarget"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.StoreApi.BatchVertexEdgeRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.StoreApi.BatchVertexEdgeResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.StoreApi.BatchVertexEdgeRequest,
      com.alibaba.maxgraph.proto.StoreApi.BatchVertexCountResponse> METHOD_GET_BATCH_OUT_COUNT =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING,
          generateFullMethodName(
              "tinkerpop.StoreService", "getBatchOutCount"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.StoreApi.BatchVertexEdgeRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.StoreApi.BatchVertexCountResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.StoreApi.GetInEdgesRequest,
      com.alibaba.maxgraph.proto.StoreApi.GraphEdgeReponse> METHOD_GET_IN_EDGES =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING,
          generateFullMethodName(
              "tinkerpop.StoreService", "getInEdges"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.StoreApi.GetInEdgesRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.StoreApi.GraphEdgeReponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.StoreApi.BatchVertexEdgeRequest,
      com.alibaba.maxgraph.proto.StoreApi.BatchVertexEdgeResponse> METHOD_GET_BATCH_IN_TARGET =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING,
          generateFullMethodName(
              "tinkerpop.StoreService", "getBatchInTarget"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.StoreApi.BatchVertexEdgeRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.StoreApi.BatchVertexEdgeResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.StoreApi.BatchVertexEdgeRequest,
      com.alibaba.maxgraph.proto.StoreApi.BatchVertexCountResponse> METHOD_GET_BATCH_IN_COUNT =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING,
          generateFullMethodName(
              "tinkerpop.StoreService", "getBatchInCount"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.StoreApi.BatchVertexEdgeRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.StoreApi.BatchVertexCountResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.StoreApi.GetVertexsRequest,
      com.alibaba.maxgraph.proto.GremlinQuery.VertexResponse> METHOD_GET_VERTEXS =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING,
          generateFullMethodName(
              "tinkerpop.StoreService", "getVertexs"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.StoreApi.GetVertexsRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.GremlinQuery.VertexResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.StoreApi.GetEdgesRequest,
      com.alibaba.maxgraph.proto.StoreApi.GraphEdgeReponse> METHOD_GET_EDGES =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING,
          generateFullMethodName(
              "tinkerpop.StoreService", "getEdges"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.StoreApi.GetEdgesRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.StoreApi.GraphEdgeReponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.StoreApi.ScanEdgeRequest,
      com.alibaba.maxgraph.proto.StoreApi.GraphEdgeReponse> METHOD_SCAN_EDGES =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING,
          generateFullMethodName(
              "tinkerpop.StoreService", "scanEdges"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.StoreApi.ScanEdgeRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.StoreApi.GraphEdgeReponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.StoreApi.ScanRequest,
      com.alibaba.maxgraph.proto.GremlinQuery.VertexResponse> METHOD_SCAN =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING,
          generateFullMethodName(
              "tinkerpop.StoreService", "scan"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.StoreApi.ScanRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.GremlinQuery.VertexResponse.getDefaultInstance()));

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static StoreServiceStub newStub(io.grpc.Channel channel) {
    return new StoreServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static StoreServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new StoreServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary and streaming output calls on the service
   */
  public static StoreServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new StoreServiceFutureStub(channel);
  }

  /**
   * <pre>
   * The store service definition.
   * </pre>
   */
  public static abstract class StoreServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void getOutEdges(com.alibaba.maxgraph.proto.StoreApi.GetOutEdgesRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.StoreApi.GraphEdgeReponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_OUT_EDGES, responseObserver);
    }

    /**
     */
    public void getBatchOutTarget(com.alibaba.maxgraph.proto.StoreApi.BatchVertexEdgeRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.StoreApi.BatchVertexEdgeResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_BATCH_OUT_TARGET, responseObserver);
    }

    /**
     */
    public void getBatchOutCount(com.alibaba.maxgraph.proto.StoreApi.BatchVertexEdgeRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.StoreApi.BatchVertexCountResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_BATCH_OUT_COUNT, responseObserver);
    }

    /**
     */
    public void getInEdges(com.alibaba.maxgraph.proto.StoreApi.GetInEdgesRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.StoreApi.GraphEdgeReponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_IN_EDGES, responseObserver);
    }

    /**
     */
    public void getBatchInTarget(com.alibaba.maxgraph.proto.StoreApi.BatchVertexEdgeRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.StoreApi.BatchVertexEdgeResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_BATCH_IN_TARGET, responseObserver);
    }

    /**
     */
    public void getBatchInCount(com.alibaba.maxgraph.proto.StoreApi.BatchVertexEdgeRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.StoreApi.BatchVertexCountResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_BATCH_IN_COUNT, responseObserver);
    }

    /**
     */
    public void getVertexs(com.alibaba.maxgraph.proto.StoreApi.GetVertexsRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.GremlinQuery.VertexResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_VERTEXS, responseObserver);
    }

    /**
     */
    public void getEdges(com.alibaba.maxgraph.proto.StoreApi.GetEdgesRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.StoreApi.GraphEdgeReponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_EDGES, responseObserver);
    }

    /**
     */
    public void scanEdges(com.alibaba.maxgraph.proto.StoreApi.ScanEdgeRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.StoreApi.GraphEdgeReponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_SCAN_EDGES, responseObserver);
    }

    /**
     */
    public void scan(com.alibaba.maxgraph.proto.StoreApi.ScanRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.GremlinQuery.VertexResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_SCAN, responseObserver);
    }

    @java.lang.Override public io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            METHOD_GET_OUT_EDGES,
            asyncServerStreamingCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.StoreApi.GetOutEdgesRequest,
                com.alibaba.maxgraph.proto.StoreApi.GraphEdgeReponse>(
                  this, METHODID_GET_OUT_EDGES)))
          .addMethod(
            METHOD_GET_BATCH_OUT_TARGET,
            asyncServerStreamingCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.StoreApi.BatchVertexEdgeRequest,
                com.alibaba.maxgraph.proto.StoreApi.BatchVertexEdgeResponse>(
                  this, METHODID_GET_BATCH_OUT_TARGET)))
          .addMethod(
            METHOD_GET_BATCH_OUT_COUNT,
            asyncServerStreamingCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.StoreApi.BatchVertexEdgeRequest,
                com.alibaba.maxgraph.proto.StoreApi.BatchVertexCountResponse>(
                  this, METHODID_GET_BATCH_OUT_COUNT)))
          .addMethod(
            METHOD_GET_IN_EDGES,
            asyncServerStreamingCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.StoreApi.GetInEdgesRequest,
                com.alibaba.maxgraph.proto.StoreApi.GraphEdgeReponse>(
                  this, METHODID_GET_IN_EDGES)))
          .addMethod(
            METHOD_GET_BATCH_IN_TARGET,
            asyncServerStreamingCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.StoreApi.BatchVertexEdgeRequest,
                com.alibaba.maxgraph.proto.StoreApi.BatchVertexEdgeResponse>(
                  this, METHODID_GET_BATCH_IN_TARGET)))
          .addMethod(
            METHOD_GET_BATCH_IN_COUNT,
            asyncServerStreamingCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.StoreApi.BatchVertexEdgeRequest,
                com.alibaba.maxgraph.proto.StoreApi.BatchVertexCountResponse>(
                  this, METHODID_GET_BATCH_IN_COUNT)))
          .addMethod(
            METHOD_GET_VERTEXS,
            asyncServerStreamingCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.StoreApi.GetVertexsRequest,
                com.alibaba.maxgraph.proto.GremlinQuery.VertexResponse>(
                  this, METHODID_GET_VERTEXS)))
          .addMethod(
            METHOD_GET_EDGES,
            asyncServerStreamingCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.StoreApi.GetEdgesRequest,
                com.alibaba.maxgraph.proto.StoreApi.GraphEdgeReponse>(
                  this, METHODID_GET_EDGES)))
          .addMethod(
            METHOD_SCAN_EDGES,
            asyncServerStreamingCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.StoreApi.ScanEdgeRequest,
                com.alibaba.maxgraph.proto.StoreApi.GraphEdgeReponse>(
                  this, METHODID_SCAN_EDGES)))
          .addMethod(
            METHOD_SCAN,
            asyncServerStreamingCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.StoreApi.ScanRequest,
                com.alibaba.maxgraph.proto.GremlinQuery.VertexResponse>(
                  this, METHODID_SCAN)))
          .build();
    }
  }

  /**
   * <pre>
   * The store service definition.
   * </pre>
   */
  public static final class StoreServiceStub extends io.grpc.stub.AbstractStub<StoreServiceStub> {
    private StoreServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private StoreServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected StoreServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new StoreServiceStub(channel, callOptions);
    }

    /**
     */
    public void getOutEdges(com.alibaba.maxgraph.proto.StoreApi.GetOutEdgesRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.StoreApi.GraphEdgeReponse> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(METHOD_GET_OUT_EDGES, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getBatchOutTarget(com.alibaba.maxgraph.proto.StoreApi.BatchVertexEdgeRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.StoreApi.BatchVertexEdgeResponse> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(METHOD_GET_BATCH_OUT_TARGET, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getBatchOutCount(com.alibaba.maxgraph.proto.StoreApi.BatchVertexEdgeRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.StoreApi.BatchVertexCountResponse> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(METHOD_GET_BATCH_OUT_COUNT, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getInEdges(com.alibaba.maxgraph.proto.StoreApi.GetInEdgesRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.StoreApi.GraphEdgeReponse> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(METHOD_GET_IN_EDGES, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getBatchInTarget(com.alibaba.maxgraph.proto.StoreApi.BatchVertexEdgeRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.StoreApi.BatchVertexEdgeResponse> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(METHOD_GET_BATCH_IN_TARGET, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getBatchInCount(com.alibaba.maxgraph.proto.StoreApi.BatchVertexEdgeRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.StoreApi.BatchVertexCountResponse> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(METHOD_GET_BATCH_IN_COUNT, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getVertexs(com.alibaba.maxgraph.proto.StoreApi.GetVertexsRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.GremlinQuery.VertexResponse> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(METHOD_GET_VERTEXS, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getEdges(com.alibaba.maxgraph.proto.StoreApi.GetEdgesRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.StoreApi.GraphEdgeReponse> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(METHOD_GET_EDGES, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void scanEdges(com.alibaba.maxgraph.proto.StoreApi.ScanEdgeRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.StoreApi.GraphEdgeReponse> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(METHOD_SCAN_EDGES, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void scan(com.alibaba.maxgraph.proto.StoreApi.ScanRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.GremlinQuery.VertexResponse> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(METHOD_SCAN, getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   * The store service definition.
   * </pre>
   */
  public static final class StoreServiceBlockingStub extends io.grpc.stub.AbstractStub<StoreServiceBlockingStub> {
    private StoreServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private StoreServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected StoreServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new StoreServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public java.util.Iterator<com.alibaba.maxgraph.proto.StoreApi.GraphEdgeReponse> getOutEdges(
        com.alibaba.maxgraph.proto.StoreApi.GetOutEdgesRequest request) {
      return blockingServerStreamingCall(
          getChannel(), METHOD_GET_OUT_EDGES, getCallOptions(), request);
    }

    /**
     */
    public java.util.Iterator<com.alibaba.maxgraph.proto.StoreApi.BatchVertexEdgeResponse> getBatchOutTarget(
        com.alibaba.maxgraph.proto.StoreApi.BatchVertexEdgeRequest request) {
      return blockingServerStreamingCall(
          getChannel(), METHOD_GET_BATCH_OUT_TARGET, getCallOptions(), request);
    }

    /**
     */
    public java.util.Iterator<com.alibaba.maxgraph.proto.StoreApi.BatchVertexCountResponse> getBatchOutCount(
        com.alibaba.maxgraph.proto.StoreApi.BatchVertexEdgeRequest request) {
      return blockingServerStreamingCall(
          getChannel(), METHOD_GET_BATCH_OUT_COUNT, getCallOptions(), request);
    }

    /**
     */
    public java.util.Iterator<com.alibaba.maxgraph.proto.StoreApi.GraphEdgeReponse> getInEdges(
        com.alibaba.maxgraph.proto.StoreApi.GetInEdgesRequest request) {
      return blockingServerStreamingCall(
          getChannel(), METHOD_GET_IN_EDGES, getCallOptions(), request);
    }

    /**
     */
    public java.util.Iterator<com.alibaba.maxgraph.proto.StoreApi.BatchVertexEdgeResponse> getBatchInTarget(
        com.alibaba.maxgraph.proto.StoreApi.BatchVertexEdgeRequest request) {
      return blockingServerStreamingCall(
          getChannel(), METHOD_GET_BATCH_IN_TARGET, getCallOptions(), request);
    }

    /**
     */
    public java.util.Iterator<com.alibaba.maxgraph.proto.StoreApi.BatchVertexCountResponse> getBatchInCount(
        com.alibaba.maxgraph.proto.StoreApi.BatchVertexEdgeRequest request) {
      return blockingServerStreamingCall(
          getChannel(), METHOD_GET_BATCH_IN_COUNT, getCallOptions(), request);
    }

    /**
     */
    public java.util.Iterator<com.alibaba.maxgraph.proto.GremlinQuery.VertexResponse> getVertexs(
        com.alibaba.maxgraph.proto.StoreApi.GetVertexsRequest request) {
      return blockingServerStreamingCall(
          getChannel(), METHOD_GET_VERTEXS, getCallOptions(), request);
    }

    /**
     */
    public java.util.Iterator<com.alibaba.maxgraph.proto.StoreApi.GraphEdgeReponse> getEdges(
        com.alibaba.maxgraph.proto.StoreApi.GetEdgesRequest request) {
      return blockingServerStreamingCall(
          getChannel(), METHOD_GET_EDGES, getCallOptions(), request);
    }

    /**
     */
    public java.util.Iterator<com.alibaba.maxgraph.proto.StoreApi.GraphEdgeReponse> scanEdges(
        com.alibaba.maxgraph.proto.StoreApi.ScanEdgeRequest request) {
      return blockingServerStreamingCall(
          getChannel(), METHOD_SCAN_EDGES, getCallOptions(), request);
    }

    /**
     */
    public java.util.Iterator<com.alibaba.maxgraph.proto.GremlinQuery.VertexResponse> scan(
        com.alibaba.maxgraph.proto.StoreApi.ScanRequest request) {
      return blockingServerStreamingCall(
          getChannel(), METHOD_SCAN, getCallOptions(), request);
    }
  }

  /**
   * <pre>
   * The store service definition.
   * </pre>
   */
  public static final class StoreServiceFutureStub extends io.grpc.stub.AbstractStub<StoreServiceFutureStub> {
    private StoreServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private StoreServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected StoreServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new StoreServiceFutureStub(channel, callOptions);
    }
  }

  private static final int METHODID_GET_OUT_EDGES = 0;
  private static final int METHODID_GET_BATCH_OUT_TARGET = 1;
  private static final int METHODID_GET_BATCH_OUT_COUNT = 2;
  private static final int METHODID_GET_IN_EDGES = 3;
  private static final int METHODID_GET_BATCH_IN_TARGET = 4;
  private static final int METHODID_GET_BATCH_IN_COUNT = 5;
  private static final int METHODID_GET_VERTEXS = 6;
  private static final int METHODID_GET_EDGES = 7;
  private static final int METHODID_SCAN_EDGES = 8;
  private static final int METHODID_SCAN = 9;

  private static class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final StoreServiceImplBase serviceImpl;
    private final int methodId;

    public MethodHandlers(StoreServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_GET_OUT_EDGES:
          serviceImpl.getOutEdges((com.alibaba.maxgraph.proto.StoreApi.GetOutEdgesRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.StoreApi.GraphEdgeReponse>) responseObserver);
          break;
        case METHODID_GET_BATCH_OUT_TARGET:
          serviceImpl.getBatchOutTarget((com.alibaba.maxgraph.proto.StoreApi.BatchVertexEdgeRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.StoreApi.BatchVertexEdgeResponse>) responseObserver);
          break;
        case METHODID_GET_BATCH_OUT_COUNT:
          serviceImpl.getBatchOutCount((com.alibaba.maxgraph.proto.StoreApi.BatchVertexEdgeRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.StoreApi.BatchVertexCountResponse>) responseObserver);
          break;
        case METHODID_GET_IN_EDGES:
          serviceImpl.getInEdges((com.alibaba.maxgraph.proto.StoreApi.GetInEdgesRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.StoreApi.GraphEdgeReponse>) responseObserver);
          break;
        case METHODID_GET_BATCH_IN_TARGET:
          serviceImpl.getBatchInTarget((com.alibaba.maxgraph.proto.StoreApi.BatchVertexEdgeRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.StoreApi.BatchVertexEdgeResponse>) responseObserver);
          break;
        case METHODID_GET_BATCH_IN_COUNT:
          serviceImpl.getBatchInCount((com.alibaba.maxgraph.proto.StoreApi.BatchVertexEdgeRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.StoreApi.BatchVertexCountResponse>) responseObserver);
          break;
        case METHODID_GET_VERTEXS:
          serviceImpl.getVertexs((com.alibaba.maxgraph.proto.StoreApi.GetVertexsRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.GremlinQuery.VertexResponse>) responseObserver);
          break;
        case METHODID_GET_EDGES:
          serviceImpl.getEdges((com.alibaba.maxgraph.proto.StoreApi.GetEdgesRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.StoreApi.GraphEdgeReponse>) responseObserver);
          break;
        case METHODID_SCAN_EDGES:
          serviceImpl.scanEdges((com.alibaba.maxgraph.proto.StoreApi.ScanEdgeRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.StoreApi.GraphEdgeReponse>) responseObserver);
          break;
        case METHODID_SCAN:
          serviceImpl.scan((com.alibaba.maxgraph.proto.StoreApi.ScanRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.GremlinQuery.VertexResponse>) responseObserver);
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
        METHOD_GET_OUT_EDGES,
        METHOD_GET_BATCH_OUT_TARGET,
        METHOD_GET_BATCH_OUT_COUNT,
        METHOD_GET_IN_EDGES,
        METHOD_GET_BATCH_IN_TARGET,
        METHOD_GET_BATCH_IN_COUNT,
        METHOD_GET_VERTEXS,
        METHOD_GET_EDGES,
        METHOD_SCAN_EDGES,
        METHOD_SCAN);
  }

}

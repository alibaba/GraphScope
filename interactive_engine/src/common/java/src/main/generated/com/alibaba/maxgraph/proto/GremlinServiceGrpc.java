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
 * The gremlin service definition.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.0.0)",
    comments = "Source: gremlin_query.proto")
public class GremlinServiceGrpc {

  private GremlinServiceGrpc() {}

  public static final String SERVICE_NAME = "tinkerpop.GremlinService";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.GremlinQuery.EdgesRequest,
      com.alibaba.maxgraph.proto.GremlinQuery.EdgesReponse> METHOD_GET_EDGES =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING,
          generateFullMethodName(
              "tinkerpop.GremlinService", "getEdges"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.GremlinQuery.EdgesRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.GremlinQuery.EdgesReponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.GremlinQuery.VertexRequest,
      com.alibaba.maxgraph.proto.GremlinQuery.VertexResponse> METHOD_GET_VERTEXS =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING,
          generateFullMethodName(
              "tinkerpop.GremlinService", "getVertexs"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.GremlinQuery.VertexRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.GremlinQuery.VertexResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.GremlinQuery.LimitEdgeRequest,
      com.alibaba.maxgraph.proto.GremlinQuery.LimitEdgesReponse> METHOD_GET_LIMIT_EDGES =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING,
          generateFullMethodName(
              "tinkerpop.GremlinService", "getLimitEdges"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.GremlinQuery.LimitEdgeRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.GremlinQuery.LimitEdgesReponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.GremlinQuery.VertexScanRequest,
      com.alibaba.maxgraph.proto.GremlinQuery.VertexResponse> METHOD_SCAN =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING,
          generateFullMethodName(
              "tinkerpop.GremlinService", "scan"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.GremlinQuery.VertexScanRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.GremlinQuery.VertexResponse.getDefaultInstance()));

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static GremlinServiceStub newStub(io.grpc.Channel channel) {
    return new GremlinServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static GremlinServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new GremlinServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary and streaming output calls on the service
   */
  public static GremlinServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new GremlinServiceFutureStub(channel);
  }

  /**
   * <pre>
   * The gremlin service definition.
   * </pre>
   */
  public static abstract class GremlinServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void getEdges(com.alibaba.maxgraph.proto.GremlinQuery.EdgesRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.GremlinQuery.EdgesReponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_EDGES, responseObserver);
    }

    /**
     */
    public void getVertexs(com.alibaba.maxgraph.proto.GremlinQuery.VertexRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.GremlinQuery.VertexResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_VERTEXS, responseObserver);
    }

    /**
     */
    public void getLimitEdges(com.alibaba.maxgraph.proto.GremlinQuery.LimitEdgeRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.GremlinQuery.LimitEdgesReponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_LIMIT_EDGES, responseObserver);
    }

    /**
     */
    public void scan(com.alibaba.maxgraph.proto.GremlinQuery.VertexScanRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.GremlinQuery.VertexResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_SCAN, responseObserver);
    }

    @java.lang.Override public io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            METHOD_GET_EDGES,
            asyncServerStreamingCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.GremlinQuery.EdgesRequest,
                com.alibaba.maxgraph.proto.GremlinQuery.EdgesReponse>(
                  this, METHODID_GET_EDGES)))
          .addMethod(
            METHOD_GET_VERTEXS,
            asyncServerStreamingCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.GremlinQuery.VertexRequest,
                com.alibaba.maxgraph.proto.GremlinQuery.VertexResponse>(
                  this, METHODID_GET_VERTEXS)))
          .addMethod(
            METHOD_GET_LIMIT_EDGES,
            asyncServerStreamingCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.GremlinQuery.LimitEdgeRequest,
                com.alibaba.maxgraph.proto.GremlinQuery.LimitEdgesReponse>(
                  this, METHODID_GET_LIMIT_EDGES)))
          .addMethod(
            METHOD_SCAN,
            asyncServerStreamingCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.GremlinQuery.VertexScanRequest,
                com.alibaba.maxgraph.proto.GremlinQuery.VertexResponse>(
                  this, METHODID_SCAN)))
          .build();
    }
  }

  /**
   * <pre>
   * The gremlin service definition.
   * </pre>
   */
  public static final class GremlinServiceStub extends io.grpc.stub.AbstractStub<GremlinServiceStub> {
    private GremlinServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private GremlinServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected GremlinServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new GremlinServiceStub(channel, callOptions);
    }

    /**
     */
    public void getEdges(com.alibaba.maxgraph.proto.GremlinQuery.EdgesRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.GremlinQuery.EdgesReponse> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(METHOD_GET_EDGES, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getVertexs(com.alibaba.maxgraph.proto.GremlinQuery.VertexRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.GremlinQuery.VertexResponse> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(METHOD_GET_VERTEXS, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getLimitEdges(com.alibaba.maxgraph.proto.GremlinQuery.LimitEdgeRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.GremlinQuery.LimitEdgesReponse> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(METHOD_GET_LIMIT_EDGES, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void scan(com.alibaba.maxgraph.proto.GremlinQuery.VertexScanRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.GremlinQuery.VertexResponse> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(METHOD_SCAN, getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   * The gremlin service definition.
   * </pre>
   */
  public static final class GremlinServiceBlockingStub extends io.grpc.stub.AbstractStub<GremlinServiceBlockingStub> {
    private GremlinServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private GremlinServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected GremlinServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new GremlinServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public java.util.Iterator<com.alibaba.maxgraph.proto.GremlinQuery.EdgesReponse> getEdges(
        com.alibaba.maxgraph.proto.GremlinQuery.EdgesRequest request) {
      return blockingServerStreamingCall(
          getChannel(), METHOD_GET_EDGES, getCallOptions(), request);
    }

    /**
     */
    public java.util.Iterator<com.alibaba.maxgraph.proto.GremlinQuery.VertexResponse> getVertexs(
        com.alibaba.maxgraph.proto.GremlinQuery.VertexRequest request) {
      return blockingServerStreamingCall(
          getChannel(), METHOD_GET_VERTEXS, getCallOptions(), request);
    }

    /**
     */
    public java.util.Iterator<com.alibaba.maxgraph.proto.GremlinQuery.LimitEdgesReponse> getLimitEdges(
        com.alibaba.maxgraph.proto.GremlinQuery.LimitEdgeRequest request) {
      return blockingServerStreamingCall(
          getChannel(), METHOD_GET_LIMIT_EDGES, getCallOptions(), request);
    }

    /**
     */
    public java.util.Iterator<com.alibaba.maxgraph.proto.GremlinQuery.VertexResponse> scan(
        com.alibaba.maxgraph.proto.GremlinQuery.VertexScanRequest request) {
      return blockingServerStreamingCall(
          getChannel(), METHOD_SCAN, getCallOptions(), request);
    }
  }

  /**
   * <pre>
   * The gremlin service definition.
   * </pre>
   */
  public static final class GremlinServiceFutureStub extends io.grpc.stub.AbstractStub<GremlinServiceFutureStub> {
    private GremlinServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private GremlinServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected GremlinServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new GremlinServiceFutureStub(channel, callOptions);
    }
  }

  private static final int METHODID_GET_EDGES = 0;
  private static final int METHODID_GET_VERTEXS = 1;
  private static final int METHODID_GET_LIMIT_EDGES = 2;
  private static final int METHODID_SCAN = 3;

  private static class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final GremlinServiceImplBase serviceImpl;
    private final int methodId;

    public MethodHandlers(GremlinServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_GET_EDGES:
          serviceImpl.getEdges((com.alibaba.maxgraph.proto.GremlinQuery.EdgesRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.GremlinQuery.EdgesReponse>) responseObserver);
          break;
        case METHODID_GET_VERTEXS:
          serviceImpl.getVertexs((com.alibaba.maxgraph.proto.GremlinQuery.VertexRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.GremlinQuery.VertexResponse>) responseObserver);
          break;
        case METHODID_GET_LIMIT_EDGES:
          serviceImpl.getLimitEdges((com.alibaba.maxgraph.proto.GremlinQuery.LimitEdgeRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.GremlinQuery.LimitEdgesReponse>) responseObserver);
          break;
        case METHODID_SCAN:
          serviceImpl.scan((com.alibaba.maxgraph.proto.GremlinQuery.VertexScanRequest) request,
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
        METHOD_GET_EDGES,
        METHOD_GET_VERTEXS,
        METHOD_GET_LIMIT_EDGES,
        METHOD_SCAN);
  }

}

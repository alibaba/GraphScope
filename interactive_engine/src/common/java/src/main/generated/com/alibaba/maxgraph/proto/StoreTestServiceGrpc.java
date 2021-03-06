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
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.0.0)",
    comments = "Source: debug.proto")
public class StoreTestServiceGrpc {

  private StoreTestServiceGrpc() {}

  public static final String SERVICE_NAME = "StoreTestService";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.Empty,
      com.alibaba.maxgraph.proto.ServerInfo> METHOD_GET_SERVER_INFO =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "StoreTestService", "getServerInfo"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.Empty.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.ServerInfo.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.GetVertexRequest,
      com.alibaba.maxgraph.proto.StoreTestResponse> METHOD_GET_VERTEX =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "StoreTestService", "getVertex"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.GetVertexRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.StoreTestResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.GetOutEdgesRequest,
      com.alibaba.maxgraph.proto.StoreTestResponse> METHOD_GET_OUT_EDGES =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "StoreTestService", "getOutEdges"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.GetOutEdgesRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.StoreTestResponse.getDefaultInstance()));

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static StoreTestServiceStub newStub(io.grpc.Channel channel) {
    return new StoreTestServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static StoreTestServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new StoreTestServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary and streaming output calls on the service
   */
  public static StoreTestServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new StoreTestServiceFutureStub(channel);
  }

  /**
   */
  public static abstract class StoreTestServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void getServerInfo(com.alibaba.maxgraph.proto.Empty request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.ServerInfo> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_SERVER_INFO, responseObserver);
    }

    /**
     */
    public void getVertex(com.alibaba.maxgraph.proto.GetVertexRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.StoreTestResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_VERTEX, responseObserver);
    }

    /**
     */
    public void getOutEdges(com.alibaba.maxgraph.proto.GetOutEdgesRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.StoreTestResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_OUT_EDGES, responseObserver);
    }

    @java.lang.Override public io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            METHOD_GET_SERVER_INFO,
            asyncUnaryCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.Empty,
                com.alibaba.maxgraph.proto.ServerInfo>(
                  this, METHODID_GET_SERVER_INFO)))
          .addMethod(
            METHOD_GET_VERTEX,
            asyncUnaryCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.GetVertexRequest,
                com.alibaba.maxgraph.proto.StoreTestResponse>(
                  this, METHODID_GET_VERTEX)))
          .addMethod(
            METHOD_GET_OUT_EDGES,
            asyncUnaryCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.GetOutEdgesRequest,
                com.alibaba.maxgraph.proto.StoreTestResponse>(
                  this, METHODID_GET_OUT_EDGES)))
          .build();
    }
  }

  /**
   */
  public static final class StoreTestServiceStub extends io.grpc.stub.AbstractStub<StoreTestServiceStub> {
    private StoreTestServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private StoreTestServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected StoreTestServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new StoreTestServiceStub(channel, callOptions);
    }

    /**
     */
    public void getServerInfo(com.alibaba.maxgraph.proto.Empty request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.ServerInfo> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_GET_SERVER_INFO, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getVertex(com.alibaba.maxgraph.proto.GetVertexRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.StoreTestResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_GET_VERTEX, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getOutEdges(com.alibaba.maxgraph.proto.GetOutEdgesRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.StoreTestResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_GET_OUT_EDGES, getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class StoreTestServiceBlockingStub extends io.grpc.stub.AbstractStub<StoreTestServiceBlockingStub> {
    private StoreTestServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private StoreTestServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected StoreTestServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new StoreTestServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.alibaba.maxgraph.proto.ServerInfo getServerInfo(com.alibaba.maxgraph.proto.Empty request) {
      return blockingUnaryCall(
          getChannel(), METHOD_GET_SERVER_INFO, getCallOptions(), request);
    }

    /**
     */
    public com.alibaba.maxgraph.proto.StoreTestResponse getVertex(com.alibaba.maxgraph.proto.GetVertexRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_GET_VERTEX, getCallOptions(), request);
    }

    /**
     */
    public com.alibaba.maxgraph.proto.StoreTestResponse getOutEdges(com.alibaba.maxgraph.proto.GetOutEdgesRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_GET_OUT_EDGES, getCallOptions(), request);
    }
  }

  /**
   */
  public static final class StoreTestServiceFutureStub extends io.grpc.stub.AbstractStub<StoreTestServiceFutureStub> {
    private StoreTestServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private StoreTestServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected StoreTestServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new StoreTestServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.alibaba.maxgraph.proto.ServerInfo> getServerInfo(
        com.alibaba.maxgraph.proto.Empty request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_GET_SERVER_INFO, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.alibaba.maxgraph.proto.StoreTestResponse> getVertex(
        com.alibaba.maxgraph.proto.GetVertexRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_GET_VERTEX, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.alibaba.maxgraph.proto.StoreTestResponse> getOutEdges(
        com.alibaba.maxgraph.proto.GetOutEdgesRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_GET_OUT_EDGES, getCallOptions()), request);
    }
  }

  private static final int METHODID_GET_SERVER_INFO = 0;
  private static final int METHODID_GET_VERTEX = 1;
  private static final int METHODID_GET_OUT_EDGES = 2;

  private static class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final StoreTestServiceImplBase serviceImpl;
    private final int methodId;

    public MethodHandlers(StoreTestServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_GET_SERVER_INFO:
          serviceImpl.getServerInfo((com.alibaba.maxgraph.proto.Empty) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.ServerInfo>) responseObserver);
          break;
        case METHODID_GET_VERTEX:
          serviceImpl.getVertex((com.alibaba.maxgraph.proto.GetVertexRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.StoreTestResponse>) responseObserver);
          break;
        case METHODID_GET_OUT_EDGES:
          serviceImpl.getOutEdges((com.alibaba.maxgraph.proto.GetOutEdgesRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.StoreTestResponse>) responseObserver);
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
        METHOD_GET_SERVER_INFO,
        METHOD_GET_VERTEX,
        METHOD_GET_OUT_EDGES);
  }

}

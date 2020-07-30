package com.alibaba.maxgraph.proto.store;

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
    comments = "Source: meta_service.proto")
public class MetaServiceGrpc {

  private MetaServiceGrpc() {}

  public static final String SERVICE_NAME = "maxgraph_store.MetaService";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.store.GetStoreListRequest,
      com.alibaba.maxgraph.proto.store.GetStoreListResponse> METHOD_GET_STORE_LIST =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "maxgraph_store.MetaService", "get_store_list"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.store.GetStoreListRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.store.GetStoreListResponse.getDefaultInstance()));

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static MetaServiceStub newStub(io.grpc.Channel channel) {
    return new MetaServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static MetaServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new MetaServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary and streaming output calls on the service
   */
  public static MetaServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new MetaServiceFutureStub(channel);
  }

  /**
   */
  public static abstract class MetaServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void getStoreList(com.alibaba.maxgraph.proto.store.GetStoreListRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.store.GetStoreListResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_STORE_LIST, responseObserver);
    }

    @java.lang.Override public io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            METHOD_GET_STORE_LIST,
            asyncUnaryCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.store.GetStoreListRequest,
                com.alibaba.maxgraph.proto.store.GetStoreListResponse>(
                  this, METHODID_GET_STORE_LIST)))
          .build();
    }
  }

  /**
   */
  public static final class MetaServiceStub extends io.grpc.stub.AbstractStub<MetaServiceStub> {
    private MetaServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private MetaServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MetaServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new MetaServiceStub(channel, callOptions);
    }

    /**
     */
    public void getStoreList(com.alibaba.maxgraph.proto.store.GetStoreListRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.store.GetStoreListResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_GET_STORE_LIST, getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class MetaServiceBlockingStub extends io.grpc.stub.AbstractStub<MetaServiceBlockingStub> {
    private MetaServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private MetaServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MetaServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new MetaServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.alibaba.maxgraph.proto.store.GetStoreListResponse getStoreList(com.alibaba.maxgraph.proto.store.GetStoreListRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_GET_STORE_LIST, getCallOptions(), request);
    }
  }

  /**
   */
  public static final class MetaServiceFutureStub extends io.grpc.stub.AbstractStub<MetaServiceFutureStub> {
    private MetaServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private MetaServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MetaServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new MetaServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.alibaba.maxgraph.proto.store.GetStoreListResponse> getStoreList(
        com.alibaba.maxgraph.proto.store.GetStoreListRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_GET_STORE_LIST, getCallOptions()), request);
    }
  }

  private static final int METHODID_GET_STORE_LIST = 0;

  private static class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final MetaServiceImplBase serviceImpl;
    private final int methodId;

    public MethodHandlers(MetaServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_GET_STORE_LIST:
          serviceImpl.getStoreList((com.alibaba.maxgraph.proto.store.GetStoreListRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.store.GetStoreListResponse>) responseObserver);
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
        METHOD_GET_STORE_LIST);
  }

}

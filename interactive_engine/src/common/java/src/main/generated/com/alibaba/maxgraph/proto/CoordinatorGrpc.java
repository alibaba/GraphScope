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
    comments = "Source: coordinator.proto")
public class CoordinatorGrpc {

  private CoordinatorGrpc() {}

  public static final String SERVICE_NAME = "Coordinator";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.HeartbeartRequest,
      com.alibaba.maxgraph.proto.HeartbeartResponse> METHOD_HEARTBEAT =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "Coordinator", "heartbeat"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.HeartbeartRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.HeartbeartResponse.getDefaultInstance()));

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static CoordinatorStub newStub(io.grpc.Channel channel) {
    return new CoordinatorStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static CoordinatorBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new CoordinatorBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary and streaming output calls on the service
   */
  public static CoordinatorFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new CoordinatorFutureStub(channel);
  }

  /**
   */
  public static abstract class CoordinatorImplBase implements io.grpc.BindableService {

    /**
     */
    public void heartbeat(com.alibaba.maxgraph.proto.HeartbeartRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.HeartbeartResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_HEARTBEAT, responseObserver);
    }

    @java.lang.Override public io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            METHOD_HEARTBEAT,
            asyncUnaryCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.HeartbeartRequest,
                com.alibaba.maxgraph.proto.HeartbeartResponse>(
                  this, METHODID_HEARTBEAT)))
          .build();
    }
  }

  /**
   */
  public static final class CoordinatorStub extends io.grpc.stub.AbstractStub<CoordinatorStub> {
    private CoordinatorStub(io.grpc.Channel channel) {
      super(channel);
    }

    private CoordinatorStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected CoordinatorStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new CoordinatorStub(channel, callOptions);
    }

    /**
     */
    public void heartbeat(com.alibaba.maxgraph.proto.HeartbeartRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.HeartbeartResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_HEARTBEAT, getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class CoordinatorBlockingStub extends io.grpc.stub.AbstractStub<CoordinatorBlockingStub> {
    private CoordinatorBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private CoordinatorBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected CoordinatorBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new CoordinatorBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.alibaba.maxgraph.proto.HeartbeartResponse heartbeat(com.alibaba.maxgraph.proto.HeartbeartRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_HEARTBEAT, getCallOptions(), request);
    }
  }

  /**
   */
  public static final class CoordinatorFutureStub extends io.grpc.stub.AbstractStub<CoordinatorFutureStub> {
    private CoordinatorFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private CoordinatorFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected CoordinatorFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new CoordinatorFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.alibaba.maxgraph.proto.HeartbeartResponse> heartbeat(
        com.alibaba.maxgraph.proto.HeartbeartRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_HEARTBEAT, getCallOptions()), request);
    }
  }

  private static final int METHODID_HEARTBEAT = 0;

  private static class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final CoordinatorImplBase serviceImpl;
    private final int methodId;

    public MethodHandlers(CoordinatorImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_HEARTBEAT:
          serviceImpl.heartbeat((com.alibaba.maxgraph.proto.HeartbeartRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.HeartbeartResponse>) responseObserver);
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
        METHOD_HEARTBEAT);
  }

}

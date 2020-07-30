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
    comments = "Source: am.proto")
public class AppMasterApiGrpc {

  private AppMasterApiGrpc() {}

  public static final String SERVICE_NAME = "AppMasterApi";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.RestartWorkerRequest,
      com.alibaba.maxgraph.proto.Response> METHOD_RESTART_WORKER =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "AppMasterApi", "restartWorker"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.RestartWorkerRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.Response.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.KillWorkerRequest,
      com.alibaba.maxgraph.proto.Response> METHOD_KILL_WORKER =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "AppMasterApi", "killWorker"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.KillWorkerRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.Response.getDefaultInstance()));

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static AppMasterApiStub newStub(io.grpc.Channel channel) {
    return new AppMasterApiStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static AppMasterApiBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new AppMasterApiBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary and streaming output calls on the service
   */
  public static AppMasterApiFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new AppMasterApiFutureStub(channel);
  }

  /**
   */
  public static abstract class AppMasterApiImplBase implements io.grpc.BindableService {

    /**
     */
    public void restartWorker(com.alibaba.maxgraph.proto.RestartWorkerRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.Response> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_RESTART_WORKER, responseObserver);
    }

    /**
     */
    public void killWorker(com.alibaba.maxgraph.proto.KillWorkerRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.Response> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_KILL_WORKER, responseObserver);
    }

    @java.lang.Override public io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            METHOD_RESTART_WORKER,
            asyncUnaryCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.RestartWorkerRequest,
                com.alibaba.maxgraph.proto.Response>(
                  this, METHODID_RESTART_WORKER)))
          .addMethod(
            METHOD_KILL_WORKER,
            asyncUnaryCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.KillWorkerRequest,
                com.alibaba.maxgraph.proto.Response>(
                  this, METHODID_KILL_WORKER)))
          .build();
    }
  }

  /**
   */
  public static final class AppMasterApiStub extends io.grpc.stub.AbstractStub<AppMasterApiStub> {
    private AppMasterApiStub(io.grpc.Channel channel) {
      super(channel);
    }

    private AppMasterApiStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected AppMasterApiStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new AppMasterApiStub(channel, callOptions);
    }

    /**
     */
    public void restartWorker(com.alibaba.maxgraph.proto.RestartWorkerRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.Response> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_RESTART_WORKER, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void killWorker(com.alibaba.maxgraph.proto.KillWorkerRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.Response> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_KILL_WORKER, getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class AppMasterApiBlockingStub extends io.grpc.stub.AbstractStub<AppMasterApiBlockingStub> {
    private AppMasterApiBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private AppMasterApiBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected AppMasterApiBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new AppMasterApiBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.alibaba.maxgraph.proto.Response restartWorker(com.alibaba.maxgraph.proto.RestartWorkerRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_RESTART_WORKER, getCallOptions(), request);
    }

    /**
     */
    public com.alibaba.maxgraph.proto.Response killWorker(com.alibaba.maxgraph.proto.KillWorkerRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_KILL_WORKER, getCallOptions(), request);
    }
  }

  /**
   */
  public static final class AppMasterApiFutureStub extends io.grpc.stub.AbstractStub<AppMasterApiFutureStub> {
    private AppMasterApiFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private AppMasterApiFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected AppMasterApiFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new AppMasterApiFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.alibaba.maxgraph.proto.Response> restartWorker(
        com.alibaba.maxgraph.proto.RestartWorkerRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_RESTART_WORKER, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.alibaba.maxgraph.proto.Response> killWorker(
        com.alibaba.maxgraph.proto.KillWorkerRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_KILL_WORKER, getCallOptions()), request);
    }
  }

  private static final int METHODID_RESTART_WORKER = 0;
  private static final int METHODID_KILL_WORKER = 1;

  private static class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final AppMasterApiImplBase serviceImpl;
    private final int methodId;

    public MethodHandlers(AppMasterApiImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_RESTART_WORKER:
          serviceImpl.restartWorker((com.alibaba.maxgraph.proto.RestartWorkerRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.Response>) responseObserver);
          break;
        case METHODID_KILL_WORKER:
          serviceImpl.killWorker((com.alibaba.maxgraph.proto.KillWorkerRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.Response>) responseObserver);
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
        METHOD_RESTART_WORKER,
        METHOD_KILL_WORKER);
  }

}

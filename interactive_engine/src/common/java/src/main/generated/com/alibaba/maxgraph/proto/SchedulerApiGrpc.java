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
    comments = "Source: scheduler_monitor.proto")
public class SchedulerApiGrpc {

  private SchedulerApiGrpc() {}

  public static final String SERVICE_NAME = "SchedulerApi";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.RestartWorkerReq,
      com.alibaba.maxgraph.proto.Response> METHOD_RESTART_WORKER_MANUALLY =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "SchedulerApi", "restartWorkerManually"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.RestartWorkerReq.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.Response.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.SchedulerEventReq,
      com.alibaba.maxgraph.proto.SchedulerEventResp> METHOD_GET_SCHEDULER_EVENT =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "SchedulerApi", "getSchedulerEvent"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.SchedulerEventReq.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.SchedulerEventResp.getDefaultInstance()));

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static SchedulerApiStub newStub(io.grpc.Channel channel) {
    return new SchedulerApiStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static SchedulerApiBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new SchedulerApiBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary and streaming output calls on the service
   */
  public static SchedulerApiFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new SchedulerApiFutureStub(channel);
  }

  /**
   */
  public static abstract class SchedulerApiImplBase implements io.grpc.BindableService {

    /**
     */
    public void restartWorkerManually(com.alibaba.maxgraph.proto.RestartWorkerReq request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.Response> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_RESTART_WORKER_MANUALLY, responseObserver);
    }

    /**
     */
    public void getSchedulerEvent(com.alibaba.maxgraph.proto.SchedulerEventReq request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.SchedulerEventResp> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_SCHEDULER_EVENT, responseObserver);
    }

    @java.lang.Override public io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            METHOD_RESTART_WORKER_MANUALLY,
            asyncUnaryCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.RestartWorkerReq,
                com.alibaba.maxgraph.proto.Response>(
                  this, METHODID_RESTART_WORKER_MANUALLY)))
          .addMethod(
            METHOD_GET_SCHEDULER_EVENT,
            asyncUnaryCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.SchedulerEventReq,
                com.alibaba.maxgraph.proto.SchedulerEventResp>(
                  this, METHODID_GET_SCHEDULER_EVENT)))
          .build();
    }
  }

  /**
   */
  public static final class SchedulerApiStub extends io.grpc.stub.AbstractStub<SchedulerApiStub> {
    private SchedulerApiStub(io.grpc.Channel channel) {
      super(channel);
    }

    private SchedulerApiStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected SchedulerApiStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new SchedulerApiStub(channel, callOptions);
    }

    /**
     */
    public void restartWorkerManually(com.alibaba.maxgraph.proto.RestartWorkerReq request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.Response> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_RESTART_WORKER_MANUALLY, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getSchedulerEvent(com.alibaba.maxgraph.proto.SchedulerEventReq request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.SchedulerEventResp> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_GET_SCHEDULER_EVENT, getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class SchedulerApiBlockingStub extends io.grpc.stub.AbstractStub<SchedulerApiBlockingStub> {
    private SchedulerApiBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private SchedulerApiBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected SchedulerApiBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new SchedulerApiBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.alibaba.maxgraph.proto.Response restartWorkerManually(com.alibaba.maxgraph.proto.RestartWorkerReq request) {
      return blockingUnaryCall(
          getChannel(), METHOD_RESTART_WORKER_MANUALLY, getCallOptions(), request);
    }

    /**
     */
    public com.alibaba.maxgraph.proto.SchedulerEventResp getSchedulerEvent(com.alibaba.maxgraph.proto.SchedulerEventReq request) {
      return blockingUnaryCall(
          getChannel(), METHOD_GET_SCHEDULER_EVENT, getCallOptions(), request);
    }
  }

  /**
   */
  public static final class SchedulerApiFutureStub extends io.grpc.stub.AbstractStub<SchedulerApiFutureStub> {
    private SchedulerApiFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private SchedulerApiFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected SchedulerApiFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new SchedulerApiFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.alibaba.maxgraph.proto.Response> restartWorkerManually(
        com.alibaba.maxgraph.proto.RestartWorkerReq request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_RESTART_WORKER_MANUALLY, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.alibaba.maxgraph.proto.SchedulerEventResp> getSchedulerEvent(
        com.alibaba.maxgraph.proto.SchedulerEventReq request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_GET_SCHEDULER_EVENT, getCallOptions()), request);
    }
  }

  private static final int METHODID_RESTART_WORKER_MANUALLY = 0;
  private static final int METHODID_GET_SCHEDULER_EVENT = 1;

  private static class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final SchedulerApiImplBase serviceImpl;
    private final int methodId;

    public MethodHandlers(SchedulerApiImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_RESTART_WORKER_MANUALLY:
          serviceImpl.restartWorkerManually((com.alibaba.maxgraph.proto.RestartWorkerReq) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.Response>) responseObserver);
          break;
        case METHODID_GET_SCHEDULER_EVENT:
          serviceImpl.getSchedulerEvent((com.alibaba.maxgraph.proto.SchedulerEventReq) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.SchedulerEventResp>) responseObserver);
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
        METHOD_RESTART_WORKER_MANUALLY,
        METHOD_GET_SCHEDULER_EVENT);
  }

}

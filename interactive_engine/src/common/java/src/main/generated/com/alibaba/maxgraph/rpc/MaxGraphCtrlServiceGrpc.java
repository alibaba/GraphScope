package com.alibaba.maxgraph.rpc;

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
    comments = "Source: gremlin_service.proto")
public class MaxGraphCtrlServiceGrpc {

  private MaxGraphCtrlServiceGrpc() {}

  public static final String SERVICE_NAME = "maxgraph.MaxGraphCtrlService";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.rpc.GremlinService.ShowProcessListRequest,
      com.alibaba.maxgraph.rpc.GremlinService.ShowProcessListResponse> METHOD_SHOW_PROCESS_LIST =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "maxgraph.MaxGraphCtrlService", "showProcessList"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.rpc.GremlinService.ShowProcessListRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.rpc.GremlinService.ShowProcessListResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.rpc.GremlinService.CancelDataflowRequest,
      com.alibaba.maxgraph.rpc.GremlinService.CancelDataflowResponse> METHOD_CANCEL_DATAFLOW =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "maxgraph.MaxGraphCtrlService", "cancelDataflow"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.rpc.GremlinService.CancelDataflowRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.rpc.GremlinService.CancelDataflowResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.rpc.GremlinService.CancelDataflowByFrontRequest,
      com.alibaba.maxgraph.rpc.GremlinService.CancelDataflowResponse> METHOD_CANCEL_DATAFLOW_BY_FRONT =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "maxgraph.MaxGraphCtrlService", "cancelDataflowByFront"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.rpc.GremlinService.CancelDataflowByFrontRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.rpc.GremlinService.CancelDataflowResponse.getDefaultInstance()));

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static MaxGraphCtrlServiceStub newStub(io.grpc.Channel channel) {
    return new MaxGraphCtrlServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static MaxGraphCtrlServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new MaxGraphCtrlServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary and streaming output calls on the service
   */
  public static MaxGraphCtrlServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new MaxGraphCtrlServiceFutureStub(channel);
  }

  /**
   */
  public static abstract class MaxGraphCtrlServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void showProcessList(com.alibaba.maxgraph.rpc.GremlinService.ShowProcessListRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.rpc.GremlinService.ShowProcessListResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_SHOW_PROCESS_LIST, responseObserver);
    }

    /**
     */
    public void cancelDataflow(com.alibaba.maxgraph.rpc.GremlinService.CancelDataflowRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.rpc.GremlinService.CancelDataflowResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_CANCEL_DATAFLOW, responseObserver);
    }

    /**
     */
    public void cancelDataflowByFront(com.alibaba.maxgraph.rpc.GremlinService.CancelDataflowByFrontRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.rpc.GremlinService.CancelDataflowResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_CANCEL_DATAFLOW_BY_FRONT, responseObserver);
    }

    @java.lang.Override public io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            METHOD_SHOW_PROCESS_LIST,
            asyncUnaryCall(
              new MethodHandlers<
                com.alibaba.maxgraph.rpc.GremlinService.ShowProcessListRequest,
                com.alibaba.maxgraph.rpc.GremlinService.ShowProcessListResponse>(
                  this, METHODID_SHOW_PROCESS_LIST)))
          .addMethod(
            METHOD_CANCEL_DATAFLOW,
            asyncUnaryCall(
              new MethodHandlers<
                com.alibaba.maxgraph.rpc.GremlinService.CancelDataflowRequest,
                com.alibaba.maxgraph.rpc.GremlinService.CancelDataflowResponse>(
                  this, METHODID_CANCEL_DATAFLOW)))
          .addMethod(
            METHOD_CANCEL_DATAFLOW_BY_FRONT,
            asyncUnaryCall(
              new MethodHandlers<
                com.alibaba.maxgraph.rpc.GremlinService.CancelDataflowByFrontRequest,
                com.alibaba.maxgraph.rpc.GremlinService.CancelDataflowResponse>(
                  this, METHODID_CANCEL_DATAFLOW_BY_FRONT)))
          .build();
    }
  }

  /**
   */
  public static final class MaxGraphCtrlServiceStub extends io.grpc.stub.AbstractStub<MaxGraphCtrlServiceStub> {
    private MaxGraphCtrlServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private MaxGraphCtrlServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MaxGraphCtrlServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new MaxGraphCtrlServiceStub(channel, callOptions);
    }

    /**
     */
    public void showProcessList(com.alibaba.maxgraph.rpc.GremlinService.ShowProcessListRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.rpc.GremlinService.ShowProcessListResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_SHOW_PROCESS_LIST, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void cancelDataflow(com.alibaba.maxgraph.rpc.GremlinService.CancelDataflowRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.rpc.GremlinService.CancelDataflowResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_CANCEL_DATAFLOW, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void cancelDataflowByFront(com.alibaba.maxgraph.rpc.GremlinService.CancelDataflowByFrontRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.rpc.GremlinService.CancelDataflowResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_CANCEL_DATAFLOW_BY_FRONT, getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class MaxGraphCtrlServiceBlockingStub extends io.grpc.stub.AbstractStub<MaxGraphCtrlServiceBlockingStub> {
    private MaxGraphCtrlServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private MaxGraphCtrlServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MaxGraphCtrlServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new MaxGraphCtrlServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.alibaba.maxgraph.rpc.GremlinService.ShowProcessListResponse showProcessList(com.alibaba.maxgraph.rpc.GremlinService.ShowProcessListRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_SHOW_PROCESS_LIST, getCallOptions(), request);
    }

    /**
     */
    public com.alibaba.maxgraph.rpc.GremlinService.CancelDataflowResponse cancelDataflow(com.alibaba.maxgraph.rpc.GremlinService.CancelDataflowRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_CANCEL_DATAFLOW, getCallOptions(), request);
    }

    /**
     */
    public com.alibaba.maxgraph.rpc.GremlinService.CancelDataflowResponse cancelDataflowByFront(com.alibaba.maxgraph.rpc.GremlinService.CancelDataflowByFrontRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_CANCEL_DATAFLOW_BY_FRONT, getCallOptions(), request);
    }
  }

  /**
   */
  public static final class MaxGraphCtrlServiceFutureStub extends io.grpc.stub.AbstractStub<MaxGraphCtrlServiceFutureStub> {
    private MaxGraphCtrlServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private MaxGraphCtrlServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MaxGraphCtrlServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new MaxGraphCtrlServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.alibaba.maxgraph.rpc.GremlinService.ShowProcessListResponse> showProcessList(
        com.alibaba.maxgraph.rpc.GremlinService.ShowProcessListRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_SHOW_PROCESS_LIST, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.alibaba.maxgraph.rpc.GremlinService.CancelDataflowResponse> cancelDataflow(
        com.alibaba.maxgraph.rpc.GremlinService.CancelDataflowRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_CANCEL_DATAFLOW, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.alibaba.maxgraph.rpc.GremlinService.CancelDataflowResponse> cancelDataflowByFront(
        com.alibaba.maxgraph.rpc.GremlinService.CancelDataflowByFrontRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_CANCEL_DATAFLOW_BY_FRONT, getCallOptions()), request);
    }
  }

  private static final int METHODID_SHOW_PROCESS_LIST = 0;
  private static final int METHODID_CANCEL_DATAFLOW = 1;
  private static final int METHODID_CANCEL_DATAFLOW_BY_FRONT = 2;

  private static class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final MaxGraphCtrlServiceImplBase serviceImpl;
    private final int methodId;

    public MethodHandlers(MaxGraphCtrlServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_SHOW_PROCESS_LIST:
          serviceImpl.showProcessList((com.alibaba.maxgraph.rpc.GremlinService.ShowProcessListRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.rpc.GremlinService.ShowProcessListResponse>) responseObserver);
          break;
        case METHODID_CANCEL_DATAFLOW:
          serviceImpl.cancelDataflow((com.alibaba.maxgraph.rpc.GremlinService.CancelDataflowRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.rpc.GremlinService.CancelDataflowResponse>) responseObserver);
          break;
        case METHODID_CANCEL_DATAFLOW_BY_FRONT:
          serviceImpl.cancelDataflowByFront((com.alibaba.maxgraph.rpc.GremlinService.CancelDataflowByFrontRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.rpc.GremlinService.CancelDataflowResponse>) responseObserver);
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
        METHOD_SHOW_PROCESS_LIST,
        METHOD_CANCEL_DATAFLOW,
        METHOD_CANCEL_DATAFLOW_BY_FRONT);
  }

}

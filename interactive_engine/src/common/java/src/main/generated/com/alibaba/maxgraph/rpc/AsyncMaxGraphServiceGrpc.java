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
public class AsyncMaxGraphServiceGrpc {

  private AsyncMaxGraphServiceGrpc() {}

  public static final String SERVICE_NAME = "maxgraph.AsyncMaxGraphService";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.QueryFlowOuterClass.QueryFlow,
      com.alibaba.maxgraph.Message.QueryResponse> METHOD_ASYNC_QUERY =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING,
          generateFullMethodName(
              "maxgraph.AsyncMaxGraphService", "asyncQuery"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.QueryFlowOuterClass.QueryFlow.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.Message.QueryResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.QueryFlowOuterClass.QueryFlow,
      com.alibaba.maxgraph.Message.OperationResponse> METHOD_ASYNC_PREPARE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "maxgraph.AsyncMaxGraphService", "asyncPrepare"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.QueryFlowOuterClass.QueryFlow.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.Message.OperationResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.QueryFlowOuterClass.Query,
      com.alibaba.maxgraph.Message.QueryResponse> METHOD_ASYNC_QUERY2 =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING,
          generateFullMethodName(
              "maxgraph.AsyncMaxGraphService", "asyncQuery2"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.QueryFlowOuterClass.Query.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.Message.QueryResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.QueryFlowOuterClass.QueryFlow,
      com.alibaba.maxgraph.Message.QueryResponse> METHOD_ASYNC_EXECUTE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING,
          generateFullMethodName(
              "maxgraph.AsyncMaxGraphService", "asyncExecute"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.QueryFlowOuterClass.QueryFlow.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.Message.QueryResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.Message.RemoveDataflowRequest,
      com.alibaba.maxgraph.Message.OperationResponse> METHOD_ASYNC_REMOVE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "maxgraph.AsyncMaxGraphService", "asyncRemove"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.Message.RemoveDataflowRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.Message.OperationResponse.getDefaultInstance()));

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static AsyncMaxGraphServiceStub newStub(io.grpc.Channel channel) {
    return new AsyncMaxGraphServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static AsyncMaxGraphServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new AsyncMaxGraphServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary and streaming output calls on the service
   */
  public static AsyncMaxGraphServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new AsyncMaxGraphServiceFutureStub(channel);
  }

  /**
   */
  public static abstract class AsyncMaxGraphServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void asyncQuery(com.alibaba.maxgraph.QueryFlowOuterClass.QueryFlow request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.Message.QueryResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_ASYNC_QUERY, responseObserver);
    }

    /**
     */
    public void asyncPrepare(com.alibaba.maxgraph.QueryFlowOuterClass.QueryFlow request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.Message.OperationResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_ASYNC_PREPARE, responseObserver);
    }

    /**
     */
    public void asyncQuery2(com.alibaba.maxgraph.QueryFlowOuterClass.Query request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.Message.QueryResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_ASYNC_QUERY2, responseObserver);
    }

    /**
     */
    public void asyncExecute(com.alibaba.maxgraph.QueryFlowOuterClass.QueryFlow request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.Message.QueryResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_ASYNC_EXECUTE, responseObserver);
    }

    /**
     */
    public void asyncRemove(com.alibaba.maxgraph.Message.RemoveDataflowRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.Message.OperationResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_ASYNC_REMOVE, responseObserver);
    }

    @java.lang.Override public io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            METHOD_ASYNC_QUERY,
            asyncServerStreamingCall(
              new MethodHandlers<
                com.alibaba.maxgraph.QueryFlowOuterClass.QueryFlow,
                com.alibaba.maxgraph.Message.QueryResponse>(
                  this, METHODID_ASYNC_QUERY)))
          .addMethod(
            METHOD_ASYNC_PREPARE,
            asyncUnaryCall(
              new MethodHandlers<
                com.alibaba.maxgraph.QueryFlowOuterClass.QueryFlow,
                com.alibaba.maxgraph.Message.OperationResponse>(
                  this, METHODID_ASYNC_PREPARE)))
          .addMethod(
            METHOD_ASYNC_QUERY2,
            asyncServerStreamingCall(
              new MethodHandlers<
                com.alibaba.maxgraph.QueryFlowOuterClass.Query,
                com.alibaba.maxgraph.Message.QueryResponse>(
                  this, METHODID_ASYNC_QUERY2)))
          .addMethod(
            METHOD_ASYNC_EXECUTE,
            asyncServerStreamingCall(
              new MethodHandlers<
                com.alibaba.maxgraph.QueryFlowOuterClass.QueryFlow,
                com.alibaba.maxgraph.Message.QueryResponse>(
                  this, METHODID_ASYNC_EXECUTE)))
          .addMethod(
            METHOD_ASYNC_REMOVE,
            asyncUnaryCall(
              new MethodHandlers<
                com.alibaba.maxgraph.Message.RemoveDataflowRequest,
                com.alibaba.maxgraph.Message.OperationResponse>(
                  this, METHODID_ASYNC_REMOVE)))
          .build();
    }
  }

  /**
   */
  public static final class AsyncMaxGraphServiceStub extends io.grpc.stub.AbstractStub<AsyncMaxGraphServiceStub> {
    private AsyncMaxGraphServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private AsyncMaxGraphServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected AsyncMaxGraphServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new AsyncMaxGraphServiceStub(channel, callOptions);
    }

    /**
     */
    public void asyncQuery(com.alibaba.maxgraph.QueryFlowOuterClass.QueryFlow request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.Message.QueryResponse> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(METHOD_ASYNC_QUERY, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void asyncPrepare(com.alibaba.maxgraph.QueryFlowOuterClass.QueryFlow request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.Message.OperationResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_ASYNC_PREPARE, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void asyncQuery2(com.alibaba.maxgraph.QueryFlowOuterClass.Query request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.Message.QueryResponse> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(METHOD_ASYNC_QUERY2, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void asyncExecute(com.alibaba.maxgraph.QueryFlowOuterClass.QueryFlow request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.Message.QueryResponse> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(METHOD_ASYNC_EXECUTE, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void asyncRemove(com.alibaba.maxgraph.Message.RemoveDataflowRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.Message.OperationResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_ASYNC_REMOVE, getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class AsyncMaxGraphServiceBlockingStub extends io.grpc.stub.AbstractStub<AsyncMaxGraphServiceBlockingStub> {
    private AsyncMaxGraphServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private AsyncMaxGraphServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected AsyncMaxGraphServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new AsyncMaxGraphServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public java.util.Iterator<com.alibaba.maxgraph.Message.QueryResponse> asyncQuery(
        com.alibaba.maxgraph.QueryFlowOuterClass.QueryFlow request) {
      return blockingServerStreamingCall(
          getChannel(), METHOD_ASYNC_QUERY, getCallOptions(), request);
    }

    /**
     */
    public com.alibaba.maxgraph.Message.OperationResponse asyncPrepare(com.alibaba.maxgraph.QueryFlowOuterClass.QueryFlow request) {
      return blockingUnaryCall(
          getChannel(), METHOD_ASYNC_PREPARE, getCallOptions(), request);
    }

    /**
     */
    public java.util.Iterator<com.alibaba.maxgraph.Message.QueryResponse> asyncQuery2(
        com.alibaba.maxgraph.QueryFlowOuterClass.Query request) {
      return blockingServerStreamingCall(
          getChannel(), METHOD_ASYNC_QUERY2, getCallOptions(), request);
    }

    /**
     */
    public java.util.Iterator<com.alibaba.maxgraph.Message.QueryResponse> asyncExecute(
        com.alibaba.maxgraph.QueryFlowOuterClass.QueryFlow request) {
      return blockingServerStreamingCall(
          getChannel(), METHOD_ASYNC_EXECUTE, getCallOptions(), request);
    }

    /**
     */
    public com.alibaba.maxgraph.Message.OperationResponse asyncRemove(com.alibaba.maxgraph.Message.RemoveDataflowRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_ASYNC_REMOVE, getCallOptions(), request);
    }
  }

  /**
   */
  public static final class AsyncMaxGraphServiceFutureStub extends io.grpc.stub.AbstractStub<AsyncMaxGraphServiceFutureStub> {
    private AsyncMaxGraphServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private AsyncMaxGraphServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected AsyncMaxGraphServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new AsyncMaxGraphServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.alibaba.maxgraph.Message.OperationResponse> asyncPrepare(
        com.alibaba.maxgraph.QueryFlowOuterClass.QueryFlow request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_ASYNC_PREPARE, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.alibaba.maxgraph.Message.OperationResponse> asyncRemove(
        com.alibaba.maxgraph.Message.RemoveDataflowRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_ASYNC_REMOVE, getCallOptions()), request);
    }
  }

  private static final int METHODID_ASYNC_QUERY = 0;
  private static final int METHODID_ASYNC_PREPARE = 1;
  private static final int METHODID_ASYNC_QUERY2 = 2;
  private static final int METHODID_ASYNC_EXECUTE = 3;
  private static final int METHODID_ASYNC_REMOVE = 4;

  private static class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final AsyncMaxGraphServiceImplBase serviceImpl;
    private final int methodId;

    public MethodHandlers(AsyncMaxGraphServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_ASYNC_QUERY:
          serviceImpl.asyncQuery((com.alibaba.maxgraph.QueryFlowOuterClass.QueryFlow) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.Message.QueryResponse>) responseObserver);
          break;
        case METHODID_ASYNC_PREPARE:
          serviceImpl.asyncPrepare((com.alibaba.maxgraph.QueryFlowOuterClass.QueryFlow) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.Message.OperationResponse>) responseObserver);
          break;
        case METHODID_ASYNC_QUERY2:
          serviceImpl.asyncQuery2((com.alibaba.maxgraph.QueryFlowOuterClass.Query) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.Message.QueryResponse>) responseObserver);
          break;
        case METHODID_ASYNC_EXECUTE:
          serviceImpl.asyncExecute((com.alibaba.maxgraph.QueryFlowOuterClass.QueryFlow) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.Message.QueryResponse>) responseObserver);
          break;
        case METHODID_ASYNC_REMOVE:
          serviceImpl.asyncRemove((com.alibaba.maxgraph.Message.RemoveDataflowRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.Message.OperationResponse>) responseObserver);
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
        METHOD_ASYNC_QUERY,
        METHOD_ASYNC_PREPARE,
        METHOD_ASYNC_QUERY2,
        METHOD_ASYNC_EXECUTE,
        METHOD_ASYNC_REMOVE);
  }

}

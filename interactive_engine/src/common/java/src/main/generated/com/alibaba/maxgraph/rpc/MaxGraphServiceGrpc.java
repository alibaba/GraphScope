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
public class MaxGraphServiceGrpc {

  private MaxGraphServiceGrpc() {}

  public static final String SERVICE_NAME = "maxgraph.MaxGraphService";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.QueryFlowOuterClass.QueryFlow,
      com.alibaba.maxgraph.Message.QueryResponse> METHOD_QUERY =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING,
          generateFullMethodName(
              "maxgraph.MaxGraphService", "query"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.QueryFlowOuterClass.QueryFlow.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.Message.QueryResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.QueryFlowOuterClass.QueryFlow,
      com.alibaba.maxgraph.Message.OperationResponse> METHOD_PREPARE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "maxgraph.MaxGraphService", "prepare"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.QueryFlowOuterClass.QueryFlow.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.Message.OperationResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.QueryFlowOuterClass.Query,
      com.alibaba.maxgraph.Message.QueryResponse> METHOD_QUERY2 =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING,
          generateFullMethodName(
              "maxgraph.MaxGraphService", "query2"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.QueryFlowOuterClass.Query.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.Message.QueryResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.QueryFlowOuterClass.QueryFlow,
      com.alibaba.maxgraph.Message.QueryResponse> METHOD_EXECUTE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING,
          generateFullMethodName(
              "maxgraph.MaxGraphService", "execute"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.QueryFlowOuterClass.QueryFlow.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.Message.QueryResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.Message.RemoveDataflowRequest,
      com.alibaba.maxgraph.Message.OperationResponse> METHOD_REMOVE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "maxgraph.MaxGraphService", "remove"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.Message.RemoveDataflowRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.Message.OperationResponse.getDefaultInstance()));

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static MaxGraphServiceStub newStub(io.grpc.Channel channel) {
    return new MaxGraphServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static MaxGraphServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new MaxGraphServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary and streaming output calls on the service
   */
  public static MaxGraphServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new MaxGraphServiceFutureStub(channel);
  }

  /**
   */
  public static abstract class MaxGraphServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void query(com.alibaba.maxgraph.QueryFlowOuterClass.QueryFlow request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.Message.QueryResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_QUERY, responseObserver);
    }

    /**
     */
    public void prepare(com.alibaba.maxgraph.QueryFlowOuterClass.QueryFlow request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.Message.OperationResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_PREPARE, responseObserver);
    }

    /**
     */
    public void query2(com.alibaba.maxgraph.QueryFlowOuterClass.Query request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.Message.QueryResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_QUERY2, responseObserver);
    }

    /**
     */
    public void execute(com.alibaba.maxgraph.QueryFlowOuterClass.QueryFlow request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.Message.QueryResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_EXECUTE, responseObserver);
    }

    /**
     */
    public void remove(com.alibaba.maxgraph.Message.RemoveDataflowRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.Message.OperationResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_REMOVE, responseObserver);
    }

    @java.lang.Override public io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            METHOD_QUERY,
            asyncServerStreamingCall(
              new MethodHandlers<
                com.alibaba.maxgraph.QueryFlowOuterClass.QueryFlow,
                com.alibaba.maxgraph.Message.QueryResponse>(
                  this, METHODID_QUERY)))
          .addMethod(
            METHOD_PREPARE,
            asyncUnaryCall(
              new MethodHandlers<
                com.alibaba.maxgraph.QueryFlowOuterClass.QueryFlow,
                com.alibaba.maxgraph.Message.OperationResponse>(
                  this, METHODID_PREPARE)))
          .addMethod(
            METHOD_QUERY2,
            asyncServerStreamingCall(
              new MethodHandlers<
                com.alibaba.maxgraph.QueryFlowOuterClass.Query,
                com.alibaba.maxgraph.Message.QueryResponse>(
                  this, METHODID_QUERY2)))
          .addMethod(
            METHOD_EXECUTE,
            asyncServerStreamingCall(
              new MethodHandlers<
                com.alibaba.maxgraph.QueryFlowOuterClass.QueryFlow,
                com.alibaba.maxgraph.Message.QueryResponse>(
                  this, METHODID_EXECUTE)))
          .addMethod(
            METHOD_REMOVE,
            asyncUnaryCall(
              new MethodHandlers<
                com.alibaba.maxgraph.Message.RemoveDataflowRequest,
                com.alibaba.maxgraph.Message.OperationResponse>(
                  this, METHODID_REMOVE)))
          .build();
    }
  }

  /**
   */
  public static final class MaxGraphServiceStub extends io.grpc.stub.AbstractStub<MaxGraphServiceStub> {
    private MaxGraphServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private MaxGraphServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MaxGraphServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new MaxGraphServiceStub(channel, callOptions);
    }

    /**
     */
    public void query(com.alibaba.maxgraph.QueryFlowOuterClass.QueryFlow request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.Message.QueryResponse> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(METHOD_QUERY, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void prepare(com.alibaba.maxgraph.QueryFlowOuterClass.QueryFlow request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.Message.OperationResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_PREPARE, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void query2(com.alibaba.maxgraph.QueryFlowOuterClass.Query request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.Message.QueryResponse> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(METHOD_QUERY2, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void execute(com.alibaba.maxgraph.QueryFlowOuterClass.QueryFlow request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.Message.QueryResponse> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(METHOD_EXECUTE, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void remove(com.alibaba.maxgraph.Message.RemoveDataflowRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.Message.OperationResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_REMOVE, getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class MaxGraphServiceBlockingStub extends io.grpc.stub.AbstractStub<MaxGraphServiceBlockingStub> {
    private MaxGraphServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private MaxGraphServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MaxGraphServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new MaxGraphServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public java.util.Iterator<com.alibaba.maxgraph.Message.QueryResponse> query(
        com.alibaba.maxgraph.QueryFlowOuterClass.QueryFlow request) {
      return blockingServerStreamingCall(
          getChannel(), METHOD_QUERY, getCallOptions(), request);
    }

    /**
     */
    public com.alibaba.maxgraph.Message.OperationResponse prepare(com.alibaba.maxgraph.QueryFlowOuterClass.QueryFlow request) {
      return blockingUnaryCall(
          getChannel(), METHOD_PREPARE, getCallOptions(), request);
    }

    /**
     */
    public java.util.Iterator<com.alibaba.maxgraph.Message.QueryResponse> query2(
        com.alibaba.maxgraph.QueryFlowOuterClass.Query request) {
      return blockingServerStreamingCall(
          getChannel(), METHOD_QUERY2, getCallOptions(), request);
    }

    /**
     */
    public java.util.Iterator<com.alibaba.maxgraph.Message.QueryResponse> execute(
        com.alibaba.maxgraph.QueryFlowOuterClass.QueryFlow request) {
      return blockingServerStreamingCall(
          getChannel(), METHOD_EXECUTE, getCallOptions(), request);
    }

    /**
     */
    public com.alibaba.maxgraph.Message.OperationResponse remove(com.alibaba.maxgraph.Message.RemoveDataflowRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_REMOVE, getCallOptions(), request);
    }
  }

  /**
   */
  public static final class MaxGraphServiceFutureStub extends io.grpc.stub.AbstractStub<MaxGraphServiceFutureStub> {
    private MaxGraphServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private MaxGraphServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MaxGraphServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new MaxGraphServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.alibaba.maxgraph.Message.OperationResponse> prepare(
        com.alibaba.maxgraph.QueryFlowOuterClass.QueryFlow request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_PREPARE, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.alibaba.maxgraph.Message.OperationResponse> remove(
        com.alibaba.maxgraph.Message.RemoveDataflowRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_REMOVE, getCallOptions()), request);
    }
  }

  private static final int METHODID_QUERY = 0;
  private static final int METHODID_PREPARE = 1;
  private static final int METHODID_QUERY2 = 2;
  private static final int METHODID_EXECUTE = 3;
  private static final int METHODID_REMOVE = 4;

  private static class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final MaxGraphServiceImplBase serviceImpl;
    private final int methodId;

    public MethodHandlers(MaxGraphServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_QUERY:
          serviceImpl.query((com.alibaba.maxgraph.QueryFlowOuterClass.QueryFlow) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.Message.QueryResponse>) responseObserver);
          break;
        case METHODID_PREPARE:
          serviceImpl.prepare((com.alibaba.maxgraph.QueryFlowOuterClass.QueryFlow) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.Message.OperationResponse>) responseObserver);
          break;
        case METHODID_QUERY2:
          serviceImpl.query2((com.alibaba.maxgraph.QueryFlowOuterClass.Query) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.Message.QueryResponse>) responseObserver);
          break;
        case METHODID_EXECUTE:
          serviceImpl.execute((com.alibaba.maxgraph.QueryFlowOuterClass.QueryFlow) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.Message.QueryResponse>) responseObserver);
          break;
        case METHODID_REMOVE:
          serviceImpl.remove((com.alibaba.maxgraph.Message.RemoveDataflowRequest) request,
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
        METHOD_QUERY,
        METHOD_PREPARE,
        METHOD_QUERY2,
        METHOD_EXECUTE,
        METHOD_REMOVE);
  }

}

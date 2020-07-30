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
    comments = "Source: lambda_service.proto")
public class LambdaServiceGrpc {

  private LambdaServiceGrpc() {}

  public static final String SERVICE_NAME = "maxgraph.LambdaService";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaBase,
      com.alibaba.maxgraph.Message.OperationResponse> METHOD_PREPARE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "maxgraph.LambdaService", "prepare"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaBase.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.Message.OperationResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaBase,
      com.alibaba.maxgraph.Message.OperationResponse> METHOD_REMOVE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "maxgraph.LambdaService", "remove"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaBase.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.Message.OperationResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaData,
      com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaResult> METHOD_FILTER =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "maxgraph.LambdaService", "filter"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaData.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaResult.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaData,
      com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaResult> METHOD_MAP =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "maxgraph.LambdaService", "map"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaData.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaResult.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaData,
      com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaResult> METHOD_FLATMAP =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "maxgraph.LambdaService", "flatmap"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaData.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaResult.getDefaultInstance()));

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static LambdaServiceStub newStub(io.grpc.Channel channel) {
    return new LambdaServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static LambdaServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new LambdaServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary and streaming output calls on the service
   */
  public static LambdaServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new LambdaServiceFutureStub(channel);
  }

  /**
   */
  public static abstract class LambdaServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void prepare(com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaBase request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.Message.OperationResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_PREPARE, responseObserver);
    }

    /**
     */
    public void remove(com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaBase request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.Message.OperationResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_REMOVE, responseObserver);
    }

    /**
     */
    public void filter(com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaData request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaResult> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_FILTER, responseObserver);
    }

    /**
     */
    public void map(com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaData request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaResult> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_MAP, responseObserver);
    }

    /**
     */
    public void flatmap(com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaData request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaResult> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_FLATMAP, responseObserver);
    }

    @java.lang.Override public io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            METHOD_PREPARE,
            asyncUnaryCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaBase,
                com.alibaba.maxgraph.Message.OperationResponse>(
                  this, METHODID_PREPARE)))
          .addMethod(
            METHOD_REMOVE,
            asyncUnaryCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaBase,
                com.alibaba.maxgraph.Message.OperationResponse>(
                  this, METHODID_REMOVE)))
          .addMethod(
            METHOD_FILTER,
            asyncUnaryCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaData,
                com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaResult>(
                  this, METHODID_FILTER)))
          .addMethod(
            METHOD_MAP,
            asyncUnaryCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaData,
                com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaResult>(
                  this, METHODID_MAP)))
          .addMethod(
            METHOD_FLATMAP,
            asyncUnaryCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaData,
                com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaResult>(
                  this, METHODID_FLATMAP)))
          .build();
    }
  }

  /**
   */
  public static final class LambdaServiceStub extends io.grpc.stub.AbstractStub<LambdaServiceStub> {
    private LambdaServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private LambdaServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected LambdaServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new LambdaServiceStub(channel, callOptions);
    }

    /**
     */
    public void prepare(com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaBase request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.Message.OperationResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_PREPARE, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void remove(com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaBase request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.Message.OperationResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_REMOVE, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void filter(com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaData request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaResult> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_FILTER, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void map(com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaData request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaResult> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_MAP, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void flatmap(com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaData request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaResult> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_FLATMAP, getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class LambdaServiceBlockingStub extends io.grpc.stub.AbstractStub<LambdaServiceBlockingStub> {
    private LambdaServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private LambdaServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected LambdaServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new LambdaServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.alibaba.maxgraph.Message.OperationResponse prepare(com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaBase request) {
      return blockingUnaryCall(
          getChannel(), METHOD_PREPARE, getCallOptions(), request);
    }

    /**
     */
    public com.alibaba.maxgraph.Message.OperationResponse remove(com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaBase request) {
      return blockingUnaryCall(
          getChannel(), METHOD_REMOVE, getCallOptions(), request);
    }

    /**
     */
    public com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaResult filter(com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaData request) {
      return blockingUnaryCall(
          getChannel(), METHOD_FILTER, getCallOptions(), request);
    }

    /**
     */
    public com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaResult map(com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaData request) {
      return blockingUnaryCall(
          getChannel(), METHOD_MAP, getCallOptions(), request);
    }

    /**
     */
    public com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaResult flatmap(com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaData request) {
      return blockingUnaryCall(
          getChannel(), METHOD_FLATMAP, getCallOptions(), request);
    }
  }

  /**
   */
  public static final class LambdaServiceFutureStub extends io.grpc.stub.AbstractStub<LambdaServiceFutureStub> {
    private LambdaServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private LambdaServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected LambdaServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new LambdaServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.alibaba.maxgraph.Message.OperationResponse> prepare(
        com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaBase request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_PREPARE, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.alibaba.maxgraph.Message.OperationResponse> remove(
        com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaBase request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_REMOVE, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaResult> filter(
        com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaData request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_FILTER, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaResult> map(
        com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaData request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_MAP, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaResult> flatmap(
        com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaData request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_FLATMAP, getCallOptions()), request);
    }
  }

  private static final int METHODID_PREPARE = 0;
  private static final int METHODID_REMOVE = 1;
  private static final int METHODID_FILTER = 2;
  private static final int METHODID_MAP = 3;
  private static final int METHODID_FLATMAP = 4;

  private static class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final LambdaServiceImplBase serviceImpl;
    private final int methodId;

    public MethodHandlers(LambdaServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_PREPARE:
          serviceImpl.prepare((com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaBase) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.Message.OperationResponse>) responseObserver);
          break;
        case METHODID_REMOVE:
          serviceImpl.remove((com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaBase) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.Message.OperationResponse>) responseObserver);
          break;
        case METHODID_FILTER:
          serviceImpl.filter((com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaData) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaResult>) responseObserver);
          break;
        case METHODID_MAP:
          serviceImpl.map((com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaData) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaResult>) responseObserver);
          break;
        case METHODID_FLATMAP:
          serviceImpl.flatmap((com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaData) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.LambdaServiceOuterClass.LambdaResult>) responseObserver);
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
        METHOD_PREPARE,
        METHOD_REMOVE,
        METHOD_FILTER,
        METHOD_MAP,
        METHOD_FLATMAP);
  }

}

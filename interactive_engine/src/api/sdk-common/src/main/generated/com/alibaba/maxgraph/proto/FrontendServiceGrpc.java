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
    comments = "Source: sdk/client_frontend_protocol.proto")
public class FrontendServiceGrpc {

  private FrontendServiceGrpc() {}

  public static final String SERVICE_NAME = "FrontendService";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.QueryRequest,
      com.alibaba.maxgraph.proto.QueryResponse> METHOD_EXECUTE_QUERY =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "FrontendService", "executeQuery"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.QueryRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.QueryResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.PrepareRequest,
      com.alibaba.maxgraph.proto.Response> METHOD_PREPARE_QUERY =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "FrontendService", "prepareQuery"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.PrepareRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.Response.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.PrepareRequest,
      com.alibaba.maxgraph.proto.Response> METHOD_REMOVE_PREPARE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "FrontendService", "removePrepare"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.PrepareRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.Response.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.Empty,
      com.alibaba.maxgraph.proto.PrepareNames> METHOD_LIST_PREPARE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "FrontendService", "listPrepare"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.Empty.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.PrepareNames.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.MetricInfoRequest,
      com.alibaba.maxgraph.proto.MetricInfoResp> METHOD_GET_REAL_TIME_METRIC =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "FrontendService", "getRealTimeMetric"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.MetricInfoRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.MetricInfoResp.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.Request,
      com.alibaba.maxgraph.proto.AllMetricsInfoResp> METHOD_GET_ALL_REAL_TIME_METRICS =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "FrontendService", "getAllRealTimeMetrics"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.Request.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.AllMetricsInfoResp.getDefaultInstance()));

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static FrontendServiceStub newStub(io.grpc.Channel channel) {
    return new FrontendServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static FrontendServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new FrontendServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary and streaming output calls on the service
   */
  public static FrontendServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new FrontendServiceFutureStub(channel);
  }

  /**
   */
  public static abstract class FrontendServiceImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     * query
     * </pre>
     */
    public void executeQuery(com.alibaba.maxgraph.proto.QueryRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.QueryResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_EXECUTE_QUERY, responseObserver);
    }

    /**
     * <pre>
     * Prepare statement api .
     * </pre>
     */
    public void prepareQuery(com.alibaba.maxgraph.proto.PrepareRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.Response> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_PREPARE_QUERY, responseObserver);
    }

    /**
     */
    public void removePrepare(com.alibaba.maxgraph.proto.PrepareRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.Response> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_REMOVE_PREPARE, responseObserver);
    }

    /**
     */
    public void listPrepare(com.alibaba.maxgraph.proto.Empty request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.PrepareNames> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_LIST_PREPARE, responseObserver);
    }

    /**
     */
    public void getRealTimeMetric(com.alibaba.maxgraph.proto.MetricInfoRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.MetricInfoResp> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_REAL_TIME_METRIC, responseObserver);
    }

    /**
     */
    public void getAllRealTimeMetrics(com.alibaba.maxgraph.proto.Request request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.AllMetricsInfoResp> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_ALL_REAL_TIME_METRICS, responseObserver);
    }

    @java.lang.Override public io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            METHOD_EXECUTE_QUERY,
            asyncUnaryCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.QueryRequest,
                com.alibaba.maxgraph.proto.QueryResponse>(
                  this, METHODID_EXECUTE_QUERY)))
          .addMethod(
            METHOD_PREPARE_QUERY,
            asyncUnaryCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.PrepareRequest,
                com.alibaba.maxgraph.proto.Response>(
                  this, METHODID_PREPARE_QUERY)))
          .addMethod(
            METHOD_REMOVE_PREPARE,
            asyncUnaryCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.PrepareRequest,
                com.alibaba.maxgraph.proto.Response>(
                  this, METHODID_REMOVE_PREPARE)))
          .addMethod(
            METHOD_LIST_PREPARE,
            asyncUnaryCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.Empty,
                com.alibaba.maxgraph.proto.PrepareNames>(
                  this, METHODID_LIST_PREPARE)))
          .addMethod(
            METHOD_GET_REAL_TIME_METRIC,
            asyncUnaryCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.MetricInfoRequest,
                com.alibaba.maxgraph.proto.MetricInfoResp>(
                  this, METHODID_GET_REAL_TIME_METRIC)))
          .addMethod(
            METHOD_GET_ALL_REAL_TIME_METRICS,
            asyncUnaryCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.Request,
                com.alibaba.maxgraph.proto.AllMetricsInfoResp>(
                  this, METHODID_GET_ALL_REAL_TIME_METRICS)))
          .build();
    }
  }

  /**
   */
  public static final class FrontendServiceStub extends io.grpc.stub.AbstractStub<FrontendServiceStub> {
    private FrontendServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private FrontendServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected FrontendServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new FrontendServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * query
     * </pre>
     */
    public void executeQuery(com.alibaba.maxgraph.proto.QueryRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.QueryResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_EXECUTE_QUERY, getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Prepare statement api .
     * </pre>
     */
    public void prepareQuery(com.alibaba.maxgraph.proto.PrepareRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.Response> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_PREPARE_QUERY, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void removePrepare(com.alibaba.maxgraph.proto.PrepareRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.Response> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_REMOVE_PREPARE, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void listPrepare(com.alibaba.maxgraph.proto.Empty request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.PrepareNames> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_LIST_PREPARE, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getRealTimeMetric(com.alibaba.maxgraph.proto.MetricInfoRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.MetricInfoResp> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_GET_REAL_TIME_METRIC, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getAllRealTimeMetrics(com.alibaba.maxgraph.proto.Request request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.AllMetricsInfoResp> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_GET_ALL_REAL_TIME_METRICS, getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class FrontendServiceBlockingStub extends io.grpc.stub.AbstractStub<FrontendServiceBlockingStub> {
    private FrontendServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private FrontendServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected FrontendServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new FrontendServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * query
     * </pre>
     */
    public com.alibaba.maxgraph.proto.QueryResponse executeQuery(com.alibaba.maxgraph.proto.QueryRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_EXECUTE_QUERY, getCallOptions(), request);
    }

    /**
     * <pre>
     * Prepare statement api .
     * </pre>
     */
    public com.alibaba.maxgraph.proto.Response prepareQuery(com.alibaba.maxgraph.proto.PrepareRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_PREPARE_QUERY, getCallOptions(), request);
    }

    /**
     */
    public com.alibaba.maxgraph.proto.Response removePrepare(com.alibaba.maxgraph.proto.PrepareRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_REMOVE_PREPARE, getCallOptions(), request);
    }

    /**
     */
    public com.alibaba.maxgraph.proto.PrepareNames listPrepare(com.alibaba.maxgraph.proto.Empty request) {
      return blockingUnaryCall(
          getChannel(), METHOD_LIST_PREPARE, getCallOptions(), request);
    }

    /**
     */
    public com.alibaba.maxgraph.proto.MetricInfoResp getRealTimeMetric(com.alibaba.maxgraph.proto.MetricInfoRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_GET_REAL_TIME_METRIC, getCallOptions(), request);
    }

    /**
     */
    public com.alibaba.maxgraph.proto.AllMetricsInfoResp getAllRealTimeMetrics(com.alibaba.maxgraph.proto.Request request) {
      return blockingUnaryCall(
          getChannel(), METHOD_GET_ALL_REAL_TIME_METRICS, getCallOptions(), request);
    }
  }

  /**
   */
  public static final class FrontendServiceFutureStub extends io.grpc.stub.AbstractStub<FrontendServiceFutureStub> {
    private FrontendServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private FrontendServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected FrontendServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new FrontendServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * query
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.alibaba.maxgraph.proto.QueryResponse> executeQuery(
        com.alibaba.maxgraph.proto.QueryRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_EXECUTE_QUERY, getCallOptions()), request);
    }

    /**
     * <pre>
     * Prepare statement api .
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.alibaba.maxgraph.proto.Response> prepareQuery(
        com.alibaba.maxgraph.proto.PrepareRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_PREPARE_QUERY, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.alibaba.maxgraph.proto.Response> removePrepare(
        com.alibaba.maxgraph.proto.PrepareRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_REMOVE_PREPARE, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.alibaba.maxgraph.proto.PrepareNames> listPrepare(
        com.alibaba.maxgraph.proto.Empty request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_LIST_PREPARE, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.alibaba.maxgraph.proto.MetricInfoResp> getRealTimeMetric(
        com.alibaba.maxgraph.proto.MetricInfoRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_GET_REAL_TIME_METRIC, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.alibaba.maxgraph.proto.AllMetricsInfoResp> getAllRealTimeMetrics(
        com.alibaba.maxgraph.proto.Request request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_GET_ALL_REAL_TIME_METRICS, getCallOptions()), request);
    }
  }

  private static final int METHODID_EXECUTE_QUERY = 0;
  private static final int METHODID_PREPARE_QUERY = 1;
  private static final int METHODID_REMOVE_PREPARE = 2;
  private static final int METHODID_LIST_PREPARE = 3;
  private static final int METHODID_GET_REAL_TIME_METRIC = 4;
  private static final int METHODID_GET_ALL_REAL_TIME_METRICS = 5;

  private static class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final FrontendServiceImplBase serviceImpl;
    private final int methodId;

    public MethodHandlers(FrontendServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_EXECUTE_QUERY:
          serviceImpl.executeQuery((com.alibaba.maxgraph.proto.QueryRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.QueryResponse>) responseObserver);
          break;
        case METHODID_PREPARE_QUERY:
          serviceImpl.prepareQuery((com.alibaba.maxgraph.proto.PrepareRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.Response>) responseObserver);
          break;
        case METHODID_REMOVE_PREPARE:
          serviceImpl.removePrepare((com.alibaba.maxgraph.proto.PrepareRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.Response>) responseObserver);
          break;
        case METHODID_LIST_PREPARE:
          serviceImpl.listPrepare((com.alibaba.maxgraph.proto.Empty) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.PrepareNames>) responseObserver);
          break;
        case METHODID_GET_REAL_TIME_METRIC:
          serviceImpl.getRealTimeMetric((com.alibaba.maxgraph.proto.MetricInfoRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.MetricInfoResp>) responseObserver);
          break;
        case METHODID_GET_ALL_REAL_TIME_METRICS:
          serviceImpl.getAllRealTimeMetrics((com.alibaba.maxgraph.proto.Request) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.AllMetricsInfoResp>) responseObserver);
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
        METHOD_EXECUTE_QUERY,
        METHOD_PREPARE_QUERY,
        METHOD_REMOVE_PREPARE,
        METHOD_LIST_PREPARE,
        METHOD_GET_REAL_TIME_METRIC,
        METHOD_GET_ALL_REAL_TIME_METRICS);
  }

}

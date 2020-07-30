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
    comments = "Source: data.proto")
public class ServerDataApiGrpc {

  private ServerDataApiGrpc() {}

  public static final String SERVICE_NAME = "ServerDataApi";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.Request,
      com.alibaba.maxgraph.proto.InstanceInfoResp> METHOD_GET_INSTANCE_INFO =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "ServerDataApi", "getInstanceInfo"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.Request.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.InstanceInfoResp.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.ServerHBReq,
      com.alibaba.maxgraph.proto.ServerHBResp> METHOD_HEARTBEAT =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "ServerDataApi", "heartbeat"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.ServerHBReq.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.ServerHBResp.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.RuntimeEnv,
      com.alibaba.maxgraph.proto.RuntimeEnvList> METHOD_UPDATE_RUNTIME_ENV =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "ServerDataApi", "updateRuntimeEnv"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.RuntimeEnv.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.RuntimeEnvList.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.Empty,
      com.alibaba.maxgraph.proto.Empty> METHOD_RESET_RUNTIME_ENV =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "ServerDataApi", "resetRuntimeEnv"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.Empty.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.Empty.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.SimpleServerHBReq,
      com.alibaba.maxgraph.proto.SimpleServerHBResponse> METHOD_SIMPLE_HEARTBEAT =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "ServerDataApi", "simpleHeartbeat"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.SimpleServerHBReq.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.SimpleServerHBResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.Empty,
      com.alibaba.maxgraph.proto.RuntimeGroupStatusResp> METHOD_GET_RUNTIME_GROUP_STATUS =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "ServerDataApi", "getRuntimeGroupStatus"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.Empty.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.RuntimeGroupStatusResp.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.ServerIdAliveIdProto,
      com.alibaba.maxgraph.proto.DataPathStatusResponse> METHOD_IS_DATA_PATH_IN_USE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "ServerDataApi", "isDataPathInUse"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.ServerIdAliveIdProto.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.DataPathStatusResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.Request,
      com.alibaba.maxgraph.proto.RoutingServerInfoResp> METHOD_GET_WORKER_INFO_AND_ROUTING_SERVER_LIST =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "ServerDataApi", "getWorkerInfoAndRoutingServerList"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.Request.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.RoutingServerInfoResp.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.MetricInfoRequest,
      com.alibaba.maxgraph.proto.MetricInfoResp> METHOD_GET_REAL_TIME_METRIC =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "ServerDataApi", "getRealTimeMetric"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.MetricInfoRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.MetricInfoResp.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.Request,
      com.alibaba.maxgraph.proto.AllMetricsInfoResp> METHOD_GET_ALL_REAL_TIME_METRICS =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "ServerDataApi", "getAllRealTimeMetrics"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.Request.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.AllMetricsInfoResp.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.GetExecutorAliveIdRequest,
      com.alibaba.maxgraph.proto.GetExecutorAliveIdResponse> METHOD_GET_EXECUTOR_ALIVE_ID =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "ServerDataApi", "getExecutorAliveId"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.GetExecutorAliveIdRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.GetExecutorAliveIdResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.GetPartitionAssignmentRequest,
      com.alibaba.maxgraph.proto.GetPartitionAssignmentResponse> METHOD_GET_PARTITION_ASSIGNMENT =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "ServerDataApi", "getPartitionAssignment"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.GetPartitionAssignmentRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.GetPartitionAssignmentResponse.getDefaultInstance()));

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static ServerDataApiStub newStub(io.grpc.Channel channel) {
    return new ServerDataApiStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static ServerDataApiBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new ServerDataApiBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary and streaming output calls on the service
   */
  public static ServerDataApiFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new ServerDataApiFutureStub(channel);
  }

  /**
   */
  public static abstract class ServerDataApiImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     * get Server status
     * </pre>
     */
    public void getInstanceInfo(com.alibaba.maxgraph.proto.Request request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.InstanceInfoResp> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_INSTANCE_INFO, responseObserver);
    }

    /**
     * <pre>
     * heartbeat
     * </pre>
     */
    public void heartbeat(com.alibaba.maxgraph.proto.ServerHBReq request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.ServerHBResp> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_HEARTBEAT, responseObserver);
    }

    /**
     * <pre>
     * update runtime env
     * </pre>
     */
    public void updateRuntimeEnv(com.alibaba.maxgraph.proto.RuntimeEnv request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.RuntimeEnvList> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_UPDATE_RUNTIME_ENV, responseObserver);
    }

    /**
     * <pre>
     * reset runtime envs
     * </pre>
     */
    public void resetRuntimeEnv(com.alibaba.maxgraph.proto.Empty request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.Empty> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_RESET_RUNTIME_ENV, responseObserver);
    }

    /**
     */
    public void simpleHeartbeat(com.alibaba.maxgraph.proto.SimpleServerHBReq request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.SimpleServerHBResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_SIMPLE_HEARTBEAT, responseObserver);
    }

    /**
     */
    public void getRuntimeGroupStatus(com.alibaba.maxgraph.proto.Empty request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.RuntimeGroupStatusResp> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_RUNTIME_GROUP_STATUS, responseObserver);
    }

    /**
     * <pre>
     * query data path is stale or not
     * </pre>
     */
    public void isDataPathInUse(com.alibaba.maxgraph.proto.ServerIdAliveIdProto request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.DataPathStatusResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_IS_DATA_PATH_IN_USE, responseObserver);
    }

    /**
     */
    public void getWorkerInfoAndRoutingServerList(com.alibaba.maxgraph.proto.Request request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.RoutingServerInfoResp> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_WORKER_INFO_AND_ROUTING_SERVER_LIST, responseObserver);
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

    /**
     */
    public void getExecutorAliveId(com.alibaba.maxgraph.proto.GetExecutorAliveIdRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.GetExecutorAliveIdResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_EXECUTOR_ALIVE_ID, responseObserver);
    }

    /**
     */
    public void getPartitionAssignment(com.alibaba.maxgraph.proto.GetPartitionAssignmentRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.GetPartitionAssignmentResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_PARTITION_ASSIGNMENT, responseObserver);
    }

    @java.lang.Override public io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            METHOD_GET_INSTANCE_INFO,
            asyncUnaryCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.Request,
                com.alibaba.maxgraph.proto.InstanceInfoResp>(
                  this, METHODID_GET_INSTANCE_INFO)))
          .addMethod(
            METHOD_HEARTBEAT,
            asyncUnaryCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.ServerHBReq,
                com.alibaba.maxgraph.proto.ServerHBResp>(
                  this, METHODID_HEARTBEAT)))
          .addMethod(
            METHOD_UPDATE_RUNTIME_ENV,
            asyncUnaryCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.RuntimeEnv,
                com.alibaba.maxgraph.proto.RuntimeEnvList>(
                  this, METHODID_UPDATE_RUNTIME_ENV)))
          .addMethod(
            METHOD_RESET_RUNTIME_ENV,
            asyncUnaryCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.Empty,
                com.alibaba.maxgraph.proto.Empty>(
                  this, METHODID_RESET_RUNTIME_ENV)))
          .addMethod(
            METHOD_SIMPLE_HEARTBEAT,
            asyncUnaryCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.SimpleServerHBReq,
                com.alibaba.maxgraph.proto.SimpleServerHBResponse>(
                  this, METHODID_SIMPLE_HEARTBEAT)))
          .addMethod(
            METHOD_GET_RUNTIME_GROUP_STATUS,
            asyncUnaryCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.Empty,
                com.alibaba.maxgraph.proto.RuntimeGroupStatusResp>(
                  this, METHODID_GET_RUNTIME_GROUP_STATUS)))
          .addMethod(
            METHOD_IS_DATA_PATH_IN_USE,
            asyncUnaryCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.ServerIdAliveIdProto,
                com.alibaba.maxgraph.proto.DataPathStatusResponse>(
                  this, METHODID_IS_DATA_PATH_IN_USE)))
          .addMethod(
            METHOD_GET_WORKER_INFO_AND_ROUTING_SERVER_LIST,
            asyncUnaryCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.Request,
                com.alibaba.maxgraph.proto.RoutingServerInfoResp>(
                  this, METHODID_GET_WORKER_INFO_AND_ROUTING_SERVER_LIST)))
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
          .addMethod(
            METHOD_GET_EXECUTOR_ALIVE_ID,
            asyncUnaryCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.GetExecutorAliveIdRequest,
                com.alibaba.maxgraph.proto.GetExecutorAliveIdResponse>(
                  this, METHODID_GET_EXECUTOR_ALIVE_ID)))
          .addMethod(
            METHOD_GET_PARTITION_ASSIGNMENT,
            asyncUnaryCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.GetPartitionAssignmentRequest,
                com.alibaba.maxgraph.proto.GetPartitionAssignmentResponse>(
                  this, METHODID_GET_PARTITION_ASSIGNMENT)))
          .build();
    }
  }

  /**
   */
  public static final class ServerDataApiStub extends io.grpc.stub.AbstractStub<ServerDataApiStub> {
    private ServerDataApiStub(io.grpc.Channel channel) {
      super(channel);
    }

    private ServerDataApiStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ServerDataApiStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ServerDataApiStub(channel, callOptions);
    }

    /**
     * <pre>
     * get Server status
     * </pre>
     */
    public void getInstanceInfo(com.alibaba.maxgraph.proto.Request request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.InstanceInfoResp> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_GET_INSTANCE_INFO, getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * heartbeat
     * </pre>
     */
    public void heartbeat(com.alibaba.maxgraph.proto.ServerHBReq request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.ServerHBResp> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_HEARTBEAT, getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * update runtime env
     * </pre>
     */
    public void updateRuntimeEnv(com.alibaba.maxgraph.proto.RuntimeEnv request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.RuntimeEnvList> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_UPDATE_RUNTIME_ENV, getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * reset runtime envs
     * </pre>
     */
    public void resetRuntimeEnv(com.alibaba.maxgraph.proto.Empty request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.Empty> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_RESET_RUNTIME_ENV, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void simpleHeartbeat(com.alibaba.maxgraph.proto.SimpleServerHBReq request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.SimpleServerHBResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_SIMPLE_HEARTBEAT, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getRuntimeGroupStatus(com.alibaba.maxgraph.proto.Empty request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.RuntimeGroupStatusResp> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_GET_RUNTIME_GROUP_STATUS, getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * query data path is stale or not
     * </pre>
     */
    public void isDataPathInUse(com.alibaba.maxgraph.proto.ServerIdAliveIdProto request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.DataPathStatusResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_IS_DATA_PATH_IN_USE, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getWorkerInfoAndRoutingServerList(com.alibaba.maxgraph.proto.Request request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.RoutingServerInfoResp> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_GET_WORKER_INFO_AND_ROUTING_SERVER_LIST, getCallOptions()), request, responseObserver);
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

    /**
     */
    public void getExecutorAliveId(com.alibaba.maxgraph.proto.GetExecutorAliveIdRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.GetExecutorAliveIdResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_GET_EXECUTOR_ALIVE_ID, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getPartitionAssignment(com.alibaba.maxgraph.proto.GetPartitionAssignmentRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.GetPartitionAssignmentResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_GET_PARTITION_ASSIGNMENT, getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class ServerDataApiBlockingStub extends io.grpc.stub.AbstractStub<ServerDataApiBlockingStub> {
    private ServerDataApiBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private ServerDataApiBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ServerDataApiBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ServerDataApiBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * get Server status
     * </pre>
     */
    public com.alibaba.maxgraph.proto.InstanceInfoResp getInstanceInfo(com.alibaba.maxgraph.proto.Request request) {
      return blockingUnaryCall(
          getChannel(), METHOD_GET_INSTANCE_INFO, getCallOptions(), request);
    }

    /**
     * <pre>
     * heartbeat
     * </pre>
     */
    public com.alibaba.maxgraph.proto.ServerHBResp heartbeat(com.alibaba.maxgraph.proto.ServerHBReq request) {
      return blockingUnaryCall(
          getChannel(), METHOD_HEARTBEAT, getCallOptions(), request);
    }

    /**
     * <pre>
     * update runtime env
     * </pre>
     */
    public com.alibaba.maxgraph.proto.RuntimeEnvList updateRuntimeEnv(com.alibaba.maxgraph.proto.RuntimeEnv request) {
      return blockingUnaryCall(
          getChannel(), METHOD_UPDATE_RUNTIME_ENV, getCallOptions(), request);
    }

    /**
     * <pre>
     * reset runtime envs
     * </pre>
     */
    public com.alibaba.maxgraph.proto.Empty resetRuntimeEnv(com.alibaba.maxgraph.proto.Empty request) {
      return blockingUnaryCall(
          getChannel(), METHOD_RESET_RUNTIME_ENV, getCallOptions(), request);
    }

    /**
     */
    public com.alibaba.maxgraph.proto.SimpleServerHBResponse simpleHeartbeat(com.alibaba.maxgraph.proto.SimpleServerHBReq request) {
      return blockingUnaryCall(
          getChannel(), METHOD_SIMPLE_HEARTBEAT, getCallOptions(), request);
    }

    /**
     */
    public com.alibaba.maxgraph.proto.RuntimeGroupStatusResp getRuntimeGroupStatus(com.alibaba.maxgraph.proto.Empty request) {
      return blockingUnaryCall(
          getChannel(), METHOD_GET_RUNTIME_GROUP_STATUS, getCallOptions(), request);
    }

    /**
     * <pre>
     * query data path is stale or not
     * </pre>
     */
    public com.alibaba.maxgraph.proto.DataPathStatusResponse isDataPathInUse(com.alibaba.maxgraph.proto.ServerIdAliveIdProto request) {
      return blockingUnaryCall(
          getChannel(), METHOD_IS_DATA_PATH_IN_USE, getCallOptions(), request);
    }

    /**
     */
    public com.alibaba.maxgraph.proto.RoutingServerInfoResp getWorkerInfoAndRoutingServerList(com.alibaba.maxgraph.proto.Request request) {
      return blockingUnaryCall(
          getChannel(), METHOD_GET_WORKER_INFO_AND_ROUTING_SERVER_LIST, getCallOptions(), request);
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

    /**
     */
    public com.alibaba.maxgraph.proto.GetExecutorAliveIdResponse getExecutorAliveId(com.alibaba.maxgraph.proto.GetExecutorAliveIdRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_GET_EXECUTOR_ALIVE_ID, getCallOptions(), request);
    }

    /**
     */
    public com.alibaba.maxgraph.proto.GetPartitionAssignmentResponse getPartitionAssignment(com.alibaba.maxgraph.proto.GetPartitionAssignmentRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_GET_PARTITION_ASSIGNMENT, getCallOptions(), request);
    }
  }

  /**
   */
  public static final class ServerDataApiFutureStub extends io.grpc.stub.AbstractStub<ServerDataApiFutureStub> {
    private ServerDataApiFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private ServerDataApiFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ServerDataApiFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ServerDataApiFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * get Server status
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.alibaba.maxgraph.proto.InstanceInfoResp> getInstanceInfo(
        com.alibaba.maxgraph.proto.Request request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_GET_INSTANCE_INFO, getCallOptions()), request);
    }

    /**
     * <pre>
     * heartbeat
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.alibaba.maxgraph.proto.ServerHBResp> heartbeat(
        com.alibaba.maxgraph.proto.ServerHBReq request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_HEARTBEAT, getCallOptions()), request);
    }

    /**
     * <pre>
     * update runtime env
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.alibaba.maxgraph.proto.RuntimeEnvList> updateRuntimeEnv(
        com.alibaba.maxgraph.proto.RuntimeEnv request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_UPDATE_RUNTIME_ENV, getCallOptions()), request);
    }

    /**
     * <pre>
     * reset runtime envs
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.alibaba.maxgraph.proto.Empty> resetRuntimeEnv(
        com.alibaba.maxgraph.proto.Empty request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_RESET_RUNTIME_ENV, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.alibaba.maxgraph.proto.SimpleServerHBResponse> simpleHeartbeat(
        com.alibaba.maxgraph.proto.SimpleServerHBReq request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_SIMPLE_HEARTBEAT, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.alibaba.maxgraph.proto.RuntimeGroupStatusResp> getRuntimeGroupStatus(
        com.alibaba.maxgraph.proto.Empty request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_GET_RUNTIME_GROUP_STATUS, getCallOptions()), request);
    }

    /**
     * <pre>
     * query data path is stale or not
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.alibaba.maxgraph.proto.DataPathStatusResponse> isDataPathInUse(
        com.alibaba.maxgraph.proto.ServerIdAliveIdProto request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_IS_DATA_PATH_IN_USE, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.alibaba.maxgraph.proto.RoutingServerInfoResp> getWorkerInfoAndRoutingServerList(
        com.alibaba.maxgraph.proto.Request request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_GET_WORKER_INFO_AND_ROUTING_SERVER_LIST, getCallOptions()), request);
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

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.alibaba.maxgraph.proto.GetExecutorAliveIdResponse> getExecutorAliveId(
        com.alibaba.maxgraph.proto.GetExecutorAliveIdRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_GET_EXECUTOR_ALIVE_ID, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.alibaba.maxgraph.proto.GetPartitionAssignmentResponse> getPartitionAssignment(
        com.alibaba.maxgraph.proto.GetPartitionAssignmentRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_GET_PARTITION_ASSIGNMENT, getCallOptions()), request);
    }
  }

  private static final int METHODID_GET_INSTANCE_INFO = 0;
  private static final int METHODID_HEARTBEAT = 1;
  private static final int METHODID_UPDATE_RUNTIME_ENV = 2;
  private static final int METHODID_RESET_RUNTIME_ENV = 3;
  private static final int METHODID_SIMPLE_HEARTBEAT = 4;
  private static final int METHODID_GET_RUNTIME_GROUP_STATUS = 5;
  private static final int METHODID_IS_DATA_PATH_IN_USE = 6;
  private static final int METHODID_GET_WORKER_INFO_AND_ROUTING_SERVER_LIST = 7;
  private static final int METHODID_GET_REAL_TIME_METRIC = 8;
  private static final int METHODID_GET_ALL_REAL_TIME_METRICS = 9;
  private static final int METHODID_GET_EXECUTOR_ALIVE_ID = 10;
  private static final int METHODID_GET_PARTITION_ASSIGNMENT = 11;

  private static class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final ServerDataApiImplBase serviceImpl;
    private final int methodId;

    public MethodHandlers(ServerDataApiImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_GET_INSTANCE_INFO:
          serviceImpl.getInstanceInfo((com.alibaba.maxgraph.proto.Request) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.InstanceInfoResp>) responseObserver);
          break;
        case METHODID_HEARTBEAT:
          serviceImpl.heartbeat((com.alibaba.maxgraph.proto.ServerHBReq) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.ServerHBResp>) responseObserver);
          break;
        case METHODID_UPDATE_RUNTIME_ENV:
          serviceImpl.updateRuntimeEnv((com.alibaba.maxgraph.proto.RuntimeEnv) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.RuntimeEnvList>) responseObserver);
          break;
        case METHODID_RESET_RUNTIME_ENV:
          serviceImpl.resetRuntimeEnv((com.alibaba.maxgraph.proto.Empty) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.Empty>) responseObserver);
          break;
        case METHODID_SIMPLE_HEARTBEAT:
          serviceImpl.simpleHeartbeat((com.alibaba.maxgraph.proto.SimpleServerHBReq) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.SimpleServerHBResponse>) responseObserver);
          break;
        case METHODID_GET_RUNTIME_GROUP_STATUS:
          serviceImpl.getRuntimeGroupStatus((com.alibaba.maxgraph.proto.Empty) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.RuntimeGroupStatusResp>) responseObserver);
          break;
        case METHODID_IS_DATA_PATH_IN_USE:
          serviceImpl.isDataPathInUse((com.alibaba.maxgraph.proto.ServerIdAliveIdProto) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.DataPathStatusResponse>) responseObserver);
          break;
        case METHODID_GET_WORKER_INFO_AND_ROUTING_SERVER_LIST:
          serviceImpl.getWorkerInfoAndRoutingServerList((com.alibaba.maxgraph.proto.Request) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.RoutingServerInfoResp>) responseObserver);
          break;
        case METHODID_GET_REAL_TIME_METRIC:
          serviceImpl.getRealTimeMetric((com.alibaba.maxgraph.proto.MetricInfoRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.MetricInfoResp>) responseObserver);
          break;
        case METHODID_GET_ALL_REAL_TIME_METRICS:
          serviceImpl.getAllRealTimeMetrics((com.alibaba.maxgraph.proto.Request) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.AllMetricsInfoResp>) responseObserver);
          break;
        case METHODID_GET_EXECUTOR_ALIVE_ID:
          serviceImpl.getExecutorAliveId((com.alibaba.maxgraph.proto.GetExecutorAliveIdRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.GetExecutorAliveIdResponse>) responseObserver);
          break;
        case METHODID_GET_PARTITION_ASSIGNMENT:
          serviceImpl.getPartitionAssignment((com.alibaba.maxgraph.proto.GetPartitionAssignmentRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.GetPartitionAssignmentResponse>) responseObserver);
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
        METHOD_GET_INSTANCE_INFO,
        METHOD_HEARTBEAT,
        METHOD_UPDATE_RUNTIME_ENV,
        METHOD_RESET_RUNTIME_ENV,
        METHOD_SIMPLE_HEARTBEAT,
        METHOD_GET_RUNTIME_GROUP_STATUS,
        METHOD_IS_DATA_PATH_IN_USE,
        METHOD_GET_WORKER_INFO_AND_ROUTING_SERVER_LIST,
        METHOD_GET_REAL_TIME_METRIC,
        METHOD_GET_ALL_REAL_TIME_METRICS,
        METHOD_GET_EXECUTOR_ALIVE_ID,
        METHOD_GET_PARTITION_ASSIGNMENT);
  }

}
